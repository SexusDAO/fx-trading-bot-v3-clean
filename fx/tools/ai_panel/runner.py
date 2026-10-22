"""多模型并发调用。

- 每个模型一个独立请求（不带任何历史），asyncio 并发。
- 失败自动重试（默认 2 次、间隔 3 秒）；402/403/404 等“重试也没用”的错误不重试，避免白白计费。
- HTTP 传输层可注入（post 参数），默认用 httpx（已安装时），否则退回标准库 urllib，便于测试和部署。
"""
import asyncio
import json
import re
import time
import urllib.request
import urllib.error

from . import config

_NO_RETRY_STATUS = {400, 401, 402, 403, 404}


def token_param_for(model_cfg: dict) -> str:
    """决定输出上限的参数名：显式配置优先；否则 gpt-6* 与 o 系列（o3、o4-mini…）用 max_completion_tokens。"""
    if model_cfg.get("token_param"):
        return model_cfg["token_param"]
    name = model_cfg["name"]
    if name.startswith("gpt-6") or re.match(r"^o\d", name):
        return "max_completion_tokens"
    return "max_tokens"


def build_payload(model_cfg: dict, system_prompt: str, question: str) -> dict:
    return {
        "model": model_cfg["name"],
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
        token_param_for(model_cfg): int(model_cfg.get("max_tokens", 3000)),
    }


async def default_post(url: str, headers: dict, payload: dict, timeout: float):
    """返回 (http_status, json_dict_or_text)。优先 httpx，没有则用 urllib（放线程里，不阻塞事件循环）。"""
    try:
        import httpx  # type: ignore
    except ImportError:
        httpx = None
    if httpx is not None:
        # 超时：httpx 的 timeout 是“每个阶段”的时限，这里同时用 asyncio.wait_for 兜住总时限，与 urllib 路径行为一致
        async with httpx.AsyncClient(timeout=timeout) as c:
            r = await asyncio.wait_for(c.post(url, headers=headers, json=payload), timeout=timeout)
            try:
                return r.status_code, r.json()
            except Exception:
                return r.status_code, r.text

    def _sync():
        req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "ignore")
            try:
                return e.code, json.loads(body)
            except Exception:
                return e.code, body
    return await asyncio.to_thread(_sync)


def _content_text(content) -> str:
    """content 可能是字符串，也可能是内容块列表（[{"type":"text","text":...}]），统一成字符串。"""
    if isinstance(content, list):
        content = "".join(b.get("text", "") for b in content if isinstance(b, dict))
    return (content or "").strip()


def _parse_success(data: dict, latency: float) -> dict:
    choice = (data.get("choices") or [{}])[0]
    usage = data.get("usage") or {}
    return {
        "answer": _content_text((choice.get("message") or {}).get("content")),
        "input_tokens": usage.get("prompt_tokens", 0),
        "output_tokens": usage.get("completion_tokens", 0),
        "reasoning_tokens": (usage.get("completion_tokens_details") or {}).get("reasoning_tokens"),
        "latency_sec": round(latency, 2),
        "finish_reason": choice.get("finish_reason"),
        "error": None,
        # 排查空回答用：记录 message 里有哪些字段，以及思考内容的长度（有的模型把内容放在 reasoning_content）
        "message_keys": sorted((choice.get("message") or {}).keys()),
        "reasoning_chars": len(str((choice.get("message") or {}).get("reasoning_content") or "")),
    }


async def call_model(model_cfg: dict, system_prompt: str, question: str, *, post=None,
                     retries: int = None, delay: float = None, timeout: float = None) -> dict:
    """调用单个模型，返回结果字典；失败时 answer 为空、error 为原因，永不抛异常（密钥缺失除外）。"""
    post = post or default_post
    retries = config.PANEL_MAX_RETRIES if retries is None else retries
    delay = config.PANEL_RETRY_DELAY if delay is None else delay
    timeout = config.PANEL_REQUEST_TIMEOUT if timeout is None else timeout
    url = config.PANEL_BASE_URL.rstrip("/") + "/chat/completions"
    headers = {"Authorization": "Bearer " + config.get_api_key(), "Content-Type": "application/json",
               "User-Agent": "Mozilla/5.0"}
    payload = build_payload(model_cfg, system_prompt, question)
    last_err = "未知错误"
    for attempt in range(retries + 1):
        t0 = time.time()
        try:
            status, data = await post(url, headers, payload, timeout)
        except Exception as e:  # 网络/超时
            status, data = None, None
            last_err = f"{type(e).__name__}: {str(e)[:120]}"
        else:
            if status == 200 and isinstance(data, dict) and data.get("choices"):
                return _parse_success(data, time.time() - t0)
            body = json.dumps(data, ensure_ascii=False)[:160] if isinstance(data, dict) else str(data)[:160]
            last_err = f"HTTP {status}: {body}"
            if status in _NO_RETRY_STATUS:
                break
        if attempt < retries:
            await asyncio.sleep(delay)
    return {"answer": "", "input_tokens": 0, "output_tokens": 0, "reasoning_tokens": None,
            "latency_sec": 0.0, "finish_reason": None, "error": last_err}


async def run_question(question_text: str, models: list, system_prompt: str, *, post=None, **kw) -> dict:
    """同一道题并发发给多个模型，返回 {模型名: 结果}。"""
    # 并发上限：同时在途请求不超过 PANEL_MAX_CONCURRENCY，避免触发中转站限速
    sem = asyncio.Semaphore(max(1, int(config.PANEL_MAX_CONCURRENCY)))

    async def _one(m):
        async with sem:
            return await call_model(m, system_prompt, question_text, post=post, **kw)
    results = await asyncio.gather(*[_one(m) for m in models])
    return {m["name"]: r for m, r in zip(models, results)}
