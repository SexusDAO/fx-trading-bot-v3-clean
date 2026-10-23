"""
fx/shared/llm_judge.py — LLM 辅助判断模块（v3.41.2）

优化目标（v3.41.2）：速度优先，token 其次
  - 极简系统提示 ~30 tokens（原 11,500 tokens 工作区）
  - 极简 prompt ~80 tokens（原 ~578 tokens）
  - max_tokens 60（原 150）
  - timeout 10s（原 60s）
  - 预期响应 < 3s（OpenAI） / < 15s（openclaw lite）

触发场景（不变）：
  1. 妖币识别（3日涨>100% 但 24H 横盘）
  2. 规则边界徘徊（final_score 45~55）
  3. 新闻语义理解（caution 类新闻深度解读）

安全保证：
  - 总开关 LLM_JUDGE_ENABLED=False 全局关闭
  - 每场景独立开关
  - 调用失败 → fallback，不阻断主流程
  - 同一标的 1 小时冷却
"""

from __future__ import annotations
import json
import logging
import os
import time
from typing import Optional

logger = logging.getLogger("fx.llm_judge")

# ── 冷却缓存 ──────────────────────────────────────────────────────────────────
_llm_call_cache: dict[str, float] = {}
_LLM_COOLDOWN_SEC = 3600


def _is_cooldown(key: str) -> bool:
    return time.time() - _llm_call_cache.get(key, 0) < _LLM_COOLDOWN_SEC


def _cooldown_result(fallback: dict) -> dict:
    """
    [v3.89 #80] 冷却内没有调用模型：决策值仍是降级 skip（不改任何决策），
    但带上 source="cooldown"、unavailable=True，调用方可据此区分「模型说 skip」与「没调用模型」。
    返回副本，避免调用方改动污染降级常量。
    """
    r = dict(fallback)
    r["source"] = "cooldown"
    r["unavailable"] = True
    r["reason"] = "冷却中未调用"
    return r


def _mark_called(key: str) -> None:
    _llm_call_cache[key] = time.time()


# [v3.89 #79] LLM 返回的 JSON 能解析 ≠ 结果有效。系统提示要求 {"decision":"open"|"skip","confidence":0~1,"reason":...}。
_VALID_DECISIONS = ("open", "skip")
_invalid_result_count = 0   # 累计「结构无效」次数（与调用失败/降级分开统计）


def _validate_llm_result(result, scene: str = "") -> Optional[dict]:
    """
    校验 LLM 返回：必须是 dict、decision ∈ {open, skip}、confidence 为 [0,1] 内的有限数（bool 不算）。
    通过则原样返回；不通过返回 None 并记 warning（标明是「结构无效」，区别于调用失败）。
    调用方在 None 时沿用原有降级决策，降级值不变。
    """
    global _invalid_result_count
    import math as _m
    why = None
    if not isinstance(result, dict):
        why = f"返回不是对象: {type(result).__name__}"
    else:
        d = result.get("decision")
        c = result.get("confidence")
        if d not in _VALID_DECISIONS:
            why = f"decision 非法: {d!r}"
        elif isinstance(c, bool) or not isinstance(c, (int, float)) or not _m.isfinite(c) or not (0.0 <= c <= 1.0):
            why = f"confidence 非法: {c!r}"
    if why:
        _invalid_result_count += 1
        logger.warning(f"[llm_judge] {scene} 结果结构无效（{why}），按降级处理 | 累计 {_invalid_result_count}")
        return None
    return result


def _is_enabled(scene_flag: Optional[bool] = None) -> bool:
    try:
        from config import LLM_JUDGE_ENABLED
        if not LLM_JUDGE_ENABLED:
            return False
    except ImportError:
        pass
    if scene_flag is not None and not scene_flag:
        return False
    return True


# ── 极简系统提示（~30 tokens，不加载工作区文件）──────────────────────────────
_SYSTEM_PROMPT = (
    "合约交易信号判断助手。"
    '只输出JSON：{"decision":"open"或"skip",'
    '"confidence":0.0~1.0,"reason":"≤15字"}'
)


# ══════════════════════════════════════════════════════════════════════════════
# 核心调用函数（双路径，速度优先）
# ══════════════════════════════════════════════════════════════════════════════

LLM_CALL_TOTAL_TIMEOUT = 60.0   # [fix #35b] 单次 LLM 调用总超时（秒）
_LLM_POOL = None


def _call_llm(prompt: str, max_tokens: int = 60, model: str = None) -> Optional[dict]:
    """[fix #35b] 总超时 60s：超时返回 None，调用方按既有 fallback 使用默认分值，并记 WARNING。"""
    global _LLM_POOL
    import concurrent.futures as _cf
    if _LLM_POOL is None:
        _LLM_POOL = _cf.ThreadPoolExecutor(max_workers=2, thread_name_prefix="llm-call")
    fut = _LLM_POOL.submit(_call_llm_inner, prompt, max_tokens, model)
    try:
        return fut.result(timeout=LLM_CALL_TOTAL_TIMEOUT)
    except _cf.TimeoutError:
        logger.warning(f"[llm_judge] LLM 调用超过 {LLM_CALL_TOTAL_TIMEOUT:.0f}s 未返回，跳过 LLM 评分，使用默认分值")
        return None


def _call_llm_inner(prompt: str, max_tokens: int = 60, model: str = None) -> Optional[dict]:
    """
    调用 LLM，返回解析后的 JSON dict。失败返回 None。

    路径 A（优先）：OpenAI API 直连 —— < 3 秒，< 200 tokens
    路径 B（fallback）：openclaw agent lite —— < 15 秒，系统提示最小化
    """
    # ── 路径 A：OpenAI API 直连 ───────────────────────────────────────────────
    api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("LLM_API_KEY")
    if api_key:
        result = _call_openai_direct(prompt, api_key, max_tokens, model)
        if result is not None:
            return result

    # ── 路径 B：openclaw agent（极简提示，无工作区加载）──────────────────────
    return _call_openclaw_lite(prompt, max_tokens)


def _call_openai_direct(prompt: str, api_key: str, max_tokens: int, model: str = None) -> Optional[dict]:
    """OpenAI API 直连，目标 < 3 秒响应"""
    try:
        import urllib.request as _req
        base_url = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com")
        # model 优先级：调用方传入 > 环境变量 > config 默认 > 硬编码兜底
        _model = model or os.environ.get("OPENAI_MODEL")
        if not _model:
            try:
                from config import LLM_API_MODEL
                _model = LLM_API_MODEL
            except ImportError:
                _model = "gpt-4o"
        body = json.dumps({
            "model": _model,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": prompt},
            ],
            "max_tokens":  max_tokens,
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }).encode("utf-8")
        req = _req.Request(
            f"{base_url}/v1/chat/completions",
            data=body,
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {api_key}"},
            method="POST",
        )
        with _req.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        content = data["choices"][0]["message"]["content"].strip()
        # 剥离 markdown 代码块
        if "```" in content:
            for chunk in content.split("```"):
                chunk = chunk.strip().lstrip("json").strip()
                if chunk.startswith("{"):
                    content = chunk; break
        return json.loads(content)
    except Exception as e:
        logger.debug(f"[llm_judge] OpenAI 直连失败: {e}")
        return None


def _call_openclaw_lite(prompt: str, max_tokens: int) -> Optional[dict]:
    """
    openclaw agent 极简模式：
    - 把 system+user 合并为一条消息，减少 agent 处理的歧义
    - 不传 --session-id（每次新 session），但注入轻量系统提示覆盖工作区语境
    - timeout=15s：超时直接 fallback，不阻塞扫描
    """
    import subprocess as _sp
    import uuid as _uuid

    combined = (
        f"{_SYSTEM_PROMPT}\n\n"
        f"判断请求：\n{prompt}\n\n"
        "只输出JSON，不要解释，不要加markdown。"
    )
    sid = f"llm-j-{_uuid.uuid4().hex[:6]}"
    try:
        r = _sp.run(
            ["openclaw", "agent",
             "--message",    combined,
             "--session-id", sid,
             "--json"],
            capture_output=True, text=True, timeout=60,  # 进程超时 60s（实测 30s 左右）
        )
        if r.returncode != 0:
            logger.debug(f"[llm_judge] openclaw agent 非零返回: {r.stderr[:100]}")
            return None
        if not r.stdout.strip():
            return None

        raw_json = json.loads(r.stdout)
        # 格式：{"result": {"payloads": [{"text": "..."}]}}
        payloads   = raw_json.get("result", {}).get("payloads", [])
        reply_text = payloads[0].get("text", "") if payloads else ""
        if not reply_text:
            reply_text = raw_json.get("reply", raw_json.get("text", ""))

        reply_text = reply_text.strip()
        # 剥离 markdown 代码块（Claude 系有时会包裹 ```json ... ```）
        if "```" in reply_text:
            inner = reply_text.split("```")
            for chunk in inner:
                chunk = chunk.strip().lstrip("json").strip()
                if chunk.startswith("{"):
                    reply_text = chunk; break
        start = reply_text.find("{")
        end   = reply_text.rfind("}") + 1
        if start >= 0 and end > start:
            parsed = json.loads(reply_text[start:end])
            # 记录 token 消耗（debug 级别，不干扰正常日志）
            usage = raw_json.get("result", {}).get("meta", {}).get(
                "agentMeta", {}).get("usage", {})
            if usage:
                logger.debug(
                    f"[llm_judge] token: in={usage.get('input',0)} "
                    f"out={usage.get('output',0)} "
                    f"cacheW={usage.get('cacheWrite',0)}"
                )
            return parsed
        return None

    except _sp.TimeoutExpired:
        logger.debug("[llm_judge] openclaw agent 超时，走 fallback")
        return None
    except Exception as e:
        logger.debug(f"[llm_judge] openclaw agent 异常: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# 场景1：妖币识别（3日涨>100% 但 24H 横盘）
# ══════════════════════════════════════════════════════════════════════════════

def is_explosive_coin(k1h: list) -> tuple[bool, float, float]:
    """
    纯规则检测妖币：3日涨幅 > 100% 且 24H 涨幅绝对值 < 10%
    返回 (是否妖币, 3日涨幅%, 24H涨幅%)  零 LLM 消耗。
    """
    if len(k1h) < 73:
        return False, 0.0, 0.0
    closes    = [float(k[4]) for k in k1h]
    price_now = closes[-1]
    price_72h = closes[-73]
    price_24h = closes[-25] if len(closes) >= 25 else closes[0]
    gain_3d   = (price_now - price_72h) / (price_72h  + 1e-9) * 100
    gain_24h  = (price_now - price_24h) / (price_24h + 1e-9) * 100
    try:
        from config import LLM_EXPLOSIVE_GAIN_3D_MIN, LLM_EXPLOSIVE_GAIN_24H_MAX
    except ImportError:
        LLM_EXPLOSIVE_GAIN_3D_MIN  = 100.0
        LLM_EXPLOSIVE_GAIN_24H_MAX = 10.0
    is_exp = gain_3d > LLM_EXPLOSIVE_GAIN_3D_MIN and abs(gain_24h) < LLM_EXPLOSIVE_GAIN_24H_MAX
    return is_exp, gain_3d, gain_24h


def judge_explosive_coin_llm(
    symbol: str, gain_3d: float, gain_24h: float,
    rsi_1h: float, rsi_4h: float,
    atr_ratio: float, funding: float
) -> dict:
    """妖币 LLM 判断：横盘蓄势 or 出货？"""
    _fallback = {"decision": "skip", "confidence": 0.5, "reason": "LLM降级"}
    try:
        from config import LLM_JUDGE_EXPLOSIVE_COIN, LLM_JUDGE_MIN_CONFIDENCE
    except ImportError:
        LLM_JUDGE_EXPLOSIVE_COIN = True
        LLM_JUDGE_MIN_CONFIDENCE = 0.65

    if not _is_enabled(LLM_JUDGE_EXPLOSIVE_COIN):
        return _fallback
    if _is_cooldown(f"exp_{symbol}"):
        logger.debug(f"[llm_judge] {symbol} 妖币冷却中")
        return _cooldown_result(_fallback)

    # 极简 prompt，< 60 tokens
    # 费率负值说明：负费率=空头付多头，市场空头拥挤，不代表利多
    _fr_desc = ("空头极度拥挤" if funding < -0.005 else
                "空头拥挤" if funding < 0 else
                "多头拥挤" if funding > 0.005 else "中性")
    prompt = (
        f"{symbol} 3日+{gain_3d:.0f}% 24H{gain_24h:+.1f}%横盘 "
        f"RSI1H={rsi_1h:.0f} RSI4H={rsi_4h:.0f} "
        f"ATR={atr_ratio:.1f}% 费率={funding*100:.3f}%({_fr_desc})\n"
        "3日暴涨后横盘，是蓄势待涨还是高位出货？做range_bounce LONG合适吗？"
    )

    result = _call_llm(prompt, max_tokens=60)
    if result is None:
        return _fallback
    if _validate_llm_result(result, f"{symbol} 妖币") is None:
        return _fallback

    _mark_called(f"exp_{symbol}")
    logger.info(f"[llm_judge] {symbol} 妖币: {result}")
    return result


# ══════════════════════════════════════════════════════════════════════════════
# 场景2：规则边界徘徊（final_score 45~55）
# ══════════════════════════════════════════════════════════════════════════════

def judge_borderline_signal_llm(
    symbol: str, sig_type: str, direction: str,
    final_score: int, ind: dict,
    chg24: float, mkt_mode: str
) -> dict:
    """边界信号 LLM 综合判断（score 45~55）"""
    _fallback = {"decision": "skip", "confidence": 0.5, "reason": "边界降级"}
    try:
        from config import LLM_JUDGE_BORDERLINE
    except ImportError:
        LLM_JUDGE_BORDERLINE = True

    if not _is_enabled(LLM_JUDGE_BORDERLINE):
        return _fallback
    if _is_cooldown(f"bl_{symbol}"):
        return _cooldown_result(_fallback)

    ema_bull = ind.get("ema_bull_1h") or (
        ind.get("ema20_1h", 0) > ind.get("ema50_1h", 0)
    )

    # 极简 prompt，< 80 tokens
    prompt = (
        f"{symbol} {sig_type} {direction} score={final_score} {mkt_mode}\n"
        f"24H={chg24:+.1f}% RSI1H={ind.get('rsi_1h',50):.0f} "
        f"RSI4H={ind.get('rsi_4h',50):.0f} "
        f"ATR={ind.get('atr_ratio',0):.1f}% EMA多头={ema_bull}\n"
        "边界信号值得开仓吗？"
    )

    result = _call_llm(prompt, max_tokens=60)
    if result is None:
        return _fallback
    if _validate_llm_result(result, f"{symbol} 边界") is None:
        return _fallback

    _mark_called(f"bl_{symbol}")
    logger.info(f"[llm_judge] {symbol} 边界 score={final_score}: {result}")
    return result


# ══════════════════════════════════════════════════════════════════════════════
# 场景3：新闻语义理解
# ══════════════════════════════════════════════════════════════════════════════

def judge_news_sentiment_llm(
    symbol: str, news_text: str, signal_direction: str
) -> str:
    """新闻语义分析，返回 'bullish'|'bearish'|'neutral'"""
    try:
        from config import LLM_JUDGE_NEWS
    except ImportError:
        LLM_JUDGE_NEWS = True

    if not _is_enabled(LLM_JUDGE_NEWS):
        return "neutral"
    if len(news_text.strip()) < 20:
        return "neutral"
    _key = f"news_{symbol}"
    if _is_cooldown(_key):
        return "neutral"

    # 新闻截断到 200 字，极简 prompt
    news_short = news_text[:200]
    prompt = (
        f"{symbol} 开{signal_direction}仓\n"
        f"新闻：{news_short}\n"
        f"对{signal_direction}方向影响？"
        "open=利好 skip=利空"
    )

    try:
        from config import LLM_API_MODEL_FAST
        _news_model = LLM_API_MODEL_FAST
    except ImportError:
        _news_model = None
    result = _call_llm(prompt, max_tokens=60, model=_news_model)
    if result is None:
        return "neutral"
    if _validate_llm_result(result, f"{symbol} 新闻") is None:
        return "neutral"

    _mark_called(_key)
    decision = result.get("decision", "neutral")
    logger.info(f"[llm_judge] {symbol} 新闻: {result}")
    return "bullish" if decision == "open" else \
           "bearish" if decision == "skip" else "neutral"
