"""读取本地结果做共识/分歧汇总。不重新调用被测模型，只调用一次汇总模型（默认 claude-sonnet-5）。"""
import datetime
from pathlib import Path

from . import config, runner, storage

MIN_OUTPUT_TOKENS = 200

ANALYSIS_PROMPT = """下面是同一道题，多个独立 AI 模型各自给出的回答。请做结构化汇总，严格按以下格式，总共不超过 400 字，不要复述原回答：

【共识】多数模型（≥60%）认同的核心观点，1~2 句话。
【分歧】明显不同的立场及各自核心理由。
【最不确定的地方】各模型共同承认没把握的假设。

要求：只依据下面的回答，不要补充你自己的观点；注明每个观点来自哪几个模型。

【题目】
{question}

{answers}
"""


def is_valid(resp: dict) -> bool:
    """有效回答：无错误、输出 token > 200 且 finish_reason == "stop"。"""
    if not resp or resp.get("error") or not resp.get("answer"):
        return False
    return (resp.get("output_tokens") or 0) > MIN_OUTPUT_TOKENS and resp.get("finish_reason") == "stop"


def quality_label(resp: dict) -> str:
    """数据质量标记：有效 / 截断 / 失败。"""
    if not resp or resp.get("error") or not resp.get("answer"):
        return "失败"
    if is_valid(resp):
        return "有效"
    return "截断"


def build_prompt(question_text: str, responses: dict) -> str:
    valid = {m: r for m, r in responses.items() if is_valid(r)}
    blocks = [f"--- 模型 {m} ---\n{r['answer']}" for m, r in valid.items()]
    return ANALYSIS_PROMPT.format(question=question_text, answers="\n\n".join(blocks))


async def analyze_question(data: dict, *, post=None) -> dict:
    responses = data.get("responses", {})
    valid = [m for m, r in responses.items() if is_valid(r)]
    quality = {m: quality_label(r) for m, r in responses.items()}
    summary = None
    if len(valid) >= 2:
        r = await runner.call_model(config.PANEL_ANALYZER_MODEL,
                                    "你是严谨的研究助理，只做汇总，不添加自己的观点。",
                                    build_prompt(data.get("question_text", ""), responses), post=post)
        summary = r["answer"] if r.get("answer") else f"（汇总失败：{r.get('error')}）"
    return {"question_id": data.get("question_id"), "valid_models": valid, "quality": quality, "summary": summary}


async def analyze_dir(directory: Path, output_dir: Path = None, *, post=None) -> Path:
    all_data = storage.load_all(directory)
    lines = [f"# 专家面板汇总 {datetime.date.today().isoformat()}", f"来源目录：{directory}", ""]
    for qid, data in all_data.items():
        res = await analyze_question(data, post=post)
        lines.append(f"## {qid}")
        lines.append(f"- 有效回答 {len(res['valid_models'])} 个：{', '.join(res['valid_models']) or '无'}")
        lines.append("- 数据质量：" + "；".join(f"{m}={q}" for m, q in res["quality"].items()))
        lines.append("")
        lines.append(res["summary"] or "数据不足（有效回答少于 2 个），建议补问。")
        lines.append("")
    out_dir = Path(output_dir) if output_dir else Path(directory).parent / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{Path(directory).name}_summary.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    return out
