"""
fx/ai_advisor.py
每日 AI 参数顾问：读取 archive 历史数据 → 调用 LLM → 生成参数建议报告 → 推送 Telegram

触发方式：由 archiver.py 的 archive_daily() 完成后调用
决策权：AI 只生成建议，不自动修改 config.py。
       用户回复「采纳N」才由人工执行修改。
"""

from __future__ import annotations
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger("fx.ai_advisor")


# ─────────────────────────────────────────────────────────────────────────────
# 数据读取层
# ─────────────────────────────────────────────────────────────────────────────

def _load_archive_data(archive_dir: Path, lookback_days: int) -> list[dict]:
    """读取最近 lookback_days 天的 archive JSON 文件，返回列表（最旧→最新）。"""
    files = sorted(archive_dir.glob("*.json"))
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=lookback_days)
    result = []
    for f in files:
        try:
            day = datetime.strptime(f.stem, "%Y-%m-%d").date()
        except ValueError:
            continue
        if day >= cutoff:
            try:
                with open(f, encoding="utf-8") as fh:
                    result.append(json.load(fh))
            except Exception as e:
                logger.warning(f"[ai_advisor] 读取 {f.name} 失败: {e}")
    return result


def _aggregate(daily_list: list[dict]) -> dict:
    """
    把多天的 archive 数据合并为一份统计摘要，供 LLM 分析。
    archive 文件格式参考 archiver.py 输出。
    """
    if not daily_list:
        return {}

    all_trades: list[dict] = []
    for day_data in daily_list:
        all_trades.extend(day_data.get("trades", []))

    if not all_trades:
        return {"note": "archive 文件存在但无交易记录"}

    total = len(all_trades)
    wins  = sum(1 for t in all_trades if t.get("pnl", 0) > 0)
    pnl   = sum(t.get("pnl", 0) for t in all_trades)

    # ── 按策略模式分组 ────────────────────────────────────────────────────────
    by_mode: dict[str, dict] = {}
    for t in all_trades:
        mode = t.get("sig_type") or t.get("mode") or "unknown"
        bm   = by_mode.setdefault(mode, {
            "count": 0, "wins": 0, "pnl": 0.0,
            "close_reasons": {},
            "sl_pcts": [], "hold_mins": [],
            "max_profit_before_sl_atrs": [],
        })
        bm["count"] += 1
        if t.get("pnl", 0) > 0:
            bm["wins"] += 1
        bm["pnl"] += t.get("pnl", 0)
        cr = t.get("close_reason", "unknown")
        bm["close_reasons"][cr] = bm["close_reasons"].get(cr, 0) + 1
        if t.get("sl_pct"):
            bm["sl_pcts"].append(float(t["sl_pct"]))
        if t.get("hold_min"):
            bm["hold_mins"].append(float(t["hold_min"]))
        # 止损前最大浮盈（ATR 倍数）
        if t.get("close_reason") == "sl" and t.get("max_profit_atr") is not None:
            bm["max_profit_before_sl_atrs"].append(float(t["max_profit_atr"]))

    mode_summary = {}
    for mode, bm in by_mode.items():
        n = bm["count"]
        mode_summary[mode] = {
            "count":          n,
            "win_rate":       round(bm["wins"] / n, 3) if n else 0,
            "total_pnl":      round(bm["pnl"], 4),
            "close_reason_dist": bm["close_reasons"],
            "avg_sl_dist_pct": round(
                sum(bm["sl_pcts"]) / len(bm["sl_pcts"]) * 100, 2
            ) if bm["sl_pcts"] else None,
            "avg_hold_min":   round(
                sum(bm["hold_mins"]) / len(bm["hold_mins"]), 1
            ) if bm["hold_mins"] else None,
            "avg_max_profit_before_sl_atr": round(
                sum(bm["max_profit_before_sl_atrs"]) / len(bm["max_profit_before_sl_atrs"]), 2
            ) if bm["max_profit_before_sl_atrs"] else None,
        }

    # ── 信号强度分析 ─────────────────────────────────────────────────────────
    strength_groups: dict[str, dict] = {}
    for t in all_trades:
        s = t.get("signal_strength", "unknown")
        sg = strength_groups.setdefault(s, {"count": 0, "wins": 0})
        sg["count"] += 1
        if t.get("pnl", 0) > 0:
            sg["wins"] += 1
    strength_summary = {
        s: {"count": sg["count"],
            "win_rate": round(sg["wins"] / sg["count"], 3) if sg["count"] else 0}
        for s, sg in strength_groups.items()
    }

    # ── 止损质量 ─────────────────────────────────────────────────────────────
    degraded_trades = [t for t in all_trades if t.get("sl_degraded")]
    liq_trades      = [t for t in all_trades if not t.get("sl_degraded")]
    sl_hit          = [t for t in all_trades if t.get("close_reason") == "sl"]

    sl_analysis = {
        "sl_hit_count":           len(sl_hit),
        "sl_hit_rate":            round(len(sl_hit) / total, 3) if total else 0,
        "degraded_count":         len(degraded_trades),
        "degraded_win_rate":      round(
            sum(1 for t in degraded_trades if t.get("pnl", 0) > 0) / len(degraded_trades), 3
        ) if degraded_trades else None,
        "liq_sl_win_rate":        round(
            sum(1 for t in liq_trades if t.get("pnl", 0) > 0) / len(liq_trades), 3
        ) if liq_trades else None,
    }

    # ── 挂单效率 ─────────────────────────────────────────────────────────────
    pending_trades    = [t for t in all_trades if t.get("entry_mode") == "pending_order"]
    cancelled_health  = sum(1 for t in all_trades if t.get("cancel_reason") == "health_check")
    cancelled_expire  = sum(1 for t in all_trades if t.get("cancel_reason") == "expire")
    pending_filled    = sum(1 for t in pending_trades if t.get("filled", True))

    pending_analysis = {
        "pending_signal_count":   len(pending_trades),
        "fill_rate":              round(pending_filled / len(pending_trades), 3) if pending_trades else None,
        "cancelled_by_health":    cancelled_health,
        "cancelled_by_expire":    cancelled_expire,
    }

    # ── 时间分析 ─────────────────────────────────────────────────────────────
    time_stop = [t for t in all_trades if t.get("close_reason") == "time_stop"]
    tp1_times = [t["hold_min"] for t in all_trades
                 if t.get("close_reason") in ("tp1", "tp2") and t.get("hold_min")]
    sl_times  = [t["hold_min"] for t in all_trades
                 if t.get("close_reason") == "sl" and t.get("hold_min")]

    time_analysis = {
        "time_stop_count":        len(time_stop),
        "time_stop_rate":         round(len(time_stop) / total, 3) if total else 0,
        "avg_tp1_hit_time_min":   round(sum(tp1_times)/len(tp1_times), 1) if tp1_times else None,
        "avg_sl_hit_time_min":    round(sum(sl_times)/len(sl_times), 1)   if sl_times  else None,
    }

    return {
        "analysis_period_days":   len(daily_list),
        "total_trades":           total,
        "overall_win_rate":       round(wins / total, 3) if total else 0,
        "overall_pnl":            round(pnl, 4),
        "by_mode":                mode_summary,
        "signal_strength":        strength_summary,
        "sl_analysis":            sl_analysis,
        "pending_analysis":       pending_analysis,
        "time_analysis":          time_analysis,
    }


def _get_current_params() -> dict:
    """从 config.py 读取关键参数快照。"""
    try:
        import config as cfg
        keys = getattr(cfg, "AI_ADVISOR_PARAM_KEYS", [])
        return {k: getattr(cfg, k, None) for k in keys if hasattr(cfg, k)}
    except Exception as e:
        logger.warning(f"[ai_advisor] 读取 config 失败: {e}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# LLM 调用层（支持 OpenAI / 兼容端点）
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
你是一个合约交易策略顾问，负责分析一个自动化交易系统的历史表现并给出参数调整建议。

系统有四套策略：pullback（趋势回踩）、breakout（动量突破）、reversal（极端反转）、funding_arb（资金费率顺势）。

分析规则：
1. 只基于提供的数据分析，不做主观预测
2. 参数调整建议必须有数据支撑，格式：「因为XX数据显示XX，所以建议调整XX参数」
3. 样本量 < 10 笔的策略不给调整建议，只说「样本不足，建议继续观察」
4. 每条建议必须说明：调整方向（收紧/放宽）、调整幅度（小幅5~10% / 中幅10~20% / 大幅>20%）、预期效果
5. 最多给 3 条建议，按重要性排序
6. 如果整体表现良好，可以直接建议「维持现状」
7. 用中文输出，语言简洁，不要废话

输出格式（严格遵守）：
【整体评价】一句话总结整体表现
【问题发现】用编号列出 1~3 个数据驱动的问题（无问题则写「暂无明显问题」）
【参数建议】
1️⃣ [优先级：高/中/低] 建议标题
   建议内容：具体修改什么参数，从X改到Y
   数据依据：引用具体数字
   预期效果：简短描述
   风险提示：可能的副作用
（无建议时写：当前参数表现正常，建议维持现状）
【维持不变】哪些参数/策略表现良好不需要动
"""

def _call_llm(stats: dict, current_params: dict) -> Optional[str]:
    """
    调用 LLM 生成建议报告。
    优先使用 OPENAI_API_KEY；没有则降级为内置分析。
    """
    api_key = os.environ.get("OPENAI_API_KEY") or _get_cfg("OPENAI_API_KEY", "")
    if not api_key:
        logger.warning("[ai_advisor] 未配置 OPENAI_API_KEY，使用内置规则分析")
        return _rule_based_analysis(stats, current_params)

    user_content = (
        "以下是交易系统最近的运行数据，请给出分析和参数建议：\n\n"
        f"```json\n{json.dumps({'stats': stats, 'current_params': current_params}, ensure_ascii=False, indent=2)}\n```"
    )

    try:
        import urllib.request
        payload = json.dumps({
            "model":      os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
            "messages":   [
                {"role": "system",  "content": SYSTEM_PROMPT},
                {"role": "user",    "content": user_content},
            ],
            "max_tokens": 1200,
            "temperature": 0.3,
        }).encode()

        req = urllib.request.Request(
            url     = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com") + "/v1/chat/completions",
            data    = payload,
            headers = {
                "Content-Type":  "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            method  = "POST",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
        return data["choices"][0]["message"]["content"].strip()

    except Exception as e:
        logger.warning(f"[ai_advisor] LLM 调用失败: {e}，降级为规则分析")
        return _rule_based_analysis(stats, current_params)


def _get_cfg(key: str, default=None):
    try:
        import config as cfg
        return getattr(cfg, key, default)
    except Exception:
        return default


def _rule_based_analysis(stats: dict, current_params: dict) -> str:
    """
    无 LLM API 时的降级：用简单规则生成文字报告。
    覆盖最常见的几个信号：止损率过高、策略胜率过低、时间止损过多。
    """
    lines = []
    total   = stats.get("total_trades", 0)
    wr      = stats.get("overall_win_rate", 0)
    pnl     = stats.get("overall_pnl", 0)
    by_mode = stats.get("by_mode", {})
    sl_info = stats.get("sl_analysis", {})

    # ── 整体评价 ─────────────────────────────────────────────────────────────
    if total < 5:
        return (
            "【整体评价】交易笔数不足（< 5 笔），暂无有效分析\n"
            "【参数建议】样本不足，建议继续运行后再分析\n"
            "【维持不变】全部参数"
        )

    perf_word = "良好" if wr >= 0.55 else ("一般" if wr >= 0.45 else "偏低")
    lines.append(f"【整体评价】近期共 {total} 笔，胜率 {wr:.1%}，总PnL {pnl:+.2f}U，整体表现{perf_word}")

    # ── 问题发现 ─────────────────────────────────────────────────────────────
    issues = []
    sl_rate = sl_info.get("sl_hit_rate", 0)
    if sl_rate > 0.30:
        issues.append(f"① 止损触发率偏高（{sl_rate:.1%}，建议 < 25%），说明止损位置可能偏紧或信号质量偏低")

    for mode, bm in by_mode.items():
        n  = bm.get("count", 0)
        wr2 = bm.get("win_rate", 0)
        if n >= 10 and wr2 < 0.40:
            issues.append(f"② {mode} 策略胜率偏低（{n} 笔，胜率 {wr2:.1%}），信号质量需关注")

    t_info = stats.get("time_analysis", {})
    t_rate = t_info.get("time_stop_rate", 0)
    if t_rate > 0.20:
        issues.append(f"③ 时间止损比率偏高（{t_rate:.1%}），说明部分信号入场时机可能存在偏差")

    if issues:
        lines.append("【问题发现】\n" + "\n".join(issues))
    else:
        lines.append("【问题发现】暂无明显问题")

    # ── 参数建议 ─────────────────────────────────────────────────────────────
    suggestions = []

    if sl_rate > 0.30:
        # 止损率高 → 建议适当放宽止损系数
        cur = current_params.get("SL_MULT_MID", 1.5)
        new = round(cur * 1.15, 2)
        suggestions.append(
            f"1️⃣ [优先级：高] 适当放宽中波动层止损系数\n"
            f"   建议内容：SL_MULT_MID 从 {cur} → {new}（小幅放宽15%）\n"
            f"   数据依据：止损触发率 {sl_rate:.1%} > 30% 警戒线\n"
            f"   预期效果：减少正常波动引发的止损，提升胜率\n"
            f"   风险提示：单笔最大亏损增加约15%，需确认账户承受能力"
        )

    low_mode = [(m, bm) for m, bm in by_mode.items()
                if bm.get("count", 0) >= 10 and bm.get("win_rate", 0) < 0.40]
    if low_mode:
        m, bm = low_mode[0]
        suggestions.append(
            f"2️⃣ [优先级：中] {m} 策略信号门槛适当收紧\n"
            f"   建议内容：提高 {m} 的入场条件严格程度（具体参数需人工判断）\n"
            f"   数据依据：{m} 近期 {bm['count']} 笔，胜率仅 {bm['win_rate']:.1%}\n"
            f"   预期效果：减少低质量信号，提升该策略胜率\n"
            f"   风险提示：信号频次会降低，短期开仓减少"
        )

    if not suggestions:
        lines.append("【参数建议】当前参数表现正常，建议维持现状")
    else:
        lines.append("【参数建议】\n" + "\n\n".join(suggestions))

    # ── 维持不变 ─────────────────────────────────────────────────────────────
    good_modes = [m for m, bm in by_mode.items()
                  if bm.get("count", 0) >= 5 and bm.get("win_rate", 0) >= 0.55]
    if good_modes:
        lines.append(f"【维持不变】{', '.join(good_modes)} 策略表现稳定，参数无需调整")
    else:
        lines.append("【维持不变】样本量尚不足以做出稳定判断，建议继续观察")

    return "\n\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Telegram 推送
# ─────────────────────────────────────────────────────────────────────────────

def _send_telegram(text: str) -> bool:
    """通过 Telegram Bot API 推送报告。"""
    token   = os.environ.get("TELEGRAM_BOT_TOKEN")  or _get_cfg("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")    or _get_cfg("TELEGRAM_CHAT_ID",    "")
    if not token or not chat_id:
        logger.warning("[ai_advisor] 未配置 TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID，跳过推送")
        return False

    import urllib.request, urllib.parse
    url     = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({
        "chat_id":    chat_id,
        "text":       text,
        "parse_mode": "HTML",
    }).encode()
    try:
        req = urllib.request.Request(url, data=payload,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=15):
            pass
        return True
    except Exception as e:
        logger.warning(f"[ai_advisor] Telegram 推送失败: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# 日志记录
# ─────────────────────────────────────────────────────────────────────────────

def _log_result(log_path: Path, date_str: str, report: str,
                stats: dict, sent: bool) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "date":       date_str,
        "ts":         datetime.now(timezone.utc).isoformat(),
        "sent":       sent,
        "total_trades": stats.get("total_trades", 0),
        "overall_wr":   stats.get("overall_win_rate"),
        "overall_pnl":  stats.get("overall_pnl"),
        "report_len":   len(report),
        # 不存全文（太长），只存摘要；全文在 Telegram 里
        "report_head":  report[:300],
        # 用户决策在此追加（由外部写入）
        "user_decision": None,
    }
    from shared.trade_log_io import append_jsonl_bg as _bg_append   # [refactor-B3] 后台写入
    _bg_append(log_path, entry)


# ─────────────────────────────────────────────────────────────────────────────
# 公开入口
# ─────────────────────────────────────────────────────────────────────────────

def analyze(date_str: Optional[str] = None) -> None:
    """
    分析最近 N 天数据并推送 Telegram。
    date_str: 本次归档日期（YYYY-MM-DD），默认今天 UTC。
    """
    try:
        import config as cfg
        if not getattr(cfg, "AI_ADVISOR_ENABLED", True):
            logger.info("[ai_advisor] AI_ADVISOR_ENABLED=False，跳过")
            return

        archive_dir  = Path(getattr(cfg, "ARCHIVE_DIR",
                                     Path(__file__).parent / "archive" / "trades"))
        lookback     = int(getattr(cfg, "AI_ADVISOR_LOOKBACK_DAYS", 7))
        log_path     = Path(getattr(cfg, "AI_ADVISOR_LOG",
                                     Path(__file__).parent / "logs" / "advisor_log.jsonl"))
    except Exception as e:
        logger.error(f"[ai_advisor] 读取 config 失败: {e}")
        return

    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logger.info(f"[ai_advisor] 开始分析，回溯 {lookback} 天，归档目录: {archive_dir}")

    # 1. 读取历史数据
    daily_list = _load_archive_data(archive_dir, lookback)
    if not daily_list:
        logger.info("[ai_advisor] 无历史归档数据，跳过分析")
        return

    # 2. 聚合统计
    stats = _aggregate(daily_list)
    if not stats or stats.get("total_trades", 0) < 3:
        logger.info(f"[ai_advisor] 有效交易笔数不足（{stats.get('total_trades', 0)} 笔），跳过分析")
        return

    # 3. 获取当前参数快照
    current_params = _get_current_params()

    # 4. 调用 LLM / 规则引擎
    report = _call_llm(stats, current_params)
    if not report:
        logger.warning("[ai_advisor] 生成报告失败")
        return

    # 5. 拼装 Telegram 消息
    period = stats.get("analysis_period_days", lookback)
    total  = stats.get("total_trades", 0)
    wr     = stats.get("overall_win_rate", 0)
    pnl    = stats.get("overall_pnl", 0)

    header = (
        f"🤖 <b>每日 AI 参数分析</b>（近 {period} 天 / {total} 笔）\n"
        f"胜率 {wr:.1%} | PnL {pnl:+.2f}U | {date_str}\n"
        f"{'─'*30}\n"
    )
    footer = (
        f"\n{'─'*30}\n"
        "回复「采纳1」「采纳2」「采纳3」或「全忽略」\n"
        "<i>⚠️ AI 建议仅供参考，参数修改需人工确认后由系统执行</i>"
    )
    full_msg = header + report + footer

    # Telegram 单条消息上限 4096 字符
    if len(full_msg) > 4000:
        full_msg = full_msg[:3980] + "\n…（报告过长已截断）" + footer

    # 6. 推送
    sent = _send_telegram(full_msg)
    logger.info(f"[ai_advisor] 报告生成完毕，Telegram 推送: {'成功' if sent else '失败（见日志）'}")

    # 7. 记录日志
    _log_result(log_path, date_str, report, stats, sent)
