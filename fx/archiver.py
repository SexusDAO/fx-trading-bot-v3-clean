"""
archiver.py — 每日诊断快照生成
触发点：
  1. position_manager._generate_daily_report() 末尾调用 archive_daily()（每天 00:00 UTC）
  2. main.py 收到 SIGUSR1 信号时调用 archive_daily(trigger="manual")
  
输出：精简版诊断快照 JSON（2-5KB），用于快速定位问题
"""
from __future__ import annotations

import gc
import json
import logging
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("archiver")

_FX_ROOT = Path(__file__).parent


def _get_bot_version() -> str:
    """从 main.py 头部注释提取版本号"""
    try:
        main_py = _FX_ROOT / "main.py"
        if main_py.exists():
            text = main_py.read_text(encoding="utf-8")
            m = re.search(r"V(\d+\.\d+)", text)
            if m:
                return f"V{m.group(1)}"
    except Exception:
        pass
    return "unknown"


def _get_git_commit() -> str:
    """获取当前 HEAD commit 前 8 位"""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short=8", "HEAD"],
            cwd=str(_FX_ROOT),
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return "unknown"


def _get_uptime_hours() -> float:
    """从 /proc 计算 main.py 进程运行时长（小时）"""
    try:
        import os
        pid_file = _FX_ROOT / "logs" / "main.pid"
        if not pid_file.exists():
            return 0.0
        pid = int(pid_file.read_text().strip())
        
        # 读进程启动时间（clock ticks since boot）
        stat_file = f"/proc/{pid}/stat"
        with open(stat_file) as f:
            fields = f.read().split(")")[-1].split()
            starttime_ticks = int(fields[19])  # 22nd field
        
        # 读系统 uptime
        with open("/proc/uptime") as f:
            uptime_sec = float(f.read().split()[0])
        
        clk_tck = os.sysconf("SC_CLK_TCK")
        proc_start_sec = starttime_ticks / clk_tck
        proc_uptime_sec = uptime_sec - proc_start_sec
        return round(proc_uptime_sec / 3600, 1)
    except Exception:
        return 0.0


def _get_restart_info() -> tuple[int, str]:
    """从 daemon.log 提取最近 24h 重启次数和最后一次重启原因"""
    restart_count = 0
    last_reason = "unknown"
    try:
        daemon_log = _FX_ROOT / "logs" / "daemon.log"
        if not daemon_log.exists():
            return 0, "unknown"
        
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=24)
        
        with open(daemon_log, encoding="utf-8") as f:
            for line in f:
                # 解析时间戳
                m = re.match(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) UTC\]", line)
                if not m:
                    continue
                ts_str = m.group(1)
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                if ts < cutoff:
                    continue
                
                # 统计重启
                if "启动 main.py" in line:
                    restart_count += 1
                elif "内存" in line and "超过" in line:
                    last_reason = "memory_guard"
                elif "退出" in line and "重启" in line:
                    last_reason = "crash_restart"
    except Exception as e:
        logger.warning(f"[archiver] 读 daemon.log 失败: {e}")
    
    return restart_count, last_reason


def _get_ws_channel_status() -> dict:
    """获取 WS 三通道状态"""
    try:
        # 尝试从 ws_listener 实例获取
        import sys
        for obj in gc.get_objects():
            if type(obj).__name__ == "WsOrderListener":
                return obj.get_channel_status()
    except Exception:
        pass
    
    # fallback: 从 system.log 解析最近的连接状态
    try:
        system_log = _FX_ROOT / "logs" / "system.log"
        if not system_log.exists():
            return {"userData": False, "forceOrder": False, "markPrice": False}
        
        status = {"userData": False, "forceOrder": False, "markPrice": False}
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=10)
        
        with open(system_log, encoding="utf-8") as f:
            for line in f:
                m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                if not m:
                    continue
                ts_str = m.group(1)
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                if ts < cutoff:
                    continue
                
                if "userData 连接" in line or "userData 重连成功" in line:
                    status["userData"] = True
                elif "userData 异常" in line:
                    status["userData"] = False
                
                if "forceOrder 连接" in line or "强平事件流已连接" in line:
                    status["forceOrder"] = True
                elif "forceOrder 异常" in line:
                    status["forceOrder"] = False
                
                if "markPrice 连接" in line:
                    status["markPrice"] = True
                elif "markPrice 异常" in line:
                    status["markPrice"] = False
    except Exception as e:
        logger.warning(f"[archiver] 读 system.log 失败: {e}")
    
    return status


def _parse_scan_stats_24h() -> dict:
    """从 system.log 解析最近 24h 的扫描统计"""
    stats = {
        "total_rounds": 0,
        "raw_signals": 0,
        "signals_entered_watch": 0,
        "signals_triggered": 0,
        "precheck_reject_reasons": {},
        "score_distribution": {"below_40": 0, "40_to_48": 0, "48_to_50": 0, "above_50": 0},
    }
    
    try:
        system_log = _FX_ROOT / "logs" / "system.log"
        if not system_log.exists():
            return stats
        
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=24)
        
        with open(system_log, encoding="utf-8") as f:
            for line in f:
                m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                if not m:
                    continue
                ts_str = m.group(1)
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                if ts < cutoff:
                    continue
                
                # 扫描轮次
                if "候选池刷新完成" in line:
                    stats["total_rounds"] += 1
                
                # 原始信号数（从心跳日志提取）
                m_raw = re.search(r"原始信号(\d+)个", line)
                if m_raw:
                    stats["raw_signals"] += int(m_raw.group(1))
                
                # 进入监控数
                m_watch = re.search(r"进入监控(\d+)个", line)
                if m_watch:
                    stats["signals_entered_watch"] += int(m_watch.group(1))
                
                # 触发挂单数
                m_triggered = re.search(r"成交(\d+)笔", line)
                if m_triggered:
                    stats["signals_triggered"] += int(m_triggered.group(1))
                
                # 预检拒绝原因
                if "JUDGE_REJECT" in line:
                    # 提取拒绝原因
                    m_reason = re.search(r"JUDGE_REJECT\].*?:\s*(.+)$", line)
                    if m_reason:
                        reason = m_reason.group(1).strip()
                        # 简化原因分类
                        if "流动性" in reason or "liquidity" in reason.lower():
                            key = "流动性不足"
                        elif "已有持仓" in reason or "already" in reason.lower():
                            key = "已有持仓"
                        elif "熔断" in reason or "circuit" in reason.lower():
                            key = "熔断中"
                        elif "波动率" in reason or "ATR" in reason:
                            key = "波动率不符"
                        elif "EMA" in reason or "趋势" in reason or "空头结构" in reason or "多头排列" in reason or "下跌通道" in reason:
                            key = "趋势不符"
                        elif "量能" in reason or "vol_ratio" in reason or "放量" in reason:
                            key = "量能不足"
                        elif "方向不明确" in reason:
                            key = "方向不明"
                        elif "脉冲" in reason or "大阳" in reason:
                            key = "脉冲保护"
                        elif "未找到" in reason or "无有效" in reason:
                            key = "无止跌/止涨信号"
                        elif "不在回踩区间" in reason or "价格" in reason:
                            key = "价格位置不符"
                        elif "RSI" in reason or "超卖" in reason or "超买" in reason:
                            key = "RSI异常"
                        else:
                            key = "其他"
                        stats["precheck_reject_reasons"][key] = stats["precheck_reject_reasons"].get(key, 0) + 1
                
                # 评分分布（从 final_score 提取）
                m_score = re.search(r"final_score[=:]?\s*(\d+)", line)
                if m_score:
                    score = int(m_score.group(1))
                    if score < 40:
                        stats["score_distribution"]["below_40"] += 1
                    elif score < 48:
                        stats["score_distribution"]["40_to_48"] += 1
                    elif score < 50:
                        stats["score_distribution"]["48_to_50"] += 1
                    else:
                        stats["score_distribution"]["above_50"] += 1
    except Exception as e:
        logger.warning(f"[archiver] 解析扫描统计失败: {e}")
    
    return stats


def _parse_error_summary_24h() -> dict:
    """从 system.log 解析最近 24h 的错误摘要"""
    error_counts: dict[str, dict] = {}
    ws_disconnect = 0
    api_401 = 0
    api_timeout = 0
    
    try:
        system_log = _FX_ROOT / "logs" / "system.log"
        if not system_log.exists():
            return {"top_errors": [], "ws_disconnect_count": 0, "api_401_count": 0, "api_timeout_count": 0}
        
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=24)
        
        with open(system_log, encoding="utf-8") as f:
            for line in f:
                m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                if not m:
                    continue
                ts_str = m.group(1)
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                if ts < cutoff:
                    continue
                
                if "ERROR" in line or "WARNING" in line:
                    # WS 断连
                    if "异常" in line and ("后重连" in line or "重连" in line):
                        ws_disconnect += 1
                    
                    # API 401
                    if "401" in line or "Invalid API-key" in line:
                        api_401 += 1
                    
                    # API timeout
                    if "timeout" in line.lower() or "超时" in line:
                        api_timeout += 1
                    
                    # 提取错误摘要
                    m_err = re.search(r"(?:ERROR|WARNING)\s+\S+\s+(.+)$", line)
                    if m_err:
                        err_msg = m_err.group(1).strip()
                        # 截断过长的错误信息
                        if len(err_msg) > 100:
                            err_msg = err_msg[:100] + "..."
                        
                        if err_msg not in error_counts:
                            error_counts[err_msg] = {"count": 0, "last_seen": ts_str}
                        error_counts[err_msg]["count"] += 1
                        error_counts[err_msg]["last_seen"] = ts_str
    except Exception as e:
        logger.warning(f"[archiver] 解析错误摘要失败: {e}")
    
    # 取 top 5
    top_errors = sorted(error_counts.items(), key=lambda x: x[1]["count"], reverse=True)[:5]
    top_errors_list = [
        {"message": msg, "count": data["count"], "last_seen": data["last_seen"]}
        for msg, data in top_errors
    ]
    
    return {
        "top_errors": top_errors_list,
        "ws_disconnect_count": ws_disconnect,
        "api_401_count": api_401,
        "api_timeout_count": api_timeout,
    }


def _get_key_config() -> dict:
    """读取关键配置开关"""
    try:
        from config import (
            OPT_OID_REJECT_ALL_BULL,
            STOP_HUNT_ENABLED,
            ENABLE_USDC_PAIRS,
            OPT_LIQ_RELATIVE_STRENGTH,
            LIQUIDITY_GATE_MIN_U,
        )
        # 尝试读取 SCORE_THRESHOLD（可能不存在）
        try:
            from config import SCORE_THRESHOLD
        except ImportError:
            SCORE_THRESHOLD = 50  # 默认值
        
        return {
            "OPT_OID_REJECT_ALL_BULL": OPT_OID_REJECT_ALL_BULL,
            "STOP_HUNT_ENABLED": STOP_HUNT_ENABLED,
            "ENABLE_USDC_PAIRS": ENABLE_USDC_PAIRS,
            "OPT_LIQ_RELATIVE_STRENGTH": OPT_LIQ_RELATIVE_STRENGTH,
            "LIQUIDITY_GATE_MIN_U": LIQUIDITY_GATE_MIN_U,
            "SCORE_THRESHOLD": SCORE_THRESHOLD,
        }
    except Exception as e:
        logger.warning(f"[archiver] 读取配置失败: {e}")
        return {}


def _get_trade_summary(date_str: str) -> dict:
    """从 trade_log.jsonl 提取指定日期的交易摘要"""
    try:
        trade_log_path = _FX_ROOT / "logs" / "trade_log.jsonl"
        if not trade_log_path.exists():
            return {"total_trades": 0, "win": 0, "loss": 0, "win_rate": 0.0, "total_pnl_usdt": 0.0, "by_mode": {}}
        
        # 计算日期范围（北京时间）
        bj_tz = timezone(timedelta(hours=8))
        day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=bj_tz)
        start_ms = int(day.timestamp() * 1000)
        end_ms = start_ms + 86400_000 - 1
        
        trades = []
        with open(trade_log_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    ct = r.get("close_time", 0)
                    if start_ms <= ct <= end_ms:
                        trades.append(r)
                except Exception:
                    continue
        
        total = len(trades)
        wins = [t for t in trades if (t.get("pnl_usdt") or 0) > 0]
        losses = [t for t in trades if (t.get("pnl_usdt") or 0) <= 0]
        total_pnl = sum(t.get("pnl_usdt", 0) for t in trades)
        win_rate = len(wins) / total if total else 0.0
        
        by_mode: dict = {}
        for t in trades:
            m = t.get("mode") or t.get("sig_type") or "unknown"
            pnl = t.get("pnl_usdt", 0) or 0
            if m not in by_mode:
                by_mode[m] = {"count": 0, "win": 0, "pnl": 0.0}
            by_mode[m]["count"] += 1
            by_mode[m]["pnl"] += pnl
            if pnl > 0:
                by_mode[m]["win"] += 1
        
        for d in by_mode.values():
            d["pnl"] = round(d["pnl"], 2)
        
        return {
            "total_trades": total,
            "win": len(wins),
            "loss": len(losses),
            "win_rate": round(win_rate, 4),
            "total_pnl_usdt": round(total_pnl, 2),
            "by_mode": by_mode,
        }
    except Exception as e:
        logger.warning(f"[archiver] 交易摘要计算失败: {e}")
        return {"total_trades": 0, "win": 0, "loss": 0, "win_rate": 0.0, "total_pnl_usdt": 0.0, "by_mode": {}}


def _get_open_positions() -> list:
    """获取当前持仓快照（精简版）"""
    positions = []
    try:
        from shared.state import get_all_positions
        all_pos = get_all_positions()
        now_ms = datetime.now(timezone.utc).timestamp() * 1000
        
        for p in all_pos:
            if p.get("status") != "open":
                continue
            
            entry_price = p.get("entry_price", 0)
            current_price = p.get("current_price") or p.get("mark_price") or entry_price
            direction = (p.get("direction") or "long").upper()
            
            # 计算浮盈
            unrealized_pnl = 0.0
            if entry_price and current_price:
                if direction == "LONG":
                    unrealized_pnl = (current_price - entry_price) / entry_price * 100
                else:
                    unrealized_pnl = (entry_price - current_price) / entry_price * 100
            
            # 计算持仓时长
            open_time = p.get("open_time", 0)
            holding_hours = round((now_ms - open_time) / 3600000, 1) if open_time else 0.0
            
            positions.append({
                "symbol": p.get("symbol"),
                "direction": direction,
                "mode": p.get("mode"),
                "entry_price": entry_price,
                "sl_price": p.get("sl_price"),
                "current_price": current_price,
                "unrealized_pnl_usdt": round(unrealized_pnl, 2),
                "holding_hours": holding_hours,
                "tp1_filled": p.get("tp1_filled", False),
            })
    except Exception as e:
        logger.warning(f"[archiver] 读持仓快照失败: {e}")
    
    return positions


def archive_daily(date_str: str | None = None, trigger: str = "scheduled") -> bool:
    """
    生成精简版诊断快照 JSON。
    date_str: 指定日期（YYYY-MM-DD），默认为北京时间昨天。
    trigger: "scheduled"（定时）或 "manual"（手动）。
    返回 True 表示成功，False 表示失败。
    """
    try:
        from config import ARCHIVE_DIR
    except Exception as e:
        logger.warning(f"[archiver] config import 失败: {e}")
        return False
    
    # 默认日期：北京时间昨天
    if not date_str:
        now_bj = datetime.now(timezone.utc) + timedelta(hours=8)
        date_str = (now_bj - timedelta(days=1)).strftime("%Y-%m-%d")
    
    logger.info(f"[archiver] 开始生成诊断快照，日期={date_str}, trigger={trigger}")
    
    # 组装快照
    snapshot = {
        "meta": {
            "date": date_str,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "trigger": trigger,
            "bot_version": _get_bot_version(),
            "git_commit": _get_git_commit(),
            "uptime_hours": _get_uptime_hours(),
        },
        "process_health": {
            "ws_channels": _get_ws_channel_status(),
            "restart_count_24h": _get_restart_info()[0],
            "last_restart_reason": _get_restart_info()[1],
        },
        "scan_stats_24h": _parse_scan_stats_24h(),
        "trade_summary": _get_trade_summary(date_str),
        "open_positions": _get_open_positions(),
        "error_summary_24h": _parse_error_summary_24h(),
        "key_config": _get_key_config(),
    }
    
    # 写文件
    try:
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        out_path = ARCHIVE_DIR / f"{date_str}.json"
        out_path.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(f"[archiver] 快照已写入 {out_path}")
    except Exception as e:
        logger.error(f"[archiver] 写文件失败: {e}")
        return False
    
    # 推送通知
    try:
        from shared.notifier import notify
        total_trades = snapshot["trade_summary"]["total_trades"]
        total_rounds = snapshot["scan_stats_24h"]["total_rounds"]
        raw_signals = snapshot["scan_stats_24h"]["raw_signals"]
        error_count = len(snapshot["error_summary_24h"]["top_errors"])
        
        msg = (
            f"📸 诊断快照已生成 {date_str}\n"
            f"扫描{total_rounds}轮 / 信号{raw_signals}个 / "
            f"交易{total_trades}笔 / 错误{error_count}条"
        )
        notify(msg)
        logger.info(f"[archiver] 通知已发送: {msg}")
    except Exception as e:
        logger.warning(f"[archiver] 通知发送失败（非致命）: {e}")
    
    # 触发 AI 参数顾问（异步，不阻塞）
    try:
        from ai_advisor import analyze as _ai_analyze
        import threading
        t = threading.Thread(
            target=_ai_analyze,
            args=(date_str,),
            daemon=True,
            name="ai_advisor",
        )
        t.start()
        logger.info("[archiver] AI 参数顾问已在后台启动")
    except Exception as e:
        logger.warning(f"[archiver] AI 参数顾问启动失败（非致命）: {e}")
    
    return True
