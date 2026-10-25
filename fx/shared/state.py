"""
shared/state.py — 全局状态管理中心（V3.0 完整版）

管理五个状态文件：
    pos_state.json          — 持仓 + 挂单记录
    circuit_breaker.json    — 熔断器
    cooldown_state.json     — 冷却期
    pending_confirm_state.json — breakout 待确认
    oi_cache.json           — OI 历史缓存

所有读写：线程锁 + 原子写入（先写唯一临时文件再替换）。
[v3.89 #10] 线程锁只保证进程内线程安全，不保证跨进程；多进程同时读改写同一文件仍可能丢更新。
"""

import json
import os
import threading
import time
import uuid
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("fx.state")

# ── 路径 ──────────────────────────────────────────────────────────────────────
_BASE = Path(__file__).parent.parent / "logs"

def _p(name: str) -> Path:
    return _BASE / name

_POS_FILE     = _p("pos_state.json")
_CB_FILE      = _p("circuit_breaker.json")
_CD_FILE      = _p("cooldown_state.json")
_PC_FILE      = _p("pending_confirm_state.json")
_OI_FILE      = _p("oi_cache.json")

OI_CACHE_KEEP = 48   # 每30分钟一条 → 48条 ≈ 24H

# ── 锁 ────────────────────────────────────────────────────────────────────────
_locks = {
    "pos":     threading.Lock(),
    "cb":      threading.Lock(),
    "cd":      threading.Lock(),
    "pc":      threading.Lock(),
    "oi":      threading.Lock(),
}


# ═══════════════════════════════════════════════════════════════════════════════
# 原子读写工具
# ═══════════════════════════════════════════════════════════════════════════════
# [v3.89 #11] 持仓文件损坏标志：损坏时置 True，add_position / update_position 拒绝写入。
#   仅模块级内存标志，进程重启后恢复 False。
_POS_STATE_CORRUPTED = False


def _corrupt_marker(path: Path) -> Path:
    return path.with_name(path.name + ".CORRUPTED")


def _backup_file(path: Path, tag: str = "corrupted") -> Path:
    """[fix #15a] 复制保留原文件；失败抛异常（调用方不得继续覆盖原文件）。"""
    import shutil
    backup = path.with_name(f"{path.name}.{tag}_{int(time.time() * 1000)}")
    shutil.copy2(path, backup)
    return backup


def _backup_corrupted_pos(path: Path, err) -> None:
    """[v3.89 #11 / fix #5] 把损坏的持仓文件备份保留，并持久化损坏标志（文件 + 内存）。
    备份失败 → 抛异常，原文件保持不动（fix #15a）。"""
    global _POS_STATE_CORRUPTED
    _POS_STATE_CORRUPTED = True
    try:
        _corrupt_marker(path).write_text(
            f"{datetime.now(timezone.utc).isoformat()} {err}\n删除本文件前请人工核对持仓后再清除。\n",
            encoding="utf-8")
    except Exception as me:
        logger.error(f"[state] 写入损坏标志文件失败: {me}")
    try:
        backup = _backup_file(path, "corrupted")
    except Exception as e:
        logger.critical(f"[state] {path.name} 已损坏且备份失败（原文件保持不动，不覆盖）: {err} / 备份错误: {e}")
        raise
    logger.critical(f"[state] {path.name} 已损坏/为空，已备份为 {backup.name}，持仓写入已被阻止，"
                    f"需人工核对交易所持仓后删除 {_corrupt_marker(path).name} 方可恢复: {err}")


# 进程重启后仍保持损坏标志（fix #5b）
try:
    _POS_STATE_CORRUPTED = _corrupt_marker(_POS_FILE).exists()
except Exception:
    pass


def _read_json(path: Path, default):
    # [v3.89 #11] 仅持仓文件区分「不存在」与「损坏」；其他状态文件保持原有行为。
    if "pos_state" in path.name and not path.name.endswith(".CORRUPTED"):
        if not path.exists():
            return default
        try:
            raw = path.read_text(encoding="utf-8")
            if not raw.strip():          # [fix #5a] 0 字节/仅空白 = 损坏，不是合法空仓
                raise ValueError("pos_state 文件为空（0字节或仅空白）")
            return json.loads(raw)
        except Exception as e:
            try:
                _backup_corrupted_pos(path, e)
            except Exception:
                pass
            return default
    try:
        if not path.exists():
            return default
        raw = path.read_text(encoding="utf-8").strip()
        return json.loads(raw) if raw else default
    except Exception as e:
        logger.error(f"[state] 读取 {path.name} 失败: {e}")
        return default


def _write_json(path: Path, data, compact: bool = False) -> None:
    """原子写入：先写唯一临时文件再替换。进程内线程安全，不保证跨进程。
    compact=True 时不缩进（体积大的缓存文件用，序列化更快、文件更小）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    # [v3.89 #10] 原临时文件名固定为 .tmp，两个写者会互相覆盖临时文件。改为 pid+毫秒+随机串唯一命名，
    #   替换失败时删除临时文件，不留残留。
    tmp = path.with_suffix(f".tmp_{os.getpid()}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}")
    try:
        _txt = (json.dumps(data, separators=(",", ":"), ensure_ascii=False) if compact
                else json.dumps(data, indent=2, ensure_ascii=False))
        tmp.write_text(_txt, encoding="utf-8")
        tmp.replace(path)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# pos_state.json — 持仓 + 挂单
# ═══════════════════════════════════════════════════════════════════════════════
def _load_pos() -> list:
    return _read_json(_POS_FILE, [])


def _save_pos(data: list) -> None:
    _write_json(_POS_FILE, data)


# ── 持仓接口 ──────────────────────────────────────────────────────────────────
def get_all_positions() -> list:
    """返回所有记录（含 pending/open 两种 status）"""
    with _locks["pos"]:
        data = _load_pos()
    return sorted(data, key=lambda p: p.get("open_time", 0))


def get_all_pending_orders() -> list:
    """返回所有 status=pending 的挂单"""
    return [p for p in get_all_positions() if p.get("status") == "pending"]


def get_position(symbol: str, include_patching: bool = False) -> dict | None:
    """
    返回指定标的的 open 持仓，多条时取最新。

    include_patching=True：同时包含 status='patching' 的记录（供巡棄诊断区分「找不到仓位」和「正在补挂」）。
    问题36 记录在案：patching 状态持续时间很短，默认不包含避免巡棄误报。
    """
    statuses = ("open", "patching") if include_patching else ("open",)
    results = [p for p in get_all_positions()
               if p.get("symbol") == symbol and p.get("status") in statuses]
    if not results:
        return None
    if len(results) > 1:
        logger.error(f"[state] {symbol} 存在多条持仓记录，取最新")
    return max(results, key=lambda p: p.get("open_time", 0))


def get_pending_order(symbol: str) -> dict | None:
    results = [p for p in get_all_positions()
               if p.get("symbol") == symbol and p.get("status") == "pending"]
    return results[0] if results else None


def add_position(record: dict) -> bool:
    if _POS_STATE_CORRUPTED:  # [v3.89 #11]
        logger.error("[state] 持仓文件曾损坏，拒绝写入，请人工检查")
        return False
    sym = record.get("symbol", "")
    with _locks["pos"]:
        data = _load_pos()
        # 检查是否已有同标的持仓（包括 open / pending / patching，防止补挂进行中资已开仓的标的被二次开仓）
        existing = [p for p in data if p.get("symbol") == sym
                    and p.get("status") in ("open", "pending", "patching", "pending_guard")]
        if existing:
            logger.error(f"[state] add_position 拒绝：{sym} 已有记录")
            return False
        data.append(record)
        _save_pos(data)
    return True


def add_pending_order(record: dict) -> bool:
    """新增挂单记录"""
    return add_position(record)   # 复用，status 字段由调用方填 "pending"


def update_position(pos_id: str, updates: dict) -> bool:
    if _POS_STATE_CORRUPTED:  # [v3.89 #11]
        logger.error("[state] 持仓文件曾损坏，拒绝写入，请人工检查")
        return False
    with _locks["pos"]:
        data = _load_pos()
        for i, p in enumerate(data):
            if p.get("pos_id") == pos_id:
                data[i].update(updates)
                _save_pos(data)
                return True
    logger.warning(f"[state] update_position: pos_id={pos_id} 不存在")
    return False


def update_pending_order(symbol: str, updates: dict) -> bool:
    """按 symbol 更新挂单"""
    if _POS_STATE_CORRUPTED:  # [v3.89 #11b]
        logger.error("[state] 持仓文件曾损坏，拒绝写入: update_pending_order")
        return False
    with _locks["pos"]:
        data = _load_pos()
        for i, p in enumerate(data):
            if p.get("symbol") == symbol and p.get("status") == "pending":
                data[i].update(updates)
                _save_pos(data)
                return True
    return False


def remove_position(pos_id: str) -> bool:
    """按 pos_id 删除记录，无论 status 是什么（open/pending/patching 均可删除）。"""
    if _POS_STATE_CORRUPTED:  # [v3.89 #11b]
        logger.error("[state] 持仓文件曾损坏，拒绝写入: remove_position")
        return False
    with _locks["pos"]:
        data = _load_pos()
        new  = [p for p in data if p.get("pos_id") != pos_id]
        if len(new) == len(data):
            return True   # 不存在也算成功
        _save_pos(new)
    return True


def remove_position_by_symbol(symbol: str, status: str | None = None) -> int:
    """
    按 symbol（可选 status 过滤）删除记录，返回删除数量。
    用于清理孤儿 patching 记录：remove_position_by_symbol(sym, status='patching')
    当 status=None 时删除该 symbol 下所有记录。
    """
    if _POS_STATE_CORRUPTED:  # [v3.89 #11b]
        logger.error("[state] 持仓文件曾损坏，拒绝写入: remove_position_by_symbol")
        return 0
    with _locks["pos"]:
        data = _load_pos()
        if status is not None:
            new = [p for p in data if not (p.get("symbol") == symbol and p.get("status") == status)]
        else:
            new = [p for p in data if p.get("symbol") != symbol]
        removed = len(data) - len(new)
        if removed:
            _save_pos(new)
            logger.info(f"[state] remove_position_by_symbol: {symbol} status={status} 删除 {removed} 条")
    return removed


def remove_pending_order(symbol: str) -> bool:
    if _POS_STATE_CORRUPTED:  # [v3.89 #11b]
        logger.error("[state] 持仓文件曾损坏，拒绝写入: remove_pending_order")
        return False
    with _locks["pos"]:
        data = _load_pos()
        new  = [p for p in data if not (p.get("symbol") == symbol
                                        and p.get("status") == "pending")]
        _save_pos(new)
    return True


def get_position_count() -> dict:
    """返回 {total, long, short}：只统计 open + patching（真实持仓，不含 pending 挂单）。
    v3.72: pending 挂单从此函数中移除，改由 get_pending_count() 独立统计，
    与 scanner.py v3.53 的槽位计算逻辑保持一致。
    v3.90: TP1 已成交（tp1_filled=True）的仓位只剩 TP2 尾仓，不再占用槽位；
           其名义仍由 risk_manager.calc_leverage_remaining 计入杠杆预算 used。
    """
    positions = [p for p in get_all_positions()
                 if p.get("status") in ("open", "patching") and not p.get("tp1_filled")]
    long_cnt  = sum(1 for p in positions if p.get("direction", "").upper() == "LONG")
    short_cnt = sum(1 for p in positions if p.get("direction", "").upper() == "SHORT")
    return {"total": len(positions), "long": long_cnt, "short": short_cnt}


def get_pending_count() -> dict:
    """返回 {total, long, short}：只统计 pending 挂单（未成交的入场限价单）。
    v3.72: 挂单槽位与持仓槽位独立管理，由 signal_watcher 竞争机制控制上限。
    """
    positions = [p for p in get_all_positions()
                 if p.get("status") == "pending"]
    long_cnt  = sum(1 for p in positions if p.get("direction", "").upper() == "LONG")
    short_cnt = sum(1 for p in positions if p.get("direction", "").upper() == "SHORT")
    return {"total": len(positions), "long": long_cnt, "short": short_cnt}


def get_symbols_with_activity() -> list:
    """返回所有有持仓或挂单的标的名称列表（去重）"""
    data = get_all_positions()
    return list({p["symbol"] for p in data if "symbol" in p})


def get_pending_orders() -> list:
    """便捷包装，同 get_all_pending_orders"""
    return get_all_pending_orders()


# ═══════════════════════════════════════════════════════════════════════════════
# circuit_breaker.json — 熔断器
# ═══════════════════════════════════════════════════════════════════════════════
_CB_DEFAULT = {
    "is_open":          False,
    "daily_loss":       0.0,   # = max(0, -(daily_pnl - daily_fees - daily_funding))
    "daily_pnl":        0.0,   # 当日净已实现盈亏（正负都累计，不含费用）
    "daily_fees":       0.0,   # 当日手续费（支出）
    "daily_funding":    0.0,   # 当日资金费（支出为正）
    "last_reset_date":  "",
    "triggered_at":     None,
    "triggered_reason": None,
    "notified":         False,  # 熔断推送去重：True 表示本次触发已推送，重启后不重复推
}


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _load_cb() -> dict:
    d = _read_json(_CB_FILE, {})
    return {**_CB_DEFAULT, **d}


def _save_cb(d: dict) -> None:
    _write_json(_CB_FILE, d)


def is_circuit_open() -> bool:
    with _locks["cb"]:
        d = _load_cb()
        # 跨日自动重置
        if d.get("last_reset_date") != _today_utc():
            _reset_circuit_locked()
            return False
        if not d.get("is_open", False):
            return False
        # 双重保护：is_open=True 时再验证 daily_loss 是否真的超限
        # 防止旧缓存/错误写入导致的虚假熔断拦截开仓
        try:
            from config import CIRCUIT_BREAKER_DAILY_LOSS
            limit = float(CIRCUIT_BREAKER_DAILY_LOSS)
        except Exception:
            limit = 9999.0
        actual_loss = float(d.get("daily_loss", 0.0))
        if actual_loss < limit:
            # daily_loss 小于阈值，is_open 是广播或旧数据，自动修复
            logger.warning(f"[state] is_open=True 但 daily_loss={actual_loss:.1f}U < 限额{limit}U，自动修复熔断状态")
            _reset_circuit_locked()
            return False
        return True


def record_pnl(pnl: float, fee: float = 0.0, funding: float = 0.0) -> bool:
    """
    [fix #12] 记录净盈亏：盈利与亏损都累计（正负抵消）；手续费、资金费单独字段累计，
    并计入总亏损：daily_loss = max(0, -(daily_pnl - daily_fees - daily_funding))。
      pnl     已实现盈亏（正=盈利，负=亏损，不含手续费/资金费）
      fee     手续费（正数=支出）
      funding 资金费（正数=支出，负数=收入）
    返回本次是否触发熔断。
    """
    with _locks["cb"]:
        d = _load_cb()
        d["daily_pnl"]     = round(float(d.get("daily_pnl", 0.0)) + float(pnl), 6)
        d["daily_fees"]    = round(float(d.get("daily_fees", 0.0)) + abs(float(fee or 0.0)), 6)
        d["daily_funding"] = round(float(d.get("daily_funding", 0.0)) + float(funding or 0.0), 6)
        net = d["daily_pnl"] - d["daily_fees"] - d["daily_funding"]
        d["daily_loss"] = round(max(0.0, -net), 4)
        try:
            from config import CIRCUIT_BREAKER_DAILY_LOSS
            limit = float(CIRCUIT_BREAKER_DAILY_LOSS)
        except Exception:
            limit = 100.0
        triggered = False
        if d["daily_loss"] >= limit and not d.get("is_open"):
            d["is_open"]          = True
            d["triggered_at"]     = int(time.time() * 1000)
            d["triggered_reason"] = f"日净亏损 {d['daily_loss']:.1f}U 超限 {limit}U"
            d["notified"]         = False  # 待 risk_manager 推送后置 True
            triggered = True
            logger.warning(f"[state] 熔断触发：{d['triggered_reason']}")
        _save_cb(d)
    return triggered


def _reset_circuit_locked() -> None:
    """内部重置（已持有锁时调用）"""
    d = _CB_DEFAULT.copy()
    d["last_reset_date"] = _today_utc()
    _save_cb(d)
    logger.info("[state] 熔断器重置")


def reset_circuit() -> None:
    """外部调用（UTC 00:00 定时任务）"""
    with _locks["cb"]:
        _reset_circuit_locked()


def get_daily_loss() -> float:
    with _locks["cb"]:
        return float(_load_cb().get("daily_loss", 0.0))


# 向后兼容旧 risk_manager 调用
def record_loss(usd: float) -> None:
    record_pnl(-abs(usd))


# ═══════════════════════════════════════════════════════════════════════════════
# cooldown_state.json — 冷却期
# ═══════════════════════════════════════════════════════════════════════════════
def _load_cd() -> dict:
    return _read_json(_CD_FILE, {})


def _save_cd(d: dict) -> None:
    _write_json(_CD_FILE, d)


_BLACKLIST_MAX_AGE_SEC = 7 * 24 * 3600  # 黑名单最长保留 7 天

def is_in_cooldown(symbol: str) -> tuple[bool, str]:
    with _locks["cd"]:
        d    = _load_cd()
        entry = d.get(symbol)
        if not entry:
            return False, ""
        until  = entry.get("cooldown_until", 0)
        set_at = entry.get("set_at", 0)
        now_ms = int(time.time() * 1000)
        # 到期 或 超过黑名单最大保留期（7天） → 清除
        expired = now_ms >= until
        too_old = (now_ms - set_at) > _BLACKLIST_MAX_AGE_SEC * 1000 if set_at else False
        if expired or too_old:
            del d[symbol]
            _save_cd(d)
            return False, ""
        return True, entry.get("reason", "")


_CRASH_CD_MAX_MS = 8 * 3600 * 1000   # [fix-4] crash_guard 冷却绝对上限


def add_cooldown(symbol: str, duration_seconds: int, reason: str) -> None:
    now_ms = int(time.time() * 1000)
    until = now_ms + duration_seconds * 1000
    with _locks["cd"]:
        d = _load_cd()
        prev = d.get(symbol, {})
        existing = prev.get("cooldown_until", 0)
        _first = None
        if str(reason).startswith("crash_guard"):
            # [fix-4] 首次触发记 first_trigger_ts；仍在冷却内的 crash_guard 刷新沿用原首次时间
            _prev_first = prev.get("first_trigger_ts") if str(prev.get("reason", "")).startswith("crash_guard") \
                and existing > now_ms else None
            _first = int(_prev_first or now_ms)
            until = min(until, _first + _CRASH_CD_MAX_MS)
        if until > existing:
            d[symbol] = {
                "cooldown_until": until,
                "reason":         reason,
                "set_at":         now_ms,
            }
            if _first is not None:
                d[symbol]["first_trigger_ts"] = _first
            effective, kept = until, False
        else:
            # [v3.89 #97] 已有冷却更长：保留决定该期限的原因和设置时间，不让较短的新事件覆盖
            effective, kept = existing, True
        _save_cd(d)
    if kept:
        logger.info(f"[state] {symbol} 冷却 {duration_seconds}s(reason={reason}) 短于已有冷却，保留原因={prev.get('reason')}，"
                    f"实际到期 {effective}")
    else:
        logger.info(f"[state] {symbol} 冷却期 {duration_seconds}s reason={reason}，实际到期 {effective}")


# 向后兼容
def set_cooldown(symbol: str, hours: float, reason: str) -> None:
    add_cooldown(symbol, int(hours * 3600), reason)


def remove_cooldown(symbol: str) -> None:
    with _locks["cd"]:
        d = _load_cd()
        d.pop(symbol, None)
        _save_cd(d)


def get_all_cooldowns() -> list:
    now = time.time() * 1000
    with _locks["cd"]:
        d = _load_cd()
    result = []
    for sym, entry in d.items():
        remaining_ms = entry.get("cooldown_until", 0) - now
        if remaining_ms > 0:
            result.append({
                "symbol":       sym,
                "reason":       entry.get("reason", ""),
                "remaining_sec": remaining_ms / 1000,
            })
    return result


def clear_cooldown(symbol: str) -> None:
    remove_cooldown(symbol)


# ═══════════════════════════════════════════════════════════════════════════════
# open_dir_cooldown — v3.44 同标的同方向开仓冷却
# key 格式："{symbol}_{direction}_open_dir"
# 和止损冷却完全独立，两道都要过
# ═══════════════════════════════════════════════════════════════════════════════

_OPEN_DIR_FILE = _p("cooldown_state.json")  # 复用同一文件，不同 key


def _open_dir_key(symbol: str, direction: str) -> str:
    return f"{symbol}_{direction.upper()}_open_dir"


def is_open_dir_cooldown(symbol: str, direction: str) -> bool:
    """
    检查该标的该方向是否在开仓冷却期内。
    返回 True = 冷却中（拒绝开仓）。
    """
    key = _open_dir_key(symbol, direction)
    with _locks["cd"]:
        d     = _load_cd()
        entry = d.get(key)
        if not entry:
            return False
        until = entry.get("cooldown_until", 0)
        now_ms = int(time.time() * 1000)
        if now_ms >= until:
            del d[key]
            _save_cd(d)
            return False
        return True


def set_open_dir_cooldown(symbol: str, direction: str, hours: float) -> None:
    """
    成功开仓后调用，写入同方向冷却记录。
    hours: 冷却时长（默认 PM_COOLDOWN_OPEN_DIR_HOURS）
    """
    key    = _open_dir_key(symbol, direction)
    until  = int(time.time() * 1000) + int(hours * 3600 * 1000)
    with _locks["cd"]:
        d = _load_cd()
        # [v3.89 #98] 重复写入取 max(已有期限, 新期限)，不缩短已有的更长方向冷却
        existing = d.get(key, {}).get("cooldown_until", 0)
        if until > existing:
            d[key] = {
                "cooldown_until": until,
                "reason":         "open_dir",
                "set_at":         int(time.time() * 1000),
            }
            effective = until
        else:
            effective = existing
        _save_cd(d)
    logger.info(f"[state] {symbol} {direction.upper()} 开仓冷却 {hours}h 已写入，实际到期 {effective}")


# ═══════════════════════════════════════════════════════════════════════════════
def _load_pc() -> dict:
    return _read_json(_PC_FILE, {})


def _save_pc(d: dict) -> None:
    _write_json(_PC_FILE, d)


def add_pending_confirm(symbol: str, record: dict) -> None:
    with _locks["pc"]:
        d = _load_pc()
        if symbol in d:
            logger.info(f"[state] {symbol} pending_confirm 覆盖旧记录")
        d[symbol] = record
        _save_pc(d)


def get_pending_confirm(symbol: str) -> dict | None:
    with _locks["pc"]:
        return _load_pc().get(symbol)


def remove_pending_confirm(symbol: str) -> None:
    with _locks["pc"]:
        d = _load_pc()
        d.pop(symbol, None)
        _save_pc(d)


def get_all_pending_confirms() -> dict:
    with _locks["pc"]:
        return _load_pc()


def cleanup_expired_confirms() -> int:
    """清理已过期记录，返回清理数量"""
    now = time.time() * 1000
    with _locks["pc"]:
        d    = _load_pc()
        keep = {k: v for k, v in d.items() if v.get("expire_at", 0) > now}
        removed = len(d) - len(keep)
        if removed:
            _save_pc(keep)
    return removed


# ═══════════════════════════════════════════════════════════════════════════════
# oi_cache.json — OI 历史
# ═══════════════════════════════════════════════════════════════════════════════
_24H_SEC = 86_400
_15M_SEC = 900


# ── OI 缓存内存快照 ──────────────────────────────────────────────────────────
# 背景：scanner 并行分析每个标的都会调 get_oi_24h_ago / judges._get_oi_1h_ago / trigger_detector，
#   原先每次调用都整文件 exists + read + json.loads（get_oi_24h_ago 还解析两遍）；
#   多线程并发时 json.loads 长时间占用 GIL，事件循环被饿死。
# 现在：读走内存快照，TTL 内完全不碰磁盘，过期时只允许一个线程读盘；
#   写（update_oi / update_all_oi）读一份独占的磁盘副本、修改、落盘成功后整体替换快照（写穿）。
# 约定：_load_oi_cache() 返回的是共享快照，调用方只能读、不得修改；要修改请用 _load_oi_cache_disk()。
# 限制：只对单进程有效；其他进程改了文件，本进程最长 _OI_MEM_TTL 秒后才看到。
_OI_MEM_TTL = 60.0
_oi_mem: dict = {"path": None, "data": None, "ts": 0.0}
_oi_mem_lock = threading.Lock()        # 只保护 _oi_mem 字典读写（极短临界区）
_oi_refresh_lock = threading.Lock()    # 快照过期时只允许一个线程读盘，其余等待后复用


def _oi_mem_get():
    with _oi_mem_lock:
        if (_oi_mem["data"] is not None and _oi_mem["path"] == str(_OI_FILE)
                and time.monotonic() - _oi_mem["ts"] < _OI_MEM_TTL):
            return _oi_mem["data"]
    return None


def _oi_mem_set(data: dict) -> None:
    with _oi_mem_lock:
        _oi_mem["path"], _oi_mem["data"], _oi_mem["ts"] = str(_OI_FILE), data, time.monotonic()


def _oi_mem_reset() -> None:
    """清空内存快照（测试直接改文件后、或需要强制重读时使用）。"""
    with _oi_mem_lock:
        _oi_mem["path"], _oi_mem["data"], _oi_mem["ts"] = None, None, 0.0


def _load_oi_cache_disk() -> dict:
    """直接读盘，返回调用方独占的新对象（写路径用）。"""
    return _read_json(_OI_FILE, {})


def _load_oi_cache() -> dict:
    """读路径：返回内存快照（只读，调用方不得修改）。快照过期才读盘。"""
    d = _oi_mem_get()
    if d is not None:
        return d
    with _oi_refresh_lock:
        d = _oi_mem_get()              # 等锁期间别的线程可能已刷新
        if d is not None:
            return d
        d = _load_oi_cache_disk()
        _oi_mem_set(d)
        return d


def _save_oi(d: dict) -> None:
    _write_json(_OI_FILE, d, compact=True)
    _oi_mem_set(d)                     # 写穿：落盘成功后才发布为新快照（写盘抛异常则不更新）


# [v3.89 #99] OI 历史按时间窗口保留（最近 25 小时），不再按固定条数截断：
#   旧写法（最多 OI_CACHE_KEEP=48 条）假设「每 30 分钟一条」，调用更频繁时旧样本被提前挤掉，
#   get_oi_24h_ago 所需的 24H 前样本长期取不到。25H = 24H 查询点 + 1H 容差，超出窗口的记录在写入时清理。
#   注意：不再有条数上限，单标的条目数 = 25H 内的写入次数（调用频率越高文件越大）。
OI_RETAIN_SEC = 25 * 3600


def _prune_oi_series(series: list, now_ms: int) -> list:
    """保留 timestamp 在最近 OI_RETAIN_SEC 内的记录（保持原有顺序）；缺 timestamp 的坏记录丢弃。"""
    cutoff = now_ms - OI_RETAIN_SEC * 1000
    out = []
    for r in series or []:
        try:
            if int(r["timestamp"]) >= cutoff:
                out.append(r)
        except (KeyError, TypeError, ValueError):
            continue
    return out


def update_oi(symbol: str, oi: float) -> None:
    now = int(time.time() * 1000)
    with _locks["oi"]:
        d = _load_oi_cache_disk()   # 写路径：独占副本，不改已发布的快照
        series = d.get(symbol, [])
        series.append({"timestamp": now, "oi": float(oi)})
        d[symbol] = _prune_oi_series(series, now)
        _save_oi(d)


def update_all_oi(oi_data: dict) -> None:
    """批量更新所有标的 OI"""
    now = int(time.time() * 1000)
    with _locks["oi"]:
        d = _load_oi_cache_disk()   # 写路径：独占副本，不改已发布的快照
        for symbol, oi_val in oi_data.items():
            if oi_val is None or oi_val <= 0:
                continue
            series = d.get(symbol, [])
            series.append({"timestamp": now, "oi": float(oi_val)})
            d[symbol] = series
        # [v3.89 #99] 对所有标的统一按时间窗口清理；整段过期（如已下架）的标的整体移除
        for symbol in list(d.keys()):
            kept = _prune_oi_series(d[symbol], now)
            if kept:
                d[symbol] = kept
            else:
                d.pop(symbol, None)
        _save_oi(d)


def _get_oi_at(symbol: str, target_ts_ms: int, max_err_ms: int) -> float | None:
    d      = _load_oi_cache()
    series = d.get(symbol)
    if not series or len(series) < 2:
        return None
    best = min(series, key=lambda r: abs(r["timestamp"] - target_ts_ms))
    if abs(best["timestamp"] - target_ts_ms) > max_err_ms:
        return None
    return float(best["oi"])


def get_oi_24h_ago(symbol: str) -> float | None:
    now = int(time.time() * 1000)
    with _locks["oi"]:
        d = _load_oi_cache()
        series = d.get(symbol)
        if not series or len(series) < 2:
            return None
        oldest_ts = series[0]["timestamp"]
        if (now - oldest_ts) < _24H_SEC * 0.83 * 1000:
            return None   # 数据不足 20H
        return _get_oi_at(symbol, now - _24H_SEC * 1000, 3600 * 1000)


def get_oi_15min_ago(symbol: str) -> float | None:
    now = int(time.time() * 1000)
    with _locks["oi"]:
        return _get_oi_at(symbol, now - _15M_SEC * 1000, 600_000)


# 向后兼容旧函数名
def update_oi_cache(oi_map: dict) -> None:
    update_all_oi(oi_map)


# ═══════════════════════════════════════════════════════════════════════════════
# 启动自检
# ═══════════════════════════════════════════════════════════════════════════════
def _ensure_files() -> None:
    """确保所有状态文件存在且格式正确"""
    _BASE.mkdir(parents=True, exist_ok=True)
    # CB 文件的默认值需要带当天日期，防止首次加载被误判为跨日重置
    cb_default = {**_CB_DEFAULT, "last_reset_date": _today_utc()}
    for path, default in [
        (_POS_FILE, []),
        (_CB_FILE,  cb_default),
        (_CD_FILE,  {}),
        (_PC_FILE,  {}),
        (_OI_FILE,  {}),
    ]:
        if not path.exists():
            if path == _POS_FILE and _POS_STATE_CORRUPTED:
                # [fix #5c] 损坏标志仍在：不自动新建空仓文件，等待人工确认
                logger.critical(f"[state] {path.name} 缺失且损坏标志存在，不自动重置，等待人工确认")
                continue
            _write_json(path, default)
        else:
            try:
                raw = path.read_text(encoding="utf-8").strip()
                if raw:
                    json.loads(raw)   # 格式校验
                elif path == _POS_FILE:
                    raise ValueError("pos_state 文件为空（0字节或仅空白）")   # [fix #5a]
            except Exception as e:
                if path == _POS_FILE:
                    # [fix #5c] 持仓文件损坏：备份 + 持久化标志 + 告警日志，不自动重置，等人工确认
                    try:
                        _backup_corrupted_pos(path, e)
                    except Exception:
                        pass
                    logger.critical(f"[state] ⚠️ {path.name} 损坏，系统将拒绝写入持仓，请人工用交易所持仓重建后清除标志")
                else:
                    # [fix #15b] 其他状态文件（含 circuit_breaker / cooldown）：先备份，备份失败则不覆盖
                    try:
                        bk = _backup_file(path, "corrupted")
                        logger.error(f"[state] {path.name} 格式损坏，已备份为 {bk.name} 后重置: {e}")
                    except Exception as be:
                        logger.critical(f"[state] {path.name} 格式损坏且备份失败，不覆盖原文件: {e} / {be}")
                        continue
                    _write_json(path, default)


# 模块导入时自动执行
_ensure_files()


def list_orphan_patching() -> list:
    """只读：返回孤儿 patching 记录（定义与 cleanup_orphan_patching 相同），供清理前关闭对应信号卡片。"""
    with _locks["pos"]:
        data = _load_pos()
        return [
            dict(p) for p in data
            if p.get("status") == "patching"
            and not any(q.get("symbol") == p.get("symbol") and q.get("status") == "open" for q in data)
        ]


def cleanup_orphan_patching() -> list:
    """
    清理孤儿 patching 记录。

    孤儿定义：status == 'patching' 且不存在对应的 open 记录。
    这种记录可能由于进程崩溃/网络异常导致补挂时 status 永远停在 patching，
    导致 add_position 拒绝该标的再开仓。

    返回被清除的 symbol 列表。
    """
    if _POS_STATE_CORRUPTED:  # [v3.89 #11b] 清理孤儿记录不拦截，仅提示人工核查
        logger.warning("[state] 持仓文件曾损坏，cleanup_orphan_patching 仍执行，请人工核查结果")
    with _locks["pos"]:
        data = _load_pos()
        orphans = [
            p for p in data
            if p.get("status") == "patching"
            # 确认同标的是否存在 open 记录（正常流程下 patching 过次会升级为 open）
            and not any(
                q.get("symbol") == p.get("symbol") and q.get("status") == "open"
                for q in data
            )
        ]
        if not orphans:
            return []
        orphan_ids = {p.get("pos_id") for p in orphans}
        new = [p for p in data if p.get("pos_id") not in orphan_ids]
        _save_pos(new)
        cleaned = [p.get("symbol", "unknown") for p in orphans]
        logger.warning(
            f"[state] 开机自检 广清孤儿 patching 记录 {len(cleaned)} 条: {cleaned}"
        )
    return cleaned


# ═══════════════════════════════════════════════════════════════════════════════
# funding_arb 状态管理
# ═══════════════════════════════════════════════════════════════════════════════
_FA_FILE = _p("funding_arb_state.json")
_locks["fa"] = __import__("threading").Lock()

def _read_fa() -> dict:
    return _read_json(_FA_FILE, {"positions": [], "consec_sl": 0, "paused_until": 0})

def _write_fa(d: dict) -> None:
    _write_json(_FA_FILE, d)


def get_funding_arb_count() -> int:
    """当前 funding_arb 活跃仓位数（open/pending）"""
    positions = get_all_positions()
    return sum(1 for p in positions
               if p.get("sig_type") == "funding_arb"
               and p.get("status") in ("open", "pending"))


def is_funding_arb_paused() -> bool:
    """检查 funding_arb 是否处于熔断暂停中"""
    with _locks["fa"]:
        d = _read_fa()
        return __import__("time").time() < d.get("paused_until", 0)


def set_funding_arb_paused(hours: float = 24.0) -> None:
    """触发 funding_arb 熔断，暂停指定小时"""
    with _locks["fa"]:
        d = _read_fa()
        d["paused_until"] = __import__("time").time() + hours * 3600
        d["consec_sl"]    = 0   # 重置连续止损计数
        _write_fa(d)
    logger.warning(f"[state] funding_arb 熔断：暂停 {hours}h")


def get_funding_arb_consec_sl() -> int:
    """获取连续止损次数"""
    with _locks["fa"]:
        return _read_fa().get("consec_sl", 0)


def inc_funding_arb_consec_sl() -> int:
    """连续止损次数 +1，返回新值"""
    with _locks["fa"]:
        d = _read_fa()
        d["consec_sl"] = d.get("consec_sl", 0) + 1
        _write_fa(d)
        return d["consec_sl"]


def reset_funding_arb_consec_sl() -> None:
    """非止损退出时重置连续止损计数"""
    with _locks["fa"]:
        d = _read_fa()
        d["consec_sl"] = 0
        _write_fa(d)


def mark_circuit_notified() -> None:
    """熔断推送成功后调用，持久化标记避免重启后重推。"""
    with _locks["cb"]:
        d = _load_cb()
        d["notified"] = True
        _save_cb(d)


def is_circuit_notified() -> bool:
    """本次熔断是否已推送过通知（跨重启持久）。"""
    with _locks["cb"]:
        return bool(_load_cb().get("notified", False))
