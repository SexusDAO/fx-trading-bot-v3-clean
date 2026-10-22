"""
shared/risk_manager.py — 风控守门人（V3.0 完整版）

对外接口：
    is_circuit_open()                     → bool
    record_pnl(pnl, symbol, reason)       → dict
    check_position_limits(direction)      → bool
    check_account_margin(nominal, lev)    → bool
    get_risk_summary()                    → dict
    reset_circuit()                       → None
    get_market_mode()                     → str  (bull/bear/range)
"""

import json
import logging
import os
import threading
from pathlib import Path
from shared.indicators import ema as _calc_ema
import time
from datetime import datetime, timezone

logger = logging.getLogger("fx.risk_manager")

# ── 市场模式缓存 ───────────────────────────────────────────────────────────────
_market_mode_cache = {"mode": None, "cached_at": 0.0}
_MARKET_CACHE_TTL  = 300   # 5 分钟

# v3.33 BTC模式滔山缓冲：bear→range 需连续 2 轮才切换，range/bull→bear 单轮即切
# 防止 bear/range 边界反复横跳，同一信号 5 分钟内评分突变
_mode_pending = {"mode": None, "count": 0}  # 待确认的模式 + 连续计数
_MODE_CONFIRM_ROUNDS = 2  # bear→range/bull 需连续几轮才切换


# ═══════════════════════════════════════════════════════════════════════════════
# 市场模式判断（BTC 4H EMA20 / EMA50 / MA99）
# ═══════════════════════════════════════════════════════════════════════════════
def get_market_mode() -> str:
    """
    返回当前市场模式：bull / bear / range
    结果缓存 5 分钟。
    """
    global _market_mode_cache
    now = time.time()
    if _market_mode_cache["mode"] and (now - _market_mode_cache["cached_at"]) < _MARKET_CACHE_TTL:
        return _market_mode_cache["mode"]

    try:
        from shared import binance_api as api
        from config import MARKET_MODE_EMA_FAST, MARKET_MODE_EMA_MID, MARKET_MODE_EMA_SLOW
        k4h = api.get_klines("BTCUSDT", "4h", limit=120)
        closes = [float(k[4]) for k in k4h]
        if not closes:
            raise ValueError("empty klines")

        ema_fast = _calc_ema(closes, MARKET_MODE_EMA_FAST)
        # 数据不足时直接降级为 range，避免用最新价凑数导致判断失真
        if len(closes) < MARKET_MODE_EMA_SLOW:
            logger.warning(
                f"[risk] K线数量不足（{len(closes)} < {MARKET_MODE_EMA_SLOW}），"
                f"MA{MARKET_MODE_EMA_SLOW} 无法计算，市场模式降级为 range"
            )
            _market_mode_cache = {"mode": "range", "cached_at": now}
            return "range"
        ma_slow  = sum(closes[-MARKET_MODE_EMA_SLOW:]) / MARKET_MODE_EMA_SLOW
        price    = closes[-1]
        ema_50   = _calc_ema(closes, MARKET_MODE_EMA_MID)

        if ema_fast > ema_50 and price > ma_slow:
            raw_mode = "bull"
        elif ema_fast < ema_50 and price < ma_slow:
            raw_mode = "bear"
        else:
            raw_mode = "range"

        # ── v3.33 滔山缓冲：bear→range/bull 需连续 2 轮才切换 ──────────────────
        prev_mode = _market_mode_cache.get("mode") or raw_mode
        if prev_mode == "bear" and raw_mode != "bear":
            # bear 转出：需连续确认
            if _mode_pending["mode"] == raw_mode:
                _mode_pending["count"] += 1
            else:
                _mode_pending["mode"]  = raw_mode
                _mode_pending["count"] = 1
            if _mode_pending["count"] >= _MODE_CONFIRM_ROUNDS:
                mode = raw_mode
                _mode_pending["mode"]  = None
                _mode_pending["count"] = 0
                logger.info(f"[risk] 市场模式 bear→{mode}（已连续{_MODE_CONFIRM_ROUNDS}轮确认）")
            else:
                mode = prev_mode  # 尚未达到确认轮数，保持 bear
                logger.debug(f"[risk] 市场模式待切换 bear→{raw_mode}，确认轮{_mode_pending['count']}/{_MODE_CONFIRM_ROUNDS}")
        else:
            # 切入 bear 单轮即切；非 bear 转换清空待审队列
            if raw_mode == "bear" and prev_mode != "bear":
                logger.info(f"[risk] 市场模式 {prev_mode}→bear（单轮即切）")
            if raw_mode != prev_mode and raw_mode != "bear":
                # 其他非-bear 转换（bull↔range）也单轮即切允许
                pass
            _mode_pending["mode"]  = None
            _mode_pending["count"] = 0
            mode = raw_mode

        # ── v3.38 熊市强度感知 ────────────────────────────────────────────────
        if mode == "bear":
            try:
                # 用已有的 closes[-1] 和 96根前（≈24H）算涨跌幅
                _price_24h_ago = closes[-97] if len(closes) >= 97 else closes[0]
                _btc_chg24 = (price - _price_24h_ago) / (_price_24h_ago + 1e-9) * 100
                _ema_gap_pct = (ema_fast - ema_50) / (ema_50 + 1e-9) * 100  # 负值
                if _btc_chg24 < -5 or _ema_gap_pct < -5:
                    _bear_strength = "strong"
                elif _btc_chg24 < -2 or _ema_gap_pct < -2:
                    _bear_strength = "moderate"
                else:
                    _bear_strength = "weak"
            except Exception:
                _bear_strength = "weak"
            _market_mode_cache["bear_strength"] = _bear_strength
            logger.debug(f"[risk] bear强度={_bear_strength} "
                         f"chg24={_btc_chg24:.1f}% ema_gap={_ema_gap_pct:.1f}%")
        else:
            _market_mode_cache["bear_strength"] = None

        _market_mode_cache = {"mode": mode, "cached_at": now,
                              "bear_strength": _market_mode_cache.get("bear_strength")}
        logger.debug(f"[risk] 市场模式={mode} raw={raw_mode} ema_fast={ema_fast:.0f} price={price:.0f}")
        return mode
    except Exception as e:
        logger.warning(f"[risk] 市场模式判断失败，降级为 range: {e}")
        _market_mode_cache = {"mode": "range", "cached_at": now}
        return "range"



def get_bear_strength() -> str | None:
    """
    v3.38: 返回当前熊市强度：'strong' / 'moderate' / 'weak' / None（非熊市）
    依赖 get_market_mode() 已运行过，使用缓存值。
    """
    return _market_mode_cache.get("bear_strength", None)


def _get_position_limits(mode: str) -> dict:
    from config import (
        MAX_POSITIONS_TOTAL_BULL, MAX_POSITIONS_LONG_BULL, MAX_POSITIONS_SHORT_BULL,
        MAX_POSITIONS_TOTAL_BEAR, MAX_POSITIONS_LONG_BEAR, MAX_POSITIONS_SHORT_BEAR,
        MAX_POSITIONS_TOTAL_RANGE, MAX_POSITIONS_LONG_RANGE, MAX_POSITIONS_SHORT_RANGE,
    )
    if mode == "bull":
        return {"total": MAX_POSITIONS_TOTAL_BULL,
                "long":  MAX_POSITIONS_LONG_BULL,
                "short": MAX_POSITIONS_SHORT_BULL}
    if mode == "bear":
        return {"total": MAX_POSITIONS_TOTAL_BEAR,
                "long":  MAX_POSITIONS_LONG_BEAR,
                "short": MAX_POSITIONS_SHORT_BEAR}
    return {"total": MAX_POSITIONS_TOTAL_RANGE,
            "long":  MAX_POSITIONS_LONG_RANGE,
            "short": MAX_POSITIONS_SHORT_RANGE}


# ═══════════════════════════════════════════════════════════════════════════════
# is_circuit_open
# ═══════════════════════════════════════════════════════════════════════════════
def is_circuit_open() -> bool:
    """熔断器是否触发。包含跨日自动重置逻辑。文件异常时保守返回 True。"""
    try:
        from shared.state import is_circuit_open as _state_check
        return _state_check()
    except Exception as e:
        logger.error(f"[risk] is_circuit_open 读取失败，保守返回 True: {e}")
        return True


# ═══════════════════════════════════════════════════════════════════════════════
# record_pnl — 平仓后调用
# ═══════════════════════════════════════════════════════════════════════════════

def record_pnl(pnl: float, symbol: str = "", reason: str = "", fee: float = 0.0, funding: float = 0.0) -> dict:
    """
    记录净盈亏（正负都累计）；fee/funding 单独记录并计入总亏损。
    返回 {"circuit_triggered": bool|None, "daily_loss": float|None, "persisted": bool[, "error": str]}
    [v3.89 #63] persisted=False 表示本次盈亏没有记入 state（circuit_triggered 此时为 None=未知）。
    """
    from shared.state import record_pnl as _state_record, get_daily_loss
    from shared.state import mark_circuit_notified

    triggered = False
    persisted = True
    persist_error = None
    try:
        triggered = _state_record(pnl, fee=fee, funding=funding)
    except Exception as e:
        # [v3.89 #63] 持久化失败不再伪装成「已记账、未触发熔断」：circuit_triggered 置 None（未知，且为假值，
        #   兼容 if result["circuit_triggered"] 的既有用法），并带 persisted=False 与 error 供上游识别、对账。
        persisted = False
        persist_error = str(e)
        triggered = None
        logger.error(f"[risk] record_pnl state 写入失败，本次盈亏 {pnl} ({symbol}) 未记入: {e}")

    try:
        daily_loss = get_daily_loss()
    except Exception as e:
        daily_loss = None
        logger.error(f"[risk] 读取 daily_loss 失败: {e}")

    if triggered:
        try:
            from config import CIRCUIT_BREAKER_DAILY_LOSS
            limit = CIRCUIT_BREAKER_DAILY_LOSS
        except Exception:
            limit = 9999.0
        msg = (
            f"🔴 熔断触发！\n"
            f"触发原因：{reason or '日亏损超限'}\n"
            f"当日累计亏损：{daily_loss:.1f}U（上限 {limit}U）\n"
            f"触发时间：{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
            f"下次 UTC 00:00 自动重置"
        )
        # 只写日志，不发送 Jarvis 推送（避免 gateway 缓存重发导致重复信息）
        # 如需恢复推送，表齢此处注释并调用 notify(msg)
        logger.error(f"[risk] {msg}")
        mark_circuit_notified()  # 标记已处理，防止其他路径重复触发

    result = {"circuit_triggered": triggered, "daily_loss": daily_loss, "persisted": persisted}
    if not persisted:
        result["error"] = persist_error
    return result


# 向后兼容旧接口
def record_loss(usd: float) -> None:
    record_pnl(-abs(usd))


def record_win() -> None:
    pass   # 盈利不影响熔断计算，保留接口兼容性


# ═══════════════════════════════════════════════════════════════════════════════
# check_position_limits
# ═══════════════════════════════════════════════════════════════════════════════
def check_position_limits(direction: str) -> bool | str:
    """
    检查是否允许新开仓（方向 long/short）。
    v3.72: 双层检查
      第一层：open 持仓槽位（动态上限，由市场模式决定）
        → 满了返回 False（直接拒绝）
      第二层：pending 挂单槽位（固定上限 MAX_PENDING_LONG/SHORT）
        → 满了返回 "compete"（交由 signal_watcher 竞争机制决定）
        → 未满返回 True（直接注册）
    """
    try:
        from shared.state import get_position_count, get_pending_count
        from config import MAX_PENDING_LONG, MAX_PENDING_SHORT, MAX_PENDING_TOTAL
        counts  = get_position_count()   # 只含 open+patching
        pending = get_pending_count()    # 只含 pending
        mode    = get_market_mode()
        limits  = _get_position_limits(mode)
        dir_upper = direction.upper()

        # ── 第一层：open 持仓槽位 ─────────────────────────────────────────────
        if counts["total"] >= limits["total"]:
            logger.info(f"[risk] 总持仓 {counts['total']}/{limits['total']} 已满")
            return False
        if dir_upper == "LONG" and counts["long"] >= limits["long"]:
            logger.info(f"[risk] 多单持仓 {counts['long']}/{limits['long']} 已满")
            return False
        if dir_upper == "SHORT" and counts["short"] >= limits["short"]:
            logger.info(f"[risk] 空单持仓 {counts['short']}/{limits['short']} 已满")
            return False

        # ── 第二层：pending 挂单槽位 ─────────────────────────────────────────
        if pending["total"] >= MAX_PENDING_TOTAL:
            logger.info(f"[risk] 挂单总数 {pending['total']}/{MAX_PENDING_TOTAL} 已满，转竞争")
            return "compete"
        if dir_upper == "LONG" and pending["long"] >= MAX_PENDING_LONG:
            logger.info(f"[risk] 多单挂单 {pending['long']}/{MAX_PENDING_LONG} 已满，转竞争")
            return "compete"
        if dir_upper == "SHORT" and pending["short"] >= MAX_PENDING_SHORT:
            logger.info(f"[risk] 空单挂单 {pending['short']}/{MAX_PENDING_SHORT} 已满，转竞争")
            return "compete"

        return True
    except Exception as e:
        logger.warning(f"[risk] check_position_limits 异常，保守返回 False: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# check_account_margin
# ═══════════════════════════════════════════════════════════════════════════════
def check_account_margin(nominal_value: float, leverage: int) -> bool:
    """
    检查账户余额是否足够开仓：
    1. 可用余额 >= 名义仓位 / 杠杆 × MARGIN_BUFFER
    2. 总名义仓位 + 本次 <= 账户余额 × MAX_TOTAL_NOMINAL
    3. 本次名义仓位 <= MAX_SINGLE_NOMINAL
    """
    try:
        from shared import binance_api as api
        from shared.state import get_all_positions
        from config import MARGIN_BUFFER, MAX_TOTAL_NOMINAL, MAX_SINGLE_NOMINAL

        acc = api.get_account()
        available = float(acc.get("availableBalance", 0))
        total_bal = float(acc.get("totalWalletBalance", 0))

        margin_needed = nominal_value / max(leverage, 1) * MARGIN_BUFFER
        if available < margin_needed:
            logger.info(f"[risk] 可用余额不足: {available:.2f} < {margin_needed:.2f}")
            return False

        positions = get_all_positions()
        current_nominal = sum(
            float(p.get("quantity", 0)) * float(p.get("entry_price", 0))
            for p in positions if p.get("status") in ("open", "pending")
        )
        if current_nominal + nominal_value > total_bal * MAX_TOTAL_NOMINAL:
            logger.info(f"[risk] 总名义仓位超限: {current_nominal+nominal_value:.0f} > {total_bal*MAX_TOTAL_NOMINAL:.0f}")
            return False

        # v3.58: 单仓上限取 min(固定上限, 账户权益×40%)，防止账户缩水后单仓名义仍过大
        _dynamic_single_max = min(MAX_SINGLE_NOMINAL, total_bal * 0.4)
        if nominal_value > _dynamic_single_max:
            logger.info(
                f"[risk] 单仓名义仓位超限: {nominal_value:.0f} > "
                f"{_dynamic_single_max:.0f}U（min(固定{MAX_SINGLE_NOMINAL}, 账户{total_bal:.0f}U×40%)）"
            )
            return False

        return True
    except Exception as e:
        logger.warning(f"[risk] check_account_margin 异常，保守返回 False: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# reset_circuit
# ═══════════════════════════════════════════════════════════════════════════════
def reset_circuit() -> None:
    """UTC 00:00 定时任务调用。重置熔断 + 备份日志。"""
    from shared.state import reset_circuit as _state_reset, get_daily_loss
    from shared.notifier import notify
    import shutil
    from pathlib import Path
    from config import TRADE_LOG_FILE, POS_STATE_FILE

    yesterday_loss = get_daily_loss()
    _state_reset()
    logger.info(f"[risk] 熔断器重置，昨日亏损={yesterday_loss:.1f}U")

    # 备份
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    archive  = Path(__file__).parent.parent / "logs" / "archive"  # fx/logs/archive
    archive.mkdir(parents=True, exist_ok=True)

    for src_path in [TRADE_LOG_FILE, POS_STATE_FILE]:
        src = Path(src_path)
        if src.exists():
            try:
                dst = archive / f"{src.stem}_{date_str}{src.suffix}"
                shutil.copy2(src, dst)
                # 清理 30 天前的备份
                _clean_old_backups(archive, src.stem, 30)
            except Exception as e:
                logger.warning(f"[risk] 备份 {src_path} 失败: {e}")

    try:
        notify(
            f"📅 日重置完成 {date_str}\n"
            f"昨日累计亏损：{yesterday_loss:.1f}U\n"
            f"今日熔断器：正常"
        )
    except Exception:
        pass


def _clean_old_backups(archive: "Path", stem: str, keep_days: int) -> None:
    from pathlib import Path
    files = sorted(archive.glob(f"{stem}_*"), key=lambda p: p.stat().st_mtime)
    while len(files) > keep_days:
        files[0].unlink(missing_ok=True)
        files = files[1:]


# ═══════════════════════════════════════════════════════════════════════════════
# get_risk_summary
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# 全局最大回撤保护（v3.57）— 周级别高水位跟踪
# ═══════════════════════════════════════════════════════════════════════════════
_DRAWDOWN_STATE_FILE = str(Path(__file__).parent.parent / "logs" / "drawdown_state.json")

_DRAWDOWN_LOCK = threading.RLock()        # [v3.89 #67] 读-改-写串行化
_DRAWDOWN_STATE_CORRUPTED = False         # [v3.89 #67] 状态文件曾损坏（进程内标志，重启清除；备份文件保留）


def _default_drawdown_state() -> dict:
    return {"high_water": 0.0, "hw_updated_at": 0.0, "halt_until": 0.0}


def _load_drawdown_state() -> dict:
    """
    [v3.89 #67] 文件不存在 → 默认值（首次初始化，正常）；
    文件存在但读取/解析失败 → 保留损坏文件备份（.corrupted_<ms>），logger.error，置 _DRAWDOWN_STATE_CORRUPTED，
    再返回默认值——不再静默返回零高水位。注意：默认值下高水位会从当前权益重新累计，
    历史高水位只在备份文件里，需人工核对。
    """
    global _DRAWDOWN_STATE_CORRUPTED
    p = Path(_DRAWDOWN_STATE_FILE)
    if not p.exists():
        return _default_drawdown_state()
    try:
        data = json.loads(p.read_text())
        if not isinstance(data, dict):
            raise ValueError(f"状态不是对象: {type(data).__name__}")
        return data
    except Exception as e:
        _DRAWDOWN_STATE_CORRUPTED = True
        backup = p.with_name(f"{p.name}.corrupted_{int(time.time() * 1000)}")
        try:
            p.replace(backup)
            logger.error(f"[risk] 回撤状态文件损坏，已备份为 {backup.name}，高水位/暂停状态已丢失，请人工核对: {e}")
        except Exception as be:
            logger.error(f"[risk] 回撤状态文件损坏且备份失败（原文件保留）: {e} / 备份错误: {be}")
        return _default_drawdown_state()


def _save_drawdown_state(state: dict) -> None:
    """[v3.89 #67] 原子写入：唯一临时文件 + replace，避免中断留下半截文件。"""
    import uuid
    p = Path(_DRAWDOWN_STATE_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(f"{p.name}.tmp_{os.getpid()}_{uuid.uuid4().hex[:8]}")
    try:
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2))
        tmp.replace(p)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        raise


def update_drawdown_high_water(current_equity: float) -> None:
    """
    每次账户状态更新后调用，维护高水位。
    current_equity: 账户总权益（totalMarginBalance）
    """
    with _DRAWDOWN_LOCK:
        state = _load_drawdown_state()
        if current_equity > state.get("high_water", 0):
            state["high_water"]    = current_equity
            state["hw_updated_at"] = time.time()
            _save_drawdown_state(state)
            logger.debug(f"[risk] 高水位更新: {current_equity:.2f}U")

def check_max_drawdown(current_equity: float, notifier_fn=None) -> tuple[bool, str]:
    """
    检查当前权益是否触发最大回撤保护。
    返回 (ok, reason)：ok=False 时禁止开仓。

    两档保护：
      ① 从高水位回撤 >= DRAWDOWN_HALT_PCT（默认15%）→ 禁止开仓 N 天
      ② 从高水位回撤 >= DRAWDOWN_WARN_PCT（默认8%）→ 降槽位50%（通知但不禁止）
    """
    try:
        from config import (
            DRAWDOWN_HALT_PCT   as _HALT,    # 默认 0.15  回撤>=15% 暂停开仓
            DRAWDOWN_HALT_DAYS  as _DAYS,    # 默认 3     暂停天数
            DRAWDOWN_WARN_PCT   as _WARN,    # 默认 0.08  回撤>=8% 警告
        )
    except Exception:
        _HALT = 0.15
        _DAYS = 3
        _WARN = 0.08

    state = _load_drawdown_state()
    hw    = state.get("high_water", 0.0)

    # 首次运行或无高水位记录时，初始化高水位
    if hw <= 0:
        update_drawdown_high_water(current_equity)
        return True, ""

    # 检查是否仍在暂停期
    halt_until = state.get("halt_until", 0.0)
    if halt_until > time.time():
        remaining_h = (halt_until - time.time()) / 3600
        return False, f"最大回撤保护暂停中（还剩 {remaining_h:.1f}h，高水位={hw:.0f}U 当前={current_equity:.0f}U）"

    if hw <= 0:
        return True, ""

    drawdown = (hw - current_equity) / hw
    if drawdown < 0:
        drawdown = 0.0

    # 更新高水位（只升不降）
    if current_equity > hw:
        update_drawdown_high_water(current_equity)

    # ① 超过暂停阈值
    if drawdown >= _HALT:
        halt_ts = time.time() + _DAYS * 86400
        state["halt_until"] = halt_ts
        _save_drawdown_state(state)
        reason = (
            f"最大回撤保护触发: 从高水位{hw:.0f}U回撤{drawdown*100:.1f}% >= {_HALT*100:.0f}%，"
            f"暂停开仓{_DAYS}天"
        )
        logger.warning(f"[risk] {reason}")
        if notifier_fn:
            try:
                import asyncio as _aio
                _aio.ensure_future(notifier_fn(f"⛔ {reason}"))
            except Exception:
                pass
        return False, reason

    # ② 超过警告阈值
    if drawdown >= _WARN:
        reason = f"回撤警告: 从高水位{hw:.0f}U回撤{drawdown*100:.1f}% >= {_WARN*100:.0f}%"
        logger.info(f"[risk] {reason}（不阻断开仓，已写入日志）")

    return True, ""


# ═══════════════════════════════════════════════════════════════════════════════
# BTC 市场周期阶段识别（v3.58）
# ═══════════════════════════════════════════════════════════════════════════════
_cycle_cache: dict = {"phase": None, "cached_at": 0.0}
_CYCLE_CACHE_TTL = 300   # 5分钟

def get_market_cycle_phase() -> str:
    """
    识别当前 BTC 牛市所处阶段，返回：
      "early_bull"  — 刚从 bear 转 bull，动能充足，breakout 信号最佳
      "mid_bull"    — 趋势加速中段，pullback 信号最佳
      "late_bull"   — EMA 多头但 RSI 超买/资金费率极端，做多危险
      "bear"        — 空头市场
      "range"       — 震荡市
    """
    global _cycle_cache
    now = time.time()
    if _cycle_cache["phase"] and (now - _cycle_cache["cached_at"]) < _CYCLE_CACHE_TTL:
        return _cycle_cache["phase"]

    try:
        from shared import binance_api as api

        k4h    = api.get_klines("BTCUSDT", "4h", 120)
        closes = [float(k[4]) for k in k4h]
        if len(closes) < 50:
            return "range"

        price    = closes[-1]
        ema20    = _calc_ema(closes, 20)
        ema50    = _calc_ema(closes, 50)
        ma99     = sum(closes[-99:]) / 99 if len(closes) >= 99 else sum(closes) / len(closes)

        # RSI（14）
        diffs  = [closes[i] - closes[i-1] for i in range(1, len(closes))]
        gains  = [d if d > 0 else 0 for d in diffs[-14:]]
        losses = [-d if d < 0 else 0 for d in diffs[-14:]]
        ag     = sum(gains) / 14; al = sum(losses) / 14
        rsi    = 100 - 100 / (1 + ag / al) if al > 0 else 100

        # 资金费率
        md      = api.get_mark_price("BTCUSDT")
        funding = float(md.get("lastFundingRate", 0))

        # 判断阶段
        bull_structure = ema20 > ema50 and price > ma99
        if not bull_structure:
            phase = "range" if ema20 > ema50 * 0.98 else "bear"
        else:
            # 牛市内部阶段
            ema_gap_pct = (ema20 - ema50) / ema50 * 100  # EMA 间距
            if rsi > 75 or funding > 0.005:
                phase = "late_bull"    # RSI 超买或费率极高 → 末期
            elif ema_gap_pct < 2.0:
                phase = "early_bull"   # EMA 刚刚分叉 → 初期
            else:
                phase = "mid_bull"     # 主升段

        _cycle_cache = {"phase": phase, "cached_at": now}
        logger.debug(f"[risk] 市场周期阶段={phase} rsi={rsi:.1f} ema_gap={ema_gap_pct if bull_structure else 0:.1f}% fr={funding*100:.4f}%")
        return phase

    except Exception as e:
        logger.debug(f"[risk] get_market_cycle_phase 失败，降级 range: {e}")
        return "range"

# ═══════════════════════════════════════════════════════════════════════════════
# check_margin_safety — 保证金安全垫检查（供 executor._account_final_check 调用）
# ═══════════════════════════════════════════════════════════════════════════════
def check_margin_safety(notifier_fn=None) -> tuple[bool, str, dict]:
    """
    检查当前账户保证金状态是否健康，返回 (ok, reason, stats)。

    三档判断：
      ① margin_ratio = totalMaintMargin / totalMarginBalance
        >= MARGIN_SAFETY_RATIO_HALT  → 不允许开仓（保证金率过高，接近爆仓）
        >= MARGIN_SAFETY_RATIO_TIGHT → 允许开仓但缩减仓位（position_scale 降级）
        <  MARGIN_SAFETY_RATIO_TIGHT → 正常

      ② available_ratio = availableBalance / totalMarginBalance
        <  MARGIN_SAFETY_AVAIL_MIN   → 可用余额不足，不允许开仓

    返回:
      ok             : bool — False 时不允许开仓
      reason         : str  — 拒绝原因（ok=True 时为空）
      stats          : dict — {margin_ratio, available_ratio, position_scale,
                                margin_tight_hint, total_maint, total_margin,
                                available}
    """
    # 配置常量（config 里不存在时用合理默认值）
    try:
        from config import (
            MARGIN_SAFETY_RATIO_HALT  as _HALT,   # 默认 0.5  — 维持/余额 >= 50% 停止开仓
            MARGIN_SAFETY_RATIO_TIGHT as _TIGHT,  # 默认 0.3  — >= 30% 缩减仓位
            MARGIN_SAFETY_AVAIL_MIN   as _AVAIL,  # 默认 0.1  — 可用 < 10% 停止开仓
            MARGIN_SAFETY_SCALE_TIGHT as _SCALE,  # 默认 0.5  — 偏紧时仓位缩至 50%
        )
    except Exception:
        _HALT  = 0.5
        _TIGHT = 0.3
        _AVAIL = 0.1
        _SCALE = 0.5

    stats = {
        "margin_ratio":     0.0,
        "available_ratio":  1.0,
        "position_scale":   1.0,
        "margin_tight_hint": "",
        "total_maint":      0.0,
        "total_margin":     0.0,
        "available":        0.0,
    }

    try:
        from shared import binance_api as _api
        acc = _api.get_account()

        total_maint  = float(acc.get("totalMaintMargin",  0))
        total_margin = float(acc.get("totalMarginBalance", 0))
        available    = float(acc.get("availableBalance",  0))

        stats["total_maint"]  = total_maint
        stats["total_margin"] = total_margin
        stats["available"]    = available

        # 没有持仓时 total_margin ≈ walletBalance，margin_ratio = 0 ✅
        margin_ratio    = total_maint / total_margin if total_margin > 1e-6 else 0.0
        available_ratio = available   / total_margin if total_margin > 1e-6 else 1.0
        stats["margin_ratio"]    = round(margin_ratio,    4)
        stats["available_ratio"] = round(available_ratio, 4)

        # ① 维持保证金率过高 → 禁止开仓
        if margin_ratio >= _HALT:
            reason = (
                f"保证金率 {margin_ratio*100:.1f}% >= {_HALT*100:.0f}%，"
                f"维持保证金 {total_maint:.1f}U / 余额 {total_margin:.1f}U，禁止开仓"
            )
            logger.warning(f"[risk] check_margin_safety: {reason}")
            return False, reason, stats

        # ② 可用余额比例过低 → 禁止开仓
        if available_ratio < _AVAIL:
            reason = (
                f"可用余额比例 {available_ratio*100:.1f}% < {_AVAIL*100:.0f}%，"
                f"可用 {available:.1f}U / 余额 {total_margin:.1f}U，禁止开仓"
            )
            logger.warning(f"[risk] check_margin_safety: {reason}")
            return False, reason, stats

        # ③ 维持保证金率偏紧 → 缩减仓位
        if margin_ratio >= _TIGHT:
            stats["position_scale"]   = _SCALE
            stats["margin_tight_hint"] = (
                f"保证金偏紧({margin_ratio*100:.1f}%)，仓位缩减至 {_SCALE*100:.0f}%"
            )
            logger.info(f"[risk] check_margin_safety: {stats['margin_tight_hint']}")

        return True, "", stats

    except Exception as e:
        # [fix #29] 统一口径：检查异常 → 拒绝开仓
        logger.error(f"[risk] check_margin_safety 异常，拒绝开仓: {e}")
        return False, f"保证金安全垫检查异常: {e}", stats


def get_risk_summary() -> dict:
    from shared.state import get_position_count, get_all_cooldowns, get_daily_loss
    from config import CIRCUIT_BREAKER_DAILY_LOSS
    try:
        daily_loss = get_daily_loss()
        limit      = float(CIRCUIT_BREAKER_DAILY_LOSS)
        counts     = get_position_count()
        mode       = get_market_mode()
        limits     = _get_position_limits(mode)
        cooldowns  = [c["symbol"] for c in get_all_cooldowns()]
        return {
            "circuit_open":     is_circuit_open(),
            "daily_loss":       daily_loss,
            "daily_loss_limit": limit,
            "daily_loss_pct":   round(daily_loss / limit * 100, 1) if limit else 0,
            "position_count":   counts,
            "market_mode":      mode,
            "position_limits":  limits,
            "cooldown_symbols": cooldowns,
        }
    except Exception as e:
        logger.error(f"[risk] get_risk_summary 失败: {e}")
        return {}


# 向后兼容
def get_state() -> dict:
    return get_risk_summary()


# ═══════════════════════════════════════════════════════════════════════════════
# 账户状态感知仓位调整（v3.71）
# ═══════════════════════════════════════════════════════════════════════════════

def _count_consecutive_loss_days() -> int:
    """
    从 archive 目录读取最近 7 天的归档文件，计算连续亏损天数。
    失败时返回 0（不阻断开仓）。
    """
    try:
        from pathlib import Path
        from config import ARCHIVE_DIR
        import json as _j
        from datetime import datetime, timezone, timedelta
        archive_dir = Path(ARCHIVE_DIR)
        today = datetime.now(timezone.utc).date()
        consec = 0
        for i in range(1, 8):  # 向前最多查 7 天
            day_str = str(today - timedelta(days=i))
            fp = archive_dir / f"{day_str}.json"
            if not fp.exists():
                break
            try:
                obj = _j.loads(fp.read_text())
                pnl = obj.get("summary", {}).get("total_pnl_usdt", 0.0) or 0.0
                if pnl >= 0:
                    break
                consec += 1
            except Exception:
                break
        return consec
    except Exception:
        return 0


def get_account_state_scale() -> float:
    """
    根据账户当前状态返回仓位缩放系数（0.0~1.0）。
    1.0 = 正常仓位；< 1.0 = 降仓。
    三个触发条件（取最小值）：
      1. 当日亏损 > ACCOUNT_STATE_LOSS_SCALE_THRESH × 权益
      2. 近4H内有止损平仓
      3. 连续 ACCOUNT_STATE_CONSEC_LOSS_DAYS 天亏损
    所有异常情况返回 1.0（不因查询失败阻断开仓）。
    """
    try:
        from config import (
            ACCOUNT_STATE_LOSS_SCALE_THRESH,
            ACCOUNT_STATE_LOSS_SCALE_FACTOR,
            ACCOUNT_STATE_CONSEC_LOSS_DAYS,
            ACCOUNT_STATE_CONSEC_LOSS_FACTOR,
        )
        from shared.state import get_daily_loss, get_all_positions
        import time as _t

        scale = 1.0

        # 1. 当日亏损
        try:
            from shared import binance_api as _api
            acc = _api.get_account()
            equity = float(acc.get("totalMarginBalance", 0) or 0)
        except Exception:
            equity = 0.0
        daily_loss = get_daily_loss()  # 正值（亏损累加）
        if equity > 0 and daily_loss / equity > ACCOUNT_STATE_LOSS_SCALE_THRESH:
            scale = min(scale, ACCOUNT_STATE_LOSS_SCALE_FACTOR)
            logger.info(
                f"[risk] 账户状态：当日亏损 {daily_loss:.1f}U / {equity:.0f}U "
                f"= {daily_loss/equity*100:.1f}% > {ACCOUNT_STATE_LOSS_SCALE_THRESH*100:.0f}%，"
                f"仓位系数 → {ACCOUNT_STATE_LOSS_SCALE_FACTOR}"
            )

        # 2. 近4H内有止损
        all_pos = get_all_positions()
        recent_sl = [
            p for p in all_pos
            if p.get("close_reason") in ("sl", "sl_triggered")
            and _t.time() - p.get("close_ts", 0) < 4 * 3600
            and p.get("status") == "closed"
        ]
        if recent_sl:
            scale = min(scale, 0.7)
            logger.info(f"[risk] 账户状态：近4H内有 {len(recent_sl)} 笔止损，仓位系数 → 0.7")

        # 3. 连续亏损
        consec_days = _count_consecutive_loss_days()
        if consec_days >= ACCOUNT_STATE_CONSEC_LOSS_DAYS:
            scale = min(scale, ACCOUNT_STATE_CONSEC_LOSS_FACTOR)
            logger.info(
                f"[risk] 账户状态：连续亏损 {consec_days} 天 ≥ {ACCOUNT_STATE_CONSEC_LOSS_DAYS} 天，"
                f"仓位系数 → {ACCOUNT_STATE_CONSEC_LOSS_FACTOR}"
            )

        return scale
    except Exception as e:
        logger.warning(f"[risk] get_account_state_scale 失败，默认 1.0: {e}")
        return 1.0


# ═══════════════════════════════════════════════════════════════════════════════
# 系统性风险检测（v3.71）— 稳定币脱锚 + API异常 + 极端费率
# ═══════════════════════════════════════════════════════════════════════════════

# API 失败率追踪（简单滑动窗口）
_api_fail_log: list = []   # [(ts, failed: bool), ...]
_API_FAIL_WINDOW = 200     # 最近 200 次请求

def record_api_call(success: bool) -> None:
    """在 binance_api._retry 里调用，记录请求成败（try/except 不阻断）。"""
    try:
        import time as _t
        _api_fail_log.append((_t.time(), not success))
        if len(_api_fail_log) > _API_FAIL_WINDOW:
            del _api_fail_log[0]
    except Exception:
        pass

# [v3.89 #65] 失败率统计契约：同时受「次数窗口」（最近 50 次）与「时间窗口」（最近 600 秒）约束，
#   两者都满足的样本才计入。此前只取最近 50 次，低流量或恢复阶段的陈旧故障会被当成当前接口状态。
_API_FAIL_TAIL = 50
_API_FAIL_WINDOW_SEC = 600.0


def _api_fail_stats(now: float = None) -> dict:
    """
    返回 {"rate": 失败率, "samples": 窗口内样本数, "newest_age_s": 最新样本年龄(秒)|None, "status": "ok"|"no_sample"}。
    窗口内没有样本 → rate=0.0、status="no_sample"（沿用原行为：不触发风控，但标明是「没有样本」而非「确认零失败」）。
    """
    import time as _t
    now = _t.time() if now is None else now
    tail = list(_api_fail_log[-_API_FAIL_TAIL:])
    fresh = [(t, f) for t, f in tail if now - t <= _API_FAIL_WINDOW_SEC]
    newest = max((t for t, _ in tail), default=None)
    newest_age = (now - newest) if newest is not None else None
    if not fresh:
        return {"rate": 0.0, "samples": 0, "newest_age_s": newest_age, "status": "no_sample"}
    return {"rate": sum(1 for _, f in fresh if f) / len(fresh), "samples": len(fresh),
            "newest_age_s": newest_age, "status": "ok"}


def _api_fail_rate_recent() -> float:
    """返回时间窗口内最近请求的失败率（0.0~1.0）；窗口内无样本返回 0.0（见 _api_fail_stats 的 status）。"""
    return _api_fail_stats()["rate"]


LAST_USDT_PEG_STATUS: dict = {"status": "unknown", "rate": None, "ts": 0.0, "error": None}
_PEG_WARN_INTERVAL = 600.0   # 数据不可用告警最小间隔（秒）
_peg_last_warn = 0.0


def get_usdt_peg_observation() -> dict:
    """
    [v3.89 #66] 取 USDCUSDT 标记价格，返回 {"status": "ok"|"unavailable", "rate": float|None, "error": str|None}。
    markPrice 缺失、为零、非数字或请求异常 → unavailable（不用 1.0 代替）。同时写入 LAST_USDT_PEG_STATUS。
    """
    import math
    obs = {"status": "unavailable", "rate": None, "error": None, "ts": time.time()}
    try:
        from shared import binance_api as _api
        data = _api.get_mark_price("USDCUSDT")
        raw = data.get("markPrice") if isinstance(data, dict) else None
        rate = float(raw) if raw not in (None, "") else None
        if rate is None or not math.isfinite(rate) or rate <= 0:
            obs["error"] = f"markPrice 无效: {raw!r}"
        else:
            obs.update(status="ok", rate=rate)
    except Exception as e:
        obs["error"] = f"查询异常: {e}"
    LAST_USDT_PEG_STATUS.update(obs)
    return obs


def _warn_peg_unavailable(obs: dict) -> None:
    global _peg_last_warn
    now = time.time()
    if now - _peg_last_warn >= _PEG_WARN_INTERVAL:
        _peg_last_warn = now
        logger.warning(f"[risk] USDT 脱锚检查数据不可用（{obs.get('error')}），本次跳过该项检查")



def check_systemic_risk() -> tuple[bool, str]:
    """
    检查系统性风险事件。返回 (ok, reason)。
    ok=False 时扫描器应跳过本轮新开仓。
    所有异常均 try/except 兜底，失败时保守返回 (True, "")（允许开仓）。
    """
    try:
        from config import (
            SYSTEMIC_USDT_DEPEG_THRESH,
            SYSTEMIC_API_FAIL_RATE_THRESH,
            SYSTEMIC_FUNDING_EXTREME_THRESH,
        )
    except Exception:
        SYSTEMIC_USDT_DEPEG_THRESH       = 0.005
        SYSTEMIC_API_FAIL_RATE_THRESH    = 0.50
        SYSTEMIC_FUNDING_EXTREME_THRESH  = 0.010

    # ① USDT 脱锚（用 USDCUSDT 标记价格判断）
    # [v3.89 #66] 价格缺失/为零/查询异常 → 记录「数据不可用」状态，不再用 1.0 伪造平价观测。
    #   降级策略保持原行为（该项不单独阻断开仓，其他检查照常），但状态可见：LAST_USDT_PEG_STATUS + warning。
    _peg = get_usdt_peg_observation()
    if _peg["status"] == "unavailable":
        _warn_peg_unavailable(_peg)
    elif abs(_peg["rate"] - 1.0) > SYSTEMIC_USDT_DEPEG_THRESH:
        return False, f"USDT 脱锚警告: USDC/USDT={_peg['rate']:.4f}"

    # ② API 响应异常
    _fs = _api_fail_stats()
    fail_rate = _fs["rate"]
    if fail_rate > SYSTEMIC_API_FAIL_RATE_THRESH:
        return False, f"API 异常：最近请求失败率 {fail_rate*100:.0f}%（{_fs['samples']} 个样本，最新 {_fs['newest_age_s'] or 0:.0f}s 前）"

    # ③ 极端资金费率（持仓中的标的，排除 funding_arb 专属仓位）
    # 注意：高费率是 funding_arb 的机会，不应阻断 funding_arb 扫描
    # 这里只检测「非套利仓位」，防止真正的交易所异常事件
    try:
        from shared.state import get_all_positions as _gp
        from shared import binance_api as _api2
        for pos in _gp():
            if pos.get("status") != "open":
                continue
            # funding_arb 仓位跳过（高费率正是其开仓依据）
            if pos.get("sig_type") == "funding_arb" or pos.get("mode") == "funding_arb":
                continue
            try:
                mp = _api2.get_mark_price(pos["symbol"])
                fr = abs(float(mp.get("lastFundingRate", 0) or 0))
                if fr > SYSTEMIC_FUNDING_EXTREME_THRESH:
                    return False, (
                        f"{pos['symbol']} 资金费率极端 {fr*100:.2f}%（非套利仓位），"
                        f"疑似交易所异常，暂停新开仓"
                    )
            except Exception as _fe:
                # [fix #29] 单仓位资金费率查询失败 → 拒绝开仓
                logger.error(f"[risk] 系统性风险检查：{pos.get('symbol')} 资金费率查询失败，拒绝开仓: {_fe}")
                return False, f"系统性风险检查异常（{pos.get('symbol')} 资金费率查询失败）"
    except Exception as _se:
        logger.error(f"[risk] 系统性风险检查异常，拒绝开仓: {_se}")
        return False, f"系统性风险检查异常: {_se}"

    return True, ""



# ─────────────────────────────────────────────────────────────────────────────
# 大行情熔断（Market Shock Detector）- 同步自根目录 risk_manager.py v3.1
# ─────────────────────────────────────────────────────────────────────────────
import time as _shock_time

_SHOCK_STATE_FILE = str(Path(__file__).parent.parent / "logs" / "market_shock_state.json")
_SHOCK_DURATION        = 30 * 60   # 熔断持续30分钟
_SHOCK_RECOVERY_WINDOW = 10 * 60   # 熔断解除后「恢复窗口」10分钟（约2轮扫描），期间信号降级
_SHOCK_BTC_THRESH = 5.0       # BTC 1H 涨跌幅阈值（绝对值 %）
_SHOCK_LIQ_THRESH = 500_000_000  # 全市场1H清算量阈值（5亿U）

def _load_shock_state() -> dict:
    try:
        p = Path(_SHOCK_STATE_FILE)
        if p.exists():
            return json.loads(p.read_text())
    except Exception:
        pass
    return {"active": False, "triggered_at": 0, "reason": ""}

def _save_shock_state(state: dict):
    p = Path(_SHOCK_STATE_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, ensure_ascii=False, indent=2))

def is_market_shock_active() -> bool:
    """检查大行情熔断是否仍在激活窗口内"""
    state = _load_shock_state()
    if not state.get("active"):
        return False
    elapsed = _shock_time.time() - state.get("triggered_at", 0)
    if elapsed >= _SHOCK_DURATION:
        state["active"] = False
        _save_shock_state(state)
        return False
    return True

def is_post_shock_recovery() -> bool:
    """
    大行情熔断解除后的「恢复窗口」检测（v3.31）。
    熔断解除后 _SHOCK_RECOVERY_WINDOW（默认10分钟）内返回 True。
    此期间 ATR/EMA/RSI 处于失真状态，scanner 应对信号降级处理：
      - position_size_cap × 0.5
      - final_score 额外 -10 分
    触发熔断本身（active=True）时不进入恢复窗口（此时仍在熔断中，is_market_shock_active() 管控）。
    """
    state = _load_shock_state()
    # 熔断仍激活：不是恢复窗口
    if state.get("active"):
        return False
    triggered_at = state.get("triggered_at", 0)
    if triggered_at == 0:
        return False
    elapsed_since_trigger = _shock_time.time() - triggered_at
    # 熔断刚结束（30min < elapsed < 30min+10min）= 恢复窗口
    if _SHOCK_DURATION <= elapsed_since_trigger < (_SHOCK_DURATION + _SHOCK_RECOVERY_WINDOW):
        return True
    return False

def check_market_shock(notifier_fn=None) -> bool:
    """
    每轮扫描开始时调用。
    检测条件：BTC 1H 涨跌幅 > 5%，或全市场1H清算量 > 5亿U。
    触发时：激活熔断30分钟，收紧持仓止损。返回 True 表示本轮应暂停开仓。
    """
    if is_market_shock_active():
        return True

    shock_reason = None

    # 条件1：BTC 1H 涨跌幅检测
    # v3.59 改进：同时检查已闭合K线(-2)和当前未闭合K线(-1)
    # 赋予未闭合K线较低阈值（默认为3%），减少熔断最多落后0~60分钟的延迟
    try:
        from shared import binance_api as _api
        klines = _api.get_klines("BTCUSDT", "1h", limit=3)
        if len(klines) >= 2:
            # 已闭合K线：默认阈值
            k = klines[-2]
            open_p, close_p = float(k[1]), float(k[4])
            chg_pct = abs((close_p - open_p) / open_p * 100)
            if chg_pct >= _SHOCK_BTC_THRESH:
                shock_reason = f"BTC 1H 已闭合K线涨跌幅 {chg_pct:.1f}% ≥ {_SHOCK_BTC_THRESH}%"
            # 当前局实时未闭合K线：较低阈值（60%），用于早期预警
            if not shock_reason and len(klines) >= 3:
                k_live = klines[-1]   # 当前未闭合
                live_open, live_close = float(k_live[1]), float(k_live[4])
                live_chg = abs((live_close - live_open) / live_open * 100)
                _LIVE_THRESH = max(3.0, _SHOCK_BTC_THRESH * 0.6)  # 当前局用较低阈值
                if live_chg >= _LIVE_THRESH:
                    shock_reason = f"BTC 1H 实时涨跌幅 {live_chg:.1f}% ≥ {_LIVE_THRESH:.0f}%(已闭合阈值{_SHOCK_BTC_THRESH}%的60%)"
    except Exception:
        pass

    # 条件②：全市场1H清算量（通过 clawby_adapter 获取，不再依赖不存在的函数）
    if not shock_reason:
        try:
            from shared.clawby_adapter import get_liq_coin_list, get_liq_summary_1h
            coin_list = get_liq_coin_list()
            if coin_list:
                # coin-list 各条目可能含 liquidation_usd / liq_usd 等字段，汇总后除以24得1h均值
                _LIQ_KEYS = ("liquidation_usd", "liq_usd", "liq_24h", "liquidations")
                total_24h = sum(
                    float(c.get(k, 0) or 0)
                    for c in coin_list if isinstance(c, dict)
                    for k in _LIQ_KEYS if c.get(k)
                )
                liq = total_24h / 24 if total_24h > 0 else 0.0
            else:
                # fallback：BTC ≈ 20% 全市场，× 5 估算全市场1H清算量
                _btc = get_liq_summary_1h("BTC")
                btc_1h = _btc.get("long_liq_1h", 0) + _btc.get("short_liq_1h", 0)
                liq = btc_1h * 5
            if liq and liq >= _SHOCK_LIQ_THRESH:
                shock_reason = f"全市场1H清算量 {liq/1e8:.1f}亿U ≥ 5亿U"
        except Exception:
            pass

    if not shock_reason:
        return False

    # 触发熔断
    state = {"active": True, "triggered_at": _shock_time.time(), "reason": shock_reason}
    _save_shock_state(state)

    msg = (
        f"🚨 大行情熔断触发\n原因：{shock_reason}\n"
        f"状态：暂停新开仓 30 分钟"
    )
    if notifier_fn:
        try:
            notifier_fn(msg)
        except Exception:
            pass
    logger.warning(f"[market_shock] {msg}")
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# 杠杆利用率预算（v3.90）
# ═══════════════════════════════════════════════════════════════════════════════
_LEV_BULL_PHASE_KEY = {"early_bull": "BULL_EARLY", "mid_bull": "BULL_MID", "late_bull": "BULL_LATE"}
_LEV_BEAR_KEY = {"weak": "BEAR_WEAK", "moderate": "BEAR_MODERATE", "strong": "BEAR_STRONG"}


def _leverage_ratio(direction: str) -> float:
    """
    查表得到该方向的名义预算比例。
      mode=bull  → 用 get_market_cycle_phase() 取 early/mid/late，取不到按 mid
      mode=bear  → 用 get_bear_strength() 取 weak/moderate/strong，取不到（None）按 moderate
      其他（range 等）→ range 档
    direction 非 LONG/SHORT 抛 ValueError（由调用方降级处理）。
    """
    import config as _cfg
    d = (direction or "").upper()
    if d not in ("LONG", "SHORT"):
        raise ValueError(f"direction 非法: {direction!r}")
    mode = get_market_mode()
    if mode == "bull":
        key = _LEV_BULL_PHASE_KEY.get(get_market_cycle_phase(), "BULL_MID")
    elif mode == "bear":
        key = _LEV_BEAR_KEY.get(get_bear_strength(), "BEAR_MODERATE")
    else:
        key = "RANGE"
    return float(getattr(_cfg, f"LEVERAGE_{d}_RATIO_{key}"))


def calc_leverage_remaining(direction: str, equity: float) -> float:
    """
    返回该方向还能开的最大名义仓位（USDT）。
      budget    = equity × LEVERAGE_TARGET × ratio(方向, 市场状态)
      used      = Σ state 里同方向 open/pending/patching 仓位的 quantity_remaining × entry_price
                  （tp1_filled=True 的仓位同样计入：不占槽位，但名义照算）
      remaining = max(0, budget - used)
      返回 min(remaining, equity × MAX_SINGLE_NOMINAL_RATIO)
    任何异常都返回 equity × MAX_SINGLE_NOMINAL_RATIO（降级放行，不阻断开仓）；
    equity 本身无法使用时返回 inf（不限制）。只读 state，不调用交易所接口。
    """
    import math
    try:
        _single_ratio = 1.5
        try:
            from config import MAX_SINGLE_NOMINAL_RATIO as _single_ratio
        except Exception:
            pass
        try:
            equity = float(equity)
            if not math.isfinite(equity):
                return float("inf")
        except (TypeError, ValueError):
            return float("inf")
        single_cap = equity * float(_single_ratio)
        try:
            from config import LEVERAGE_TARGET as _target
            ratio = _leverage_ratio(direction)
            budget = equity * float(_target) * ratio
            from shared.state import get_all_positions as _gap
            d = (direction or "").upper()
            used = 0.0
            for p in _gap():
                if p.get("status") not in ("open", "pending", "patching", "pending_guard"):
                    continue
                if (p.get("direction") or "").upper() != d:
                    continue
                try:
                    q = p.get("quantity_remaining")
                    if q is None:
                        q = p.get("quantity", 0)
                    n = float(q) * float(p.get("entry_price", 0) or 0)
                    if math.isfinite(n) and n > 0:
                        used += n
                except (TypeError, ValueError):
                    continue
            remaining = max(0.0, budget - used)
            logger.debug(f"[leverage] {d} ratio={ratio} budget={budget:.0f} used={used:.0f} remaining={remaining:.0f}")
            return min(remaining, single_cap)
        except Exception as e:
            # [fix #29] 异常 → 剩余预算 0（调用方据此拒绝开仓）
            logger.error(f"[leverage] calc_leverage_remaining 异常，剩余预算置 0（拒绝开仓）: {e}")
            return 0.0
    except Exception as e:
        logger.error(f"[leverage] calc_leverage_remaining 外层异常，剩余预算置 0（拒绝开仓）: {e}")
        return 0.0
