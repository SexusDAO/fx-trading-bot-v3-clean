"""BTC 统一状态模块：signal_watcher 和 trigger_detector 共用同一份状态。

维护 BTC markPrice 的 1 小时滚动窗口，按规则判断：
  crash_warn：5 分钟跌 >= 1% 或 30 分钟跌 >= 2%  → 撤 LONG 挂单，暂停新 LONG
  crash     ：1 小时跌 >= 3%                      → 同上，另暂停 reversal/stop_hunt/range_bounce LONG
  pump_warn ：5 分钟涨 >= 1% 或 30 分钟涨 >= 2%  → 撤 SHORT 挂单，暂停新 SHORT
  pump      ：1 小时涨 >= 3%                      → 同上，另暂停 reversal SHORT
  normal    ：连续 30 分钟不满足以上任何条件后恢复
对外接口：update_price(price, ts) / get_state() / is_allowed(direction, strategy)
"""
import bisect
import logging
import threading
import time
from collections import deque
from typing import Optional

logger = logging.getLogger("fx.btc_guard")

NORMAL, CRASH_WARN, CRASH, PUMP_WARN, PUMP = "normal", "crash_warn", "crash", "pump_warn", "pump"
WINDOW_SEC = 3600
RECOVER_SEC = 1800
_SEVERITY = {NORMAL: 0, CRASH_WARN: 1, PUMP_WARN: 1, CRASH: 2, PUMP: 2}


class BtcGuard:
    def __init__(self, clock=time.time):
        self._clock = clock
        self._ts: deque = deque()
        self._px: deque = deque()
        self._state = NORMAL
        self._last_hit_ts = 0.0
        self._lock = threading.Lock()
        self._listeners = []

    # ── 内部 ─────────────────────────────────────────────────────────
    def _price_ago(self, now, sec) -> Optional[float]:
        """返回 sec 秒前（或窗口内最接近的更早点）的价格；样本不足返回 None。"""
        if not self._ts or now - self._ts[0] < sec * 0.8:
            return None
        ts = list(self._ts)
        i = bisect.bisect_right(ts, now - sec) - 1
        return self._px[max(i, 0)]

    def _evaluate(self, now, price) -> str:
        def chg(sec):
            p0 = self._price_ago(now, sec)
            return (price - p0) / p0 if p0 else 0.0
        c5, c30, c60 = chg(300), chg(1800), chg(3600)
        if c60 <= -0.03:
            return CRASH
        if c60 >= 0.03:
            return PUMP
        if c5 <= -0.01 or c30 <= -0.02:
            return CRASH_WARN
        if c5 >= 0.01 or c30 >= 0.02:
            return PUMP_WARN
        return NORMAL

    # ── 公共接口 ─────────────────────────────────────────────────────
    def add_listener(self, fn) -> None:
        """fn(old_state, new_state) 在状态升级时调用（同步，调用方自行调度异步任务）。"""
        self._listeners.append(fn)

    def update_price(self, price: float, ts: Optional[float] = None) -> str:
        if not price or price <= 0:
            return self._state
        now = float(ts if ts is not None else self._clock())
        if now > 1e11:          # 兼容毫秒时间戳
            now /= 1000.0
        with self._lock:
            if self._ts and now < self._ts[-1]:
                return self._state
            self._ts.append(now)
            self._px.append(float(price))
            while self._ts and now - self._ts[0] > WINDOW_SEC + 60:
                self._ts.popleft()
                self._px.popleft()
            hit = self._evaluate(now, float(price))
            old = self._state
            if hit != NORMAL:
                self._last_hit_ts = now
                # 同类只升不降；反向直接切换
                same_side = hit[:4] == old[:4]
                if old == NORMAL or not same_side or _SEVERITY[hit] > _SEVERITY[old]:
                    self._state = hit
            elif old != NORMAL and now - self._last_hit_ts >= RECOVER_SEC:
                self._state = NORMAL
            new = self._state
        if new != old:
            lvl = logging.WARNING if new != NORMAL else logging.INFO
            logger.log(lvl, f"[btc_guard] 状态 {old} → {new} (BTC={price})")
            for fn in list(self._listeners):
                try:
                    fn(old, new)
                except Exception as e:
                    logger.debug(f"[btc_guard] listener 异常: {e}")
        return new

    def get_state(self) -> str:
        return self._state

    def is_allowed(self, direction: str, strategy: str = "") -> bool:
        d, st = (direction or "").upper(), (strategy or "").lower()
        s = self._state
        if d == "LONG" and s in (CRASH_WARN, CRASH):
            return False
        if d == "SHORT" and s in (PUMP_WARN, PUMP):
            return False
        return True

    def cancel_direction(self) -> Optional[str]:
        """当前状态下应撤销的挂单方向：LONG / SHORT / None。"""
        s = self._state
        if s in (CRASH_WARN, CRASH):
            return "LONG"
        if s in (PUMP_WARN, PUMP):
            return "SHORT"
        return None

    def reset(self) -> None:
        with self._lock:
            self._ts.clear()
            self._px.clear()
            self._state = NORMAL
            self._last_hit_ts = 0.0


_default = BtcGuard()


def get_guard() -> BtcGuard:
    return _default


def update_price(price: float, ts: Optional[float] = None) -> str:
    return _default.update_price(price, ts)


def get_state() -> str:
    return _default.get_state()


def is_allowed(direction: str, strategy: str = "") -> bool:
    return _default.is_allowed(direction, strategy)
