"""
shared/signal_watcher.py — 信号入场价格监控 + 限价挂单管理

核心流程：
  1. scanner 扫到信号 → 预算 entry_price → 注册到 SignalWatcher
  2. WS 实时价格驱动：价格进入 [entry_low, entry_high] 时立刻下限价单
     - 限价单价格 = entry_price（EMA20 / 清算区锚点）
     - 不是市价追单，而是挂在理想点位等撮合
  3. 交易所维护挂单：
     - 有效期到期（breakout 30min / pullback+reversal 4h）→ 自动撤单
     - 仓位满时 → 撤销所有待触发的入场挂单
  4. 成交由 executor 的 WS/REST 链路处理

触发区间设计（以 entry_price 为锚，config 可调）：
  pullback:  entry_price ± 3%   — 山寨行情 1 分钟 3% 波动属正常，提前埋好限价单
  reversal:  entry_price ± 3%
  breakout:  mark_price  ± 1%   — 突破型追涨不能给太多余量

线程安全：
  - on_price_update 由主 loop 的 ws_listener 回调调用，天然在主 loop
  - add_signal 由子线程通过 run_coroutine_threadsafe 投递到主 loop
  - 所有 async 方法均在主 loop 内执行，asyncio.Lock 使用安全
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional

try:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from config import (
        SIGNAL_WATCH_BREAKOUT_EXPIRE_MIN        as _BREAKOUT_EXP_MIN,
        SIGNAL_WATCH_PULLBACK_EXPIRE_MIN        as _PULLBACK_EXP_MIN,
        SIGNAL_WATCH_GC_INTERVAL_SEC            as _GC_INTERVAL,
        SIGNAL_WATCH_ENTRY_BUFFER_PCT_PULLBACK  as _BUF_PULLBACK,
        SIGNAL_WATCH_ENTRY_BUFFER_PCT_REVERSAL  as _BUF_REVERSAL,
        SIGNAL_WATCH_ENTRY_BUFFER_PCT_BREAKOUT  as _BUF_BREAKOUT,
    )
    try:
        from config import BREAKOUT_LIQ_FILTER_ENABLED as _BREAKOUT_LIQ_ENABLED
    except ImportError:
        _BREAKOUT_LIQ_ENABLED = False
    try:
        from config import SURVIVAL_RSI_SHORT_CANCEL as _SURVIVAL_RSI_SC
        from config import SURVIVAL_RSI_LONG_CANCEL  as _SURVIVAL_RSI_LC
    except ImportError:
        _SURVIVAL_RSI_SC, _SURVIVAL_RSI_LC = 60, 45
    try:
        from config import PENDING_SCORE_TYPE_MAP as _SCORE_TYPE_MAP
    except ImportError:
        _SCORE_TYPE_MAP = {"reversal":20,"breakout":15,"stop_hunt":15,"pullback":12,
                           "oi_divergence":12,"ema_trend":10,"range_bounce":8,"funding_arb":5}
except Exception:
    _BREAKOUT_EXP_MIN = 30
    _PULLBACK_EXP_MIN = 240
    _GC_INTERVAL      = 60.0
    _BUF_PULLBACK     = 0.03   # 默认 ±3%
    _BUF_REVERSAL     = 0.03
    _BUF_BREAKOUT     = 0.01
    _BREAKOUT_LIQ_ENABLED = False
    # [v3.89] config 整体导入失败时，下面两组名字在主 try 里是内层 try 才定义的，外层回退漏掉了它们，
    #   导致 _calc_pending_score / 存活检查在回退路径上抛 NameError。补上与内层 ImportError 回退完全相同的默认值。
    _SURVIVAL_RSI_SC, _SURVIVAL_RSI_LC = 60, 45
    _SCORE_TYPE_MAP = {"reversal":20,"breakout":15,"stop_hunt":15,"pullback":12,
                       "oi_divergence":12,"ema_trend":10,"range_bounce":8,"funding_arb":5}

# ── 新闻信号模块（可选，失败自动降级）──────────────────────────────────────
try:
    from news_signal import get_news_bias as _sw_get_news_bias
    _SW_NEWS_OK = True
except Exception:
    _SW_NEWS_OK = False
    def _sw_get_news_bias(sym): return None  # type: ignore

logger = logging.getLogger("fx.signal_watcher")


# ─────────────────────────────────────────────────────────────────────────────
# 数据结构
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class WatchEntry:
    signal:      dict
    entry_low:   float          # 触发区间下限
    entry_high:  float          # 触发区间上限
    entry_price: float          # 限价单挂单价（EMA20 / 清算区锚点）
    expire_ts:   float          # 过期 Unix 秒
    on_trigger:  Callable[..., Awaitable[None]]
    triggered:   bool  = False
    added_ts:    float = field(default_factory=time.time)
    # 挂单后记录 orderId，用于有效期到期撤单
    limit_order_id: Optional[int] = None
    # ── v3.67 触发时快照字段（用于 _survival_check 挂单存活判断）──────────
    # 开仓条件和存活条件分离：挂单存活只看「核心理由是否被证伪」，
    # 不重新跑完整 judge，避免 vol_ratio/15M形态等瞬时指标波动导致误撤。
    trigger_price:    float = 0.0   # 触发时的收盘价（mark_price）
    trigger_rsi_1h:   float = 50.0  # 触发时的 RSI1H
    breakout_ref:     float = 0.0   # breakout 专用，突破基准价（ref_high 或 ref_low）
    ema20_at_trigger: float = 0.0   # pullback 专用，触发时的 EMA20
    atr_at_trigger:   float = 0.0   # 触发时的 ATR（1H）
    # ── v3.72 竞争机制字段 ───────────────────────────────────────────────────
    pending_score:         float = 0.0   # 当前综合分（动态更新，用于竞争和低分撤单）
    score_created_at:      float = 0.0   # 原始信号时间（重挂不刷新，时效性衰减基准）
    score_original_expire: float = 0.0   # 原始有效期（重挂不刷新）
    is_reorder:            bool  = False # 是否为重挂单（价格调整），受竞争保护
    reorder_count:         int   = 0     # 重挂次数
    # ── [v3.89 #40] 撤单状态机 / 条目身份（均有默认值，不影响既有构造方式）──────────
    # cancel_state: none（正常）/ cancelling（撤单进行中，禁止触发）/ cancel_unknown（撤单结果未知，待对账）
    # 状态转换必须在 SignalWatcher._lock 内完成。
    entry_id:     str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    cancel_state: str = "none"
    # [v3.89 #102] 触发结果状态：none=正常 / unknown=触发结果未知（on_trigger 在可能已下单之后失败），待人工核查
    trigger_state: str = "none"

    @property
    def symbol(self) -> str:
        return self.signal.get("symbol", "")

    @property
    def mode(self) -> str:
        return self.signal.get("mode", "pullback")

    def is_expired(self) -> bool:
        return time.time() > self.expire_ts

    def price_in_range(self, price: float) -> bool:
        return self.entry_low <= price <= self.entry_high


# ─────────────────────────────────────────────────────────────────────────────
# 入场区间计算
# ─────────────────────────────────────────────────────────────────────────────

def compute_entry_range(
    signal: dict,
    mark_price: float,
    liq_long: list | None = None,
    liq_short: list | None = None,
) -> tuple[float, float, float]:
    """
    返回 (entry_low, entry_high, entry_price)。

    entry_price：限价单实际挂单价（EMA20 / 清算区锚点）。
    entry_low/high：触发区间（价格进入后下限价单）。

    breakout 模式优先使用清算区锚点：
      1. 拿 [mark_price * (1-LIQ_SCAN_RANGE), mark_price) 区间内最近的多头清算区
      2. 有清算区 → entry_p = 清算区中价；触发区间 = [liq_mid*(1-buf), mark_price*(1+buf)]
      3. 无清算区 / liq 数据缺失 → 降级：与原来逻辑相同，mark_price ±1% 追价
    """
    mode        = signal.get("mode", "pullback")
    entry_price = signal.get("pre_entry_price") or mark_price

    if mode == "breakout":
        direction = signal.get("direction", "LONG").upper()
        # ── 突破位回踩入场逻辑（v3.21）──────────────────────────────────────
        # 核心思路：不追高，等价格从突破后的高点回踩到突破位（ref_high/ref_low），
        # 确认突破位转支撑/压力后再入场，胜率和盈亏比均改善。
        #
        # 触发区间 = ref_high ± 0.5%（LONG）/ ref_low ± 0.5%（SHORT）
        # 入场价   = ref_high + ATR × 0.1（刚好在突破位上方一点点，LONG）
        #            ref_low  - ATR × 0.1（SHORT）
        #
        # 降级条件：若 ref_high/ref_low 已高于当前价（价格已回落到突破位以下），
        # 退化为清算区锚点或 mark_price ± 1% 追价（不等回踩，直接进）

        _reason      = signal.get("reason", {}) or {}
        ref_high     = _reason.get("breakout_ref_high", 0)
        ref_low      = _reason.get("breakout_ref_low",  0)
        atr_sig      = signal.get("ind", {}).get("atr_1h", mark_price * 0.02)

        buf_ref = 0.005   # 突破位回踩窗口 ± 0.5%

        # [v3.87 OPT-13] 突破延续入场。原逻辑价格在突破位上方时一律只等回踩（有效期 30 分钟），真突破不回踩就整单错过。
        #   judge_breakout 已要求放量 + 15M 确认K线，信号本身已确认；价格距突破位 ≤ 0.5ATR 时回踩空间很小，等回踩的收益
        #   低于错过的概率，直接在现价附近入场（止损离突破位也近，失效判断清晰）。
        #   离突破位 > 0.5ATR（已走远）仍等回踩，避免追高。开关 OPT_BREAKOUT_MOMENTUM_ENTRY。
        try:
            from config import OPT_BREAKOUT_MOMENTUM_ENTRY as _OPT_BME
        except Exception:
            _OPT_BME = False
        _near_ref = False
        if _OPT_BME and atr_sig > 0:
            if direction == "LONG" and ref_high > 0 and 0 < mark_price - ref_high <= 0.5 * atr_sig:
                _near_ref = True
            elif direction == "SHORT" and ref_low > 0 and 0 < ref_low - mark_price <= 0.5 * atr_sig:
                _near_ref = True
        if _near_ref:
            entry_p = mark_price
            low     = mark_price * (1 - buf_ref)
            high    = mark_price * (1 + buf_ref)
            logger.info(f"[signal_watcher] breakout {direction} 紧贴突破位(≤0.5ATR)，延续入场 entry={entry_p:.6g}")
        elif direction == "LONG" and ref_high > 0:
            if mark_price > ref_high:
                # 价格仍在突破位上方 → 等回踩
                entry_p = ref_high + atr_sig * 0.1   # 略高于突破位，maker 吃单
                low     = ref_high * (1 - buf_ref)
                high    = ref_high * (1 + buf_ref)
                logger.info(
                    f"[signal_watcher] breakout LONG 等回踩突破位: "
                    f"ref_high={ref_high:.6g} 触发区间=[{low:.6g},{high:.6g}] entry={entry_p:.6g}"
                )
            else:
                # 价格已低于突破位（回落过深）→ 降级：mark_price 追价
                entry_p = mark_price
                low     = mark_price * (1 - buf_ref * 2)
                high    = mark_price * (1 + buf_ref * 2)
                logger.info(f"[signal_watcher] breakout LONG 价格已低于突破位，降级追价")

        elif direction == "SHORT" and ref_low > 0:
            if mark_price < ref_low:
                # 价格仍在突破位下方 → 等反弹
                entry_p = ref_low - atr_sig * 0.1
                low     = ref_low * (1 - buf_ref)
                high    = ref_low * (1 + buf_ref)
                logger.info(
                    f"[signal_watcher] breakout SHORT 等反弹突破位: "
                    f"ref_low={ref_low:.6g} 触发区间=[{low:.6g},{high:.6g}] entry={entry_p:.6g}"
                )
            else:
                entry_p = mark_price
                low     = mark_price * (1 - buf_ref * 2)
                high    = mark_price * (1 + buf_ref * 2)
                logger.info(f"[signal_watcher] breakout SHORT 价格已高于突破位，降级追价")

        else:
            # 无 ref 数据 → 兜底追价
            entry_p = mark_price
            low     = mark_price * (1 - 0.01)
            high    = mark_price * (1 + 0.01)
    else:
        # v3.51 stop_hunt：二探入场区间——等价格回踩 pivot_level 附近再入
        # 收回确认后不直接入，而是挂限价单等 pivot 附近的二次确认——提高入场价质量
        if mode == "stop_hunt":
            direction = signal.get("direction", "LONG").upper()
            _jd        = signal.get("judge_detail") or {}
            _rl        = float(_jd.get("ref_level") or _jd.get("pivot_level") or entry_price or mark_price)
            if _rl > 0:
                if direction == "LONG":
                    # LONG: 等价格从下方回踩 pivot
                    # 下限 pivot*0.990：至少二探 1% 才挂单，过浅回踩不触发（避免 TP 空间不足）
                    # 上限 pivot*0.998：超过此价格说明未真正回踩到 pivot，不入
                    low     = _rl * 0.990
                    high    = _rl * 0.998
                    entry_p = _rl * 0.995   # 区间中位，渐进式挂单
                else:
                    # SHORT: 等价格从上方回踩 pivot
                    # 下限 pivot*1.002：超过此价格说明未真正回测到 pivot，不入
                    # 上限 pivot*1.010：至少二探 1% 才挂单
                    low     = _rl * 1.002
                    high    = _rl * 1.010
                    entry_p = _rl * 1.005   # 区间中位
                logger.info(
                    f"[signal_watcher] stop_hunt {direction} 二探区间: "
                    f"pivot={_rl:.6g} 触发区间=[{low:.6g},{high:.6g}] entry={entry_p:.6g}"
                )
            else:
                # 降级：无 ref_level 时用 mark_price
                buf     = _BUF_PULLBACK
                low     = mark_price * (1 - buf)
                high    = mark_price * (1 + buf)
                entry_p = mark_price
        # pullback / reversal：以 entry_price 为中心
        elif mode == "pullback":
            buf        = _BUF_PULLBACK
            low        = entry_price * (1 - buf)
            high       = entry_price * (1 + buf)
            entry_p    = entry_price
        elif mode == "reversal":
            # [fix] 触发区间改用 ATR 维度，防止 mark 价已在区间内导致秒触发
            #   原 ±3% 固定比例在 entry 因降级紧贴 mark 时完全失效
            try:
                from config import SIGNAL_WATCH_ENTRY_BUFFER_ATR_REVERSAL as _BUF_REVERSAL_ATR
            except ImportError:
                _BUF_REVERSAL_ATR = 0.3   # 与 config.SIGNAL_WATCH_ENTRY_BUFFER_ATR_REVERSAL 保持一致
            atr_sig    = (signal.get("ind") or {}).get("atr_1h") or mark_price * 0.02
            half       = atr_sig * _BUF_REVERSAL_ATR
            low        = entry_price - half
            high       = entry_price + half
            entry_p    = entry_price
        else:
            buf        = _BUF_PULLBACK
            low        = entry_price * (1 - buf)
            high       = entry_price * (1 + buf)
            entry_p    = entry_price

    return low, high, entry_p


# ─────────────────────────────────────────────────────────────────────────────
# SignalWatcher
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# v3.72 挂单竞争评分
# ─────────────────────────────────────────────────────────────────────────────
_IV_SEC = {"15m": 900, "1h": 3600, "4h": 14400}


def _get_hub_or_rest(sym: str, data_type: str, interval: str = "1h", limit: int = 50):
    """[v2-11] 优先读 hub：markPrice age_sec>3、K 线 close_time 超过周期+10 秒 → 退回 REST 并写 WARNING。
    data_type: "mark" 返回 premiumIndex dict；"klines" 返回 K 线列表。"""
    from shared import binance_api as _api
    hub = None
    try:
        from shared.market_data_hub import get_hub
        hub = get_hub()
    except Exception:
        hub = None
    if data_type == "mark":
        if hub is not None:
            try:
                mp = hub.get_mark_price(sym)
                if mp and float(mp.get("age_sec", 99)) <= 3:
                    return mp
                logger.warning(f"[signal_watcher] {sym} hub markPrice 过旧/缺失"
                               f"(age={None if not mp else mp.get('age_sec')})，退回 REST")
            except Exception as e:
                logger.warning(f"[signal_watcher] {sym} hub markPrice 读取异常，退回 REST: {e}")
        return _api.get_mark_price(sym) or {}
    if hub is not None:
        try:
            k = hub.get_klines(sym, interval, limit)
            if k:
                lim = (float(k[-1][6]) / 1000.0) + _IV_SEC.get(interval, 3600) + 10
                if time.time() <= lim:
                    return k
            logger.warning(f"[signal_watcher] {sym} hub {interval} K线过旧/缺失，退回 REST")
        except Exception as e:
            logger.warning(f"[signal_watcher] {sym} hub K线读取异常，退回 REST: {e}")
    return _api.get_klines(sym, interval, limit) or []


def _liq_freshness(sym: str, now: float) -> float:
    """[v2-12] 清算图新鲜度系数：<=5min 1.0；5~15min 线性递减；>15min 0。读 liq_cache 条目 ts（取最新）。"""
    try:
        from shared import liq_cache as _lc
        with _lc._CACHE_LOCK:
            tss = [float((v or {}).get("ts", 0)) for k, v in _lc._CACHE.items()
                   if str(k).startswith(f"{sym}:")]
        if not tss:
            return 0.0
        age = now - max(tss)
    except Exception:
        return 1.0
    if age <= 300:
        return 1.0
    if age >= 900:
        return 0.0
    return 1.0 - (age - 300) / 600.0


def _calc_pending_score(entry: 'WatchEntry', now: float | None = None) -> float:
    """
    计算挂单综合分（0-100）。纯函数，无副作用。

    ① 信号强度分（40分）：strong=40 / medium=25 / weak=10
    ② 策略类型分（20分）：从 config.PENDING_SCORE_TYPE_MAP 读取
    ③ 时效性分（20分）：基于原始信号时间衰减，重挂不重置
    ④ 清算图质量分（20分）：liq_data_available + liq_zone 金额
    """
    import time as _t
    now = now or _t.time()

    _type_map = _SCORE_TYPE_MAP  # 模块顶层已导入

    sig = entry.signal
    jd  = sig.get("judge_detail") or {}

    # ① 信号强度
    strength = sig.get("signal_strength") or jd.get("signal_strength") or "medium"
    s1 = {"strong": 40, "medium": 25, "weak": 10}.get(str(strength).lower(), 15)

    # ② 策略类型
    mode = entry.mode or sig.get("sig_type", "pullback")
    s2 = _type_map.get(mode, 8)

    # ③ 时效性（用 score_created_at 和 score_original_expire，重挂不重置）
    t0  = entry.score_created_at or now
    t1  = entry.score_original_expire or (t0 + 4 * 3600)
    window = max(1, t1 - t0)
    elapsed = max(0, now - t0)
    ratio = min(1.0, elapsed / window)
    s3 = max(0, 20 * (1 - ratio))

    # ④ 清算图质量
    liq_ok = sig.get("liq_data_available") or jd.get("liq_data_available") or False
    if not liq_ok:
        s4 = 0
    else:
        # 尝试从 judge_detail 里读取清算量（若无，给基础分）
        liq_amt = float(jd.get("liq_zone_amount") or jd.get("liq_amount") or 0)
        if liq_amt >= 3_000_000:
            s4 = 20
        elif liq_amt >= 1_000_000:
            s4 = 12
        elif liq_amt > 0:
            s4 = 6
        else:
            s4 = 8  # liq_ok=True 但没有具体金额，给个基础分

    if s4:
        s4 = s4 * _liq_freshness(entry.symbol, now)   # [v2-12] 权重仍 20，按新鲜度递减
    return min(100.0, s1 + s2 + s3 + s4)


class SignalWatcher:
    """
    信号监控 + 限价挂单管理。

    新增能力（相比旧版）：
    - 触发时下限价单（而非直接调 execute_signal），让价格在 entry_price 成交
    - 到期自动撤单
    - 支持 cancel_all()，仓位满时一键撤所有入场挂单
    """

    def __init__(self) -> None:
        self._watched: Dict[str, WatchEntry] = {}
        self._lock     = asyncio.Lock()
        self._gc_task:      Optional[asyncio.Task] = None
        self._polling_task: Optional[asyncio.Task] = None
        # 外部注入
        self._ws_listener: Any  = None
        self._main_loop:   Any  = None
        # [v3.89 #40] 撤单结果未知的订单对账表：(symbol, order_id) -> 记录；_gc_pass 定期重试，最多 5 次
        self._orphan_orders: Dict[tuple, dict] = {}
        # v3.33 5分钟内同方向触发上限
        # {"LONG": last_trigger_ts, "SHORT": last_trigger_ts}
        self._dir_last_trigger: Dict[str, float] = {}
        self._DIR_TRIGGER_COOLDOWN = 300  # 5分钟内同方向最多触发 1 个

        # v3.55 BTC 即时波动监控（WebSocket 实时检测，不等下一轮扫描）
        # 历史数据依据：2024-08-05 日元崩盘时 1min 最大 HL=4.2%，但极罕见
        # 日常中等闪崩（合约瀑布）5min 约 1%，每周出现约 1 次
        # 选择 5min 窗口 / 1% 门槛：捕获真实危险行情，不被日常波动触发
        self._btc_price_prev: float = 0.0   # 上一个 BTC 价格快照
        self._btc_prev_ts:    float = 0.0   # 快照时间戳
        # [v3.89 #101] 事件新鲜度：每个标的最后一个有效事件时间（毫秒）+ 丢弃计数
        self._last_event_ts: Dict[str, int] = {}
        self._evt_drop: Dict[str, int] = {}
        self._evt_drop_log_ts: Dict[tuple, float] = {}
        self._BTC_SNAP_INTERVAL = 300.0     # 每5分钟更新一次 BTC 基准价（对比5min变化）
        self._BTC_MOVE_THRESH   = 0.01      # BTC 5min涨跌 >= 1% 触发处置
        self._btc_cancel_cooldown: float = 0.0   # 上次触发撤单的时间，防抖 10 分钟
        # [v2-3] 按 symbol 的注册/下单锁；[v2-2] 指标缓存；[v2-8] open 持仓缓存
        self._sym_locks: Dict[str, asyncio.Lock] = {}
        self._ind_cache: Dict[str, dict] = {}
        self._open_pos_cache: list = []
        self._open_pos_ts: float = 0.0
        self._sl_breach_alerted: Dict[str, float] = {}

    # ─────────────────────────────────────────────────────────────────────────
    # 公共接口
    # ─────────────────────────────────────────────────────────────────────────

    def register_to_ws(self, ws_listener: Any) -> None:
        self._ws_listener = ws_listener
        ws_listener.on_price_event(self.on_price_update)
        # v3.54: 始终订阅 BTCUSDT，确保 BTC 即时波动检测不依赖是否有 BTC 信号
        ws_listener.subscribe("BTCUSDT")
        logger.info("[signal_watcher] 已注册到 ws_listener 价格流（BTCUSDT 常驻订阅）")

    async def add_signal(
        self,
        signal:      dict,
        entry_low:   float,
        entry_high:  float,
        entry_price: float,
        expire_ts:   float,
        on_trigger:  Callable[..., Awaitable[None]],
        # ── v3.67 触发快照字段（可选，用于 _survival_check）──────────────
        trigger_price:    float = 0.0,
        trigger_rsi_1h:   float = 50.0,
        breakout_ref:     float = 0.0,
        ema20_at_trigger: float = 0.0,
        atr_at_trigger:   float = 0.0,
        # ── v3.72 竞争机制字段 ──────────────────────────────────────────────
        pending_score:         float = 0.0,
        score_created_at:      float = 0.0,
        score_original_expire: float = 0.0,
        is_reorder:            bool  = False,
        reorder_count:         int   = 0,
        # [v3.87 STEP5-8] replace=True：允许覆盖同标的已有的未触发条目（价格位移重挂用，先注册新单再撤旧单）
        replace:               bool  = False,
    ) -> None:
        """注册信号监控。"""
        symbol = signal.get("symbol", "")
        if not symbol:
            return
        _mode0 = signal.get("sig_type") or signal.get("mode") or ""
        _dir0 = str(signal.get("direction", "LONG")).upper()
        # [v2-6] funding_arb 最晚在结算前 60 秒过期
        if _mode0 == "funding_arb":
            _nft = float(signal.get("next_settle_ts") or signal.get("next_funding_time") or signal.get("nextFundingTime")
                         or (signal.get("ind") or {}).get("next_funding_time") or 0)
            if _nft:
                _deadline = (_nft / 1000.0 if _nft > 1e11 else _nft) - 60
                if _deadline < time.time():
                    logger.info(f"[signal_watcher] {symbol} funding_arb 已过结算前 60s 期限，拒绝注册")
                    return
                expire_ts = min(expire_ts, _deadline)
        # [v2-5] BTC 统一状态
        try:
            from shared import btc_guard as _bg
            if not _bg.is_allowed(_dir0, _mode0):
                logger.info(f"[signal_watcher] {symbol} {_dir0} 被 btc_guard 拦截（state={_bg.get_state()}）")
                return
        except Exception:
            pass
        # [v2-3] 按 symbol 加锁，5 秒拿不到放弃
        _slock = self._sym_locks.setdefault(symbol, asyncio.Lock())
        try:
            await asyncio.wait_for(_slock.acquire(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning(f"[signal_watcher] {symbol} 注册锁等待超过 5s，放弃新信号")
            return
        try:
            await self._add_signal_locked(signal, entry_low, entry_high, entry_price, expire_ts, on_trigger,
                                          trigger_price, trigger_rsi_1h, breakout_ref, ema20_at_trigger,
                                          atr_at_trigger, pending_score, score_created_at, score_original_expire,
                                          is_reorder, reorder_count, replace)
        finally:
            _slock.release()

    async def _exchange_conflict(self, symbol: str, own_order_id) -> Optional[str]:
        """[v2-3] 查交易所真实持仓/挂单；排除本系统登记的挂单。返回冲突原因或 None。"""
        from shared import binance_api as _api
        loop = asyncio.get_running_loop()
        try:
            pos = await loop.run_in_executor(None, lambda: _api.get_position(symbol)) or []
            for p in pos:
                if abs(float(p.get("positionAmt", 0) or 0)) > 0:
                    return "交易所已有持仓"
            orders = await loop.run_in_executor(None, lambda: _api.get_open_orders(symbol)) or []
            own = {int(e.limit_order_id) for e in self._watched.values() if e.limit_order_id}
            if own_order_id:
                own.add(int(own_order_id))
            for o in orders:
                if o.get("reduceOnly") or o.get("closePosition"):
                    continue
                if int(o.get("orderId", 0) or 0) not in own:
                    return "交易所有本地未知挂单"
        except Exception as e:
            logger.warning(f"[signal_watcher] {symbol} 查交易所持仓/挂单失败，保守放弃: {e}")
            return "交易所查询失败"
        return None

    async def _add_signal_locked(self, signal, entry_low, entry_high, entry_price, expire_ts, on_trigger,
                                 trigger_price, trigger_rsi_1h, breakout_ref, ema20_at_trigger, atr_at_trigger,
                                 pending_score, score_created_at, score_original_expire, is_reorder,
                                 reorder_count, replace) -> None:
        symbol = signal.get("symbol", "")
        # [v2-4] 同标的已有挂单：同方向高 8 分替换；反方向默认拒绝，高 20 分撤旧挂新
        _cur = self._watched.get(symbol)
        _replace_old = None
        if _cur and not replace and not _cur.triggered and not _cur.is_expired():
            if _cur.cancel_state != "none":
                logger.info(f"[signal_watcher] {symbol} 旧挂单撤单状态={_cur.cancel_state}，不下新单")
                return
            _probe = WatchEntry(signal=signal, entry_low=entry_low, entry_high=entry_high,
                                entry_price=entry_price, expire_ts=expire_ts, on_trigger=on_trigger,
                                score_created_at=score_created_at or time.time(),
                                score_original_expire=score_original_expire or expire_ts)
            _new_sc = _calc_pending_score(_probe)
            _old_sc = _cur.pending_score or _calc_pending_score(_cur)
            _same = _cur.signal.get("direction", "LONG").upper() == str(signal.get("direction", "LONG")).upper()
            if _same and _new_sc > _old_sc + 8:
                _replace_old = _cur
            elif not _same and _new_sc > _old_sc + 20:
                logger.warning(f"[signal_watcher] {symbol} 反向信号高出 {_new_sc - _old_sc:.0f} 分，撤旧挂新")
                _replace_old = _cur
            else:
                if not _same:
                    logger.info(f"[signal_watcher] {symbol} 方向冲突：已有 {_cur.signal.get('direction')} 挂单"
                                f"(分{_old_sc:.0f})，新 {signal.get('direction')}(分{_new_sc:.0f}) 被拒")
                else:
                    logger.info(f"[signal_watcher] {symbol} 新分{_new_sc:.0f} ≤ 旧分{_old_sc:.0f}+8，丢弃新信号")
                return
        _conf = await self._exchange_conflict(symbol, _replace_old.limit_order_id if _replace_old else None)
        if _conf:
            logger.info(f"[signal_watcher] {symbol} {_conf}，跳过注册")
            return
        if _replace_old is not None:
            if _replace_old.limit_order_id:
                _oc = await self._cancel_exchange_order(symbol, int(_replace_old.limit_order_id))
                if _oc != "confirmed":
                    if _oc == "unknown":
                        self._register_orphan(symbol, int(_replace_old.limit_order_id), "高分替换", _replace_old,
                                              on_confirmed="remove")
                    logger.warning(f"[signal_watcher] {symbol} 旧挂单撤单结果={_oc}，不下新单")
                    return
            async with self._lock:
                if self._watched.get(symbol) is _replace_old:
                    self._watched.pop(symbol, None)
            logger.info(f"[signal_watcher] {symbol} 高分新信号替换旧挂单")

        _evict_cancel = []   # [v3.89 撤单检查] 被驱逐条目的待撤订单（锁内登记，锁外执行）
        async with self._lock:
            existing = self._watched.get(symbol)
            if existing and not replace and not existing.triggered and not existing.is_expired():
                logger.info(
                    f"[signal_watcher] {symbol} 已在监控队列，跳过重复注册 (mode={existing.mode})"
                )
                return

            _now = time.time()
            _score_created = score_created_at or _now
            _score_expire  = score_original_expire or expire_ts
            entry = WatchEntry(
                signal=signal,
                entry_low=entry_low,
                entry_high=entry_high,
                entry_price=entry_price,
                expire_ts=expire_ts,
                on_trigger=on_trigger,
                trigger_price=trigger_price,
                trigger_rsi_1h=trigger_rsi_1h,
                breakout_ref=breakout_ref,
                ema20_at_trigger=ema20_at_trigger,
                atr_at_trigger=atr_at_trigger,
                pending_score=pending_score,
                score_created_at=_score_created,
                score_original_expire=_score_expire,
                is_reorder=is_reorder,
                reorder_count=reorder_count,
            )
            # ── v3.72 竞争机制：新信号（非重挂）才参与竞争 ──────────────────
            if not is_reorder:
                entry.pending_score = _calc_pending_score(entry, _now)
                try:
                    from config import (MAX_PENDING_LONG, MAX_PENDING_SHORT, MAX_PENDING_TOTAL,
                                        PENDING_MIN_SCORE_TO_COMPETE, PENDING_REPLACE_MARGIN)
                except ImportError:
                    MAX_PENDING_LONG = MAX_PENDING_SHORT = 3
                    MAX_PENDING_TOTAL = 5
                    PENDING_MIN_SCORE_TO_COMPETE = 40
                    PENDING_REPLACE_MARGIN = 8

                direction_new = signal.get("direction", "LONG").upper()
                # 同方向所有活跃 pending（含重挂单，它们占着槽位）
                same_dir = [e for e in self._watched.values()
                            if not e.triggered and not e.is_expired()
                            and e.signal.get("direction","LONG").upper() == direction_new]
                max_dir = MAX_PENDING_LONG if direction_new == "LONG" else MAX_PENDING_SHORT
                total_pending = len([e for e in self._watched.values()
                                     if not e.triggered and not e.is_expired()])

                if (len(same_dir) >= max_dir or total_pending >= MAX_PENDING_TOTAL):
                    # 槽位满 → 竞争
                    if entry.pending_score < PENDING_MIN_SCORE_TO_COMPETE:
                        logger.info(
                            f"[signal_watcher] {symbol} 分数 {entry.pending_score:.0f} "
                            f"< {PENDING_MIN_SCORE_TO_COMPETE}，直接丢弃（不参与竞争）"
                        )
                        # 评分不足，静默丢弃（不推送用户）
                        return  # 不注册

                    # 找最低分的非保护（非 is_reorder）挂单
                    evictable = [e for e in same_dir if not e.is_reorder]
                    if not evictable:
                        logger.info(
                            f"[signal_watcher] {symbol} 槽位满且全部为重挂单，无法替换，丢弃"
                        )
                        # 槽位满且全部受保护，静默丢弃
                        return

                    lowest = min(evictable, key=lambda e: e.pending_score)
                    if entry.pending_score <= lowest.pending_score + PENDING_REPLACE_MARGIN:
                        logger.info(
                            f"[signal_watcher] {symbol} 新分 {entry.pending_score:.0f} "
                            f"≤ 最低分 {lowest.pending_score:.0f}+{PENDING_REPLACE_MARGIN}，"
                            f"竞争未胜出，丢弃"
                        )
                        # 竞争未胜出，静默丢弃
                        return

                    # 替换最低分挂单
                    old_sym = lowest.symbol
                    logger.info(
                        f"[signal_watcher] {symbol} 新分 {entry.pending_score:.0f} "
                        f"> {old_sym} {lowest.pending_score:.0f}+{PENDING_REPLACE_MARGIN}，"
                        f"竞争胜出，替换"
                    )
                    self._watched.pop(old_sym, None)
                    # [v3.90 通知聚合] 被驱逐信号不再补写卡片行，改为静默丢弃缓冲区
                    try:
                        from shared.notifier import discard_signal
                        _old_sig_id = f"{old_sym}_{lowest.signal.get('_scan_ts', '')}"
                        discard_signal(_old_sig_id)
                    except Exception as _sw_e:
                        logger.debug(f"[signal_watcher] 驱逐卡片丢弃失败: {_sw_e}")
                    if lowest.limit_order_id:
                        # [v3.89 撤单检查] 原写法 ensure_future 发出去不看结果；改为锁外统一走
                        #   _cancel_exchange_order 并确认终态（这里仍在锁内，只登记待撤计划）
                        _evict_cancel.append((old_sym, int(lowest.limit_order_id), lowest))
                    # ── 清理 pos_state 残留（否则 get_pending_count 会虚高）──
                    try:
                        from shared.state import get_all_positions as _gap, remove_position as _rmp
                        _old_recs = [p for p in _gap()
                                     if p.get("symbol") == old_sym and p.get("status") == "pending"]
                        if _old_recs:
                            # [v3.89 remove-pos 检查] 检查返回值，失败只记日志
                            try:
                                if _rmp(_old_recs[0]["pos_id"]) is False:
                                    logger.warning("[signal_watcher] remove_position(%s) 返回 False，pos_state 未写入成功，请人工核查" % _old_sym)
                            except Exception as _re:
                                logger.error("[signal_watcher] remove_position(%s) 抛异常，持仓文件可能损坏，请人工核查: %s" % (_old_sym, _re))
                            logger.info(f"[signal_watcher] {old_sym} pos_state pending 记录已清理")
                    except Exception as _rpe:
                        logger.warning(f"[signal_watcher] {old_sym} pos_state 清理失败: {_rpe}")
                    if self._ws_listener:
                        self._ws_listener.unsubscribe(old_sym)

            self._watched[symbol] = entry
            expire_in = max(0, expire_ts - time.time())
            logger.info(
                f"[signal_watcher] {symbol} 注册监控 mode={entry.mode} "
                f"触发区间=[{entry_low:.6g},{entry_high:.6g}] "
                f"挂单价={entry_price:.6g} 有效期={expire_in/60:.1f}min"
            )
            # ── 创建信号卡片（进入监控节点）─────────────────────────────────
            try:
                from shared.notifier import buffer_signal, card_append, make_card_header, fmt_node_monitor
                _sig       = entry.signal
                _dir       = _sig.get("direction", "LONG").upper()
                _mode      = entry.mode
                _strength  = _sig.get("signal_strength", "medium")
                _score_val = _sig.get("ind", {}).get("final_score") or _sig.get("score") or pending_score
                _header    = make_card_header(symbol, _dir, _mode, _strength)
                # sig_id = symbol + 扫描时间戳（避免同标的多次信号冲突）
                _scan_ts   = str(_sig.get("_scan_ts", int(time.time())))
                _sig_id    = f"{symbol}_{_scan_ts}"
                _sig["_sig_id"] = _sig_id   # 写回 signal，供后续节点使用
                _expire_min   = int(max(0, expire_ts - time.time()) / 60)
                _sl_deg       = bool(_sig.get("sl_degraded") or (_sig.get("params") or {}).get("sl_degraded"))
                _monitor_line = fmt_node_monitor(_score_val, entry_price, _expire_min, _sl_deg)
                # [fix] 只进缓冲区（buffer_signal），不 flush：等 _trigger 里价格触发后再 flush 合并发出
                #   未触发就过期/被淘汰 → discard_signal 静默丢弃，不会产生幽灵卡片
                buffer_signal(_sig_id, _header)
                card_append(_sig_id, _monitor_line)
            except Exception as _ne:
                logger.debug(f"[signal_watcher] 信号卡片创建失败（非致命）: {_ne}")

        # [v3.89 撤单检查] 锁外撤被驱逐条目的交易所挂单：确认终态，结果未知登记对账
        for _esy, _eoid, _eentry in _evict_cancel:
            try:
                _oc = await self._cancel_exchange_order(_esy, _eoid)
            except Exception as _ce:
                _oc = "unknown"
                logger.warning(f"[signal_watcher] 撤低分挂单 {_esy} 异常: {_ce}")
            if _oc == "confirmed":
                logger.info(f"[signal_watcher] 低分挂单 {_esy} id={_eoid} 已撤销")
            elif _oc == "filled":
                logger.error(f"[signal_watcher] 低分挂单 {_esy} id={_eoid} 已（部分）成交，请核对持仓")
                self._append_reconcile_log({"ts": time.time(), "event": "filled_on_evict",
                                            "symbol": _esy, "order_id": _eoid})
            else:
                self._register_orphan(_esy, _eoid, "竞争淘汰", _eentry, on_confirmed="remove")

    async def cancel_all(self, reason: str = "manual") -> int:
        """
        撤销所有未触发信号的入场挂单（仓位满时调用）。
        返回撤销数量。
        """
        cancelled = 0
        async with self._lock:
            to_cancel = {
                sym: e for sym, e in self._watched.items()
                if not e.triggered and not e.is_expired()
            }

        for sym, entry in to_cancel.items():
            if entry.triggered:  # 二次确认：快照后如果已被 _trigger 激活，跳过，防止误撑进场挂单
                continue
            try:
                # [v3.89 撤单检查] 统一走 _cancel_and_remove：撤单确认终态，结果未知登记 _orphan_orders。
                #   单个条目失败只记录，不影响其他条目的撤单。
                _st = await self._cancel_and_remove(sym, entry, reason)
                if self._watched.get(sym) is entry:
                    # filled / cancel_unknown：条目保留待对账，不计入已撤销
                    logger.warning(f"[signal_watcher] {sym} 撤单未确认（{_st}），条目保留待对账")
                else:
                    cancelled += 1
            except Exception as e:
                logger.warning(f"[signal_watcher] 撤销 {sym} 失败: {e}")

        if cancelled:
            logger.info(f"[signal_watcher] 共撤销 {cancelled} 个入场挂单 ({reason})")
            # cancel_all 的通知由调用方（btc_guard / 仓位满）负责携带上下文推送
            # 此处只向各被撤信号的卡片追加结束行
            # [v3.90 通知聚合] 被撤销信号不再补写卡片行，改为静默丢弃缓冲区
            try:
                from shared.notifier import discard_signal
                for _sym, _e in to_cancel.items():
                    _sid = _e.signal.get("_sig_id", "")
                    if _sid:
                        discard_signal(_sid)
            except Exception as _sw_e:
                logger.debug(f"[signal_watcher] 批量撤单卡片丢弃失败: {_sw_e}")
        return cancelled

    def pending_count(self) -> int:
        return sum(1 for e in self._watched.values() if not e.triggered and not e.is_expired())

    def pending_symbols(self) -> list[str]:
        return [s for s, e in self._watched.items() if not e.triggered and not e.is_expired()]

    # ─────────────────────────────────────────────────────────────────────────
    # 价格回调（主 loop 内，ws_listener 直接调用）
    # ─────────────────────────────────────────────────────────────────────────

    # [v3.89 #101] 价格事件新鲜度：事件年龄（接收时间 - 事件时间）超过上限、事件时间超前本地时钟超过容差，
    #   或事件时间早于该标的已处理过的最后事件（乱序/重连后的旧帧）都不得触发信号或更新 BTC 冲击基准。
    _EVT_MAX_AGE_MS = 10_000
    _EVT_FUTURE_TOL_MS = 15_000   # [fix-3] Binance 服务器时间可能比本机快约 13s

    @staticmethod
    def _normalize_event_ts(ts) -> Optional[int]:
        """事件时间 → 毫秒；缺失/非法/<=0 返回 None（按本地接收时间处理，不做新鲜度与乱序校验）。
        小于 1e11 的值按「秒」处理（兼容仍传秒的调用方）。"""
        try:
            v = float(ts)
        except (TypeError, ValueError):
            return None
        if v != v or v in (float("inf"), float("-inf")) or v <= 0:
            return None
        return int(v * 1000) if v < 1e11 else int(v)

    def _event_acceptable(self, symbol: str, ts) -> bool:
        ev = self._normalize_event_ts(ts)
        if ev is None:
            return True
        now_ms = int(time.time() * 1000)
        age = now_ms - ev
        reason = None
        if age > self._EVT_MAX_AGE_MS:
            reason = "stale"
        elif age < -self._EVT_FUTURE_TOL_MS:
            reason = "future"
        elif ev < self._last_event_ts.get(symbol, 0):
            reason = "out_of_order"
        if reason:
            self._evt_drop[reason] = self._evt_drop.get(reason, 0) + 1
            _k = (symbol, reason)
            if time.time() - self._evt_drop_log_ts.get(_k, 0) >= 60:
                self._evt_drop_log_ts[_k] = time.time()
                logger.warning(f"[signal_watcher] 丢弃价格事件 {symbol} 原因={reason} 事件年龄={age}ms "
                               f"累计丢弃={dict(self._evt_drop)}")
            return False
        self._last_event_ts[symbol] = ev
        return True

    async def on_price_update(self, symbol: str, price: float, ts: int) -> None:
        """
        WS 实时价格回调。此方法在主 loop 内被直接 await 调用（由 ws_listener 的
        _dispatch_mark_price 触发），无需 ensure_future，直接 await 即可。
        """
        if not self._event_acceptable(symbol, ts):
            return
        if symbol == "BTCUSDT":
            await self._on_btc_price(price, ts)
            return
        await self._check_sl_breach(symbol, price)
        if not self._watched:
            return
        _e0 = self._watched.get(symbol)
        if _e0 is not None and not _e0.triggered and _e0.cancel_state == "none":
            _bad, _why = self._realtime_falsified(_e0, price)
            if _bad:
                logger.info(f"[signal_watcher] {symbol} 实时证伪撤单: {_why}")
                await self._cancel_and_remove(symbol, _e0, _why)
                return

        # ── v3.54 BTC 即时波动检测：BTC 1分钟内波动 >= 3% 立刻撤所有 pending 挂单 ──
        if False and symbol == "BTCUSDT":   # [v2-5] 旧 BTC 即时检测已由 btc_guard 接管
            _now = time.time()
            if self._btc_price_prev <= 0:
                # 第一次收到 BTC 价格，仅初始化基准，不触发撤单
                self._btc_price_prev = price
                self._btc_prev_ts    = _now
            else:
                # 超过快照间隔时更新基准
                if _now - self._btc_prev_ts >= self._BTC_SNAP_INTERVAL:
                    _move = abs(price - self._btc_price_prev) / (self._btc_price_prev + 1e-9)
                    if _move >= self._BTC_MOVE_THRESH:
                        # 防抖：10分钟内不重复触发
                        if _now - self._btc_cancel_cooldown > 600:
                            self._btc_cancel_cooldown = _now
                            _direction = "涨" if price > self._btc_price_prev else "跌"

                            # ── 第一层：入场挂单即时撤销 ──────────────────────────
                            _cnt = sum(
                                1 for e in self._watched.values()
                                if not e.triggered and not e.is_expired()
                            )
                            if _cnt > 0:
                                logger.warning(
                                    f"[signal_watcher] ⚡ BTC 5min{_direction} {_move*100:.2f}% "
                                    f"(${self._btc_price_prev:.0f}→${price:.0f})，"
                                    f"即时撤销 {_cnt} 个入场挂单"
                                )
                                await self.cancel_all(f"BTC_SHOCK_{_move*100:.1f}%")
                                # 全局推送：BTC 保护撤单
                                try:
                                    from shared.notifier import notify as _btc_ntf, _hhmm as _bhm
                                    _sym_list = ' · '.join(
                                        s for s, e in self._watched.items()
                                        if not e.triggered and not e.is_expired()
                                    ) or '无'
                                    _btc_ntf(
                                        f"⚡ BTC 5分钟急{_direction} {_move*100:.1f}%\n"
                                        f"已撤销全部 {_cnt} 个待触发监控\n"
                                        f"{_sym_list}"
                                    )
                                except Exception:
                                    pass

                            # ── 第二层：open 仓 SL/TP 重算（ATR已变，保护价可能失效）──
                            try:
                                from shared.state import get_all_positions as _gap_btc
                                import position_manager as _pm_mod
                                _open_pos = [
                                    p for p in _gap_btc()
                                    if p.get("status") == "open"
                                    and p.get("sig_type") not in ("funding_arb",)
                                ]
                                if _open_pos:
                                    logger.warning(
                                        f"[signal_watcher] ⚡ BTC剧烈波动，触发 {len(_open_pos)} 个"
                                        f"open仓 SL/TP 重算"
                                    )
                                    for _p in _open_pos:
                                        try:
                                            await _pm_mod._adjust_sl_tp_once(_p, force=True)
                                        except Exception as _ae:
                                            logger.warning(
                                                f"[signal_watcher] {_p.get('symbol')} "
                                                f"SL/TP重算失败: {_ae}"
                                            )
                            except Exception as _pe:
                                logger.warning(f"[signal_watcher] open仓重算失败: {_pe}")

                            # ── 第三层：TP 可达性 + SL 踩空紧急检查 ────────────────
                            # BTC 大跌后：多单 TP 可能已在 3ATR 外永远到不了；
                            #             SL 可能已经高于当前价（本应触发但漏报）
                            try:
                                from shared.state import get_all_positions as _gap2
                                _open2 = [p for p in _gap2() if p.get("status") == "open"
                                          and p.get("sig_type") not in ("funding_arb",)]
                                for _p2 in _open2:
                                    _sym2 = _p2.get("symbol","")
                                    _dir2 = _p2.get("direction","").upper()
                                    _sl2  = float(_p2.get("sl_price", 0))
                                    _tp12 = float(_p2.get("tp1_price", 0))
                                    try:
                                        # [v3.83 修复 BUG-3] 原代码直接用 `api`，但本模块没有顶层 import api，
                                        #   NameError 被下面的 except 吞掉并 continue，导致「TP 可达性 / SL 踩空」紧急检查从不执行。
                                        #   修复：就地导入 binance_api（与本文件其他位置的写法一致）。
                                        from shared import binance_api as api
                                        _md2  = api.get_mark_price(_sym2)
                                        _mp2  = float(_md2.get("markPrice", 0))
                                    except Exception:
                                        continue
                                    if _mp2 <= 0 or _sl2 <= 0:
                                        continue
                                    # SL 踩空检测：止损价已经越过当前价（应触发但未收到回报）
                                    _sl_triggered = (
                                        (_dir2 == "LONG"  and _mp2 <= _sl2) or
                                        (_dir2 == "SHORT" and _mp2 >= _sl2)
                                    )
                                    if _sl_triggered:
                                        logger.error(
                                            f"[signal_watcher] ⚠️ {_sym2} SL踩空！"
                                            f"现价={_mp2:.4g} SL={_sl2:.4g} dir={_dir2}，"
                                            f"触发紧急平仓检查（通知用户）"
                                        )
                                        # 止损踩空告警：独立推送（不属于任何信号卡片）
                                        try:
                                            from shared.notifier import notify as _ntf
                                            import asyncio as _asio3
                                            await _asio3.get_event_loop().run_in_executor(
                                                None, lambda: _ntf(
                                                    f"⚠️ {_sym2} 止损踩空\n"
                                                    f"现价 {_mp2:.4g} 已越过止损 {_sl2:.4g}\n"
                                                    f"BTC剧烈波动引发，请检查仓位"
                                                )
                                            )
                                        except Exception:
                                            pass
                            except Exception as _p3e:
                                logger.warning(f"[signal_watcher] TP/SL有效性检查失败: {_p3e}")

                    # 不管是否触发，每 _BTC_SNAP_INTERVAL 秒都更新基准
                    self._btc_price_prev = price
                    self._btc_prev_ts    = _now
            return  # BTC 自身不是挂单监控标的，直接返回

        entry = self._watched.get(symbol)
        if entry is None or entry.triggered:
            return


        if entry.is_expired():
            await self._handle_expired(symbol, entry)
            return

        if entry.price_in_range(price):
            # 直接 await，在主 loop 内是安全的
            await self._trigger(symbol, price, entry)

    # ─────────────────────────────────────────────────────────────────────────
    # 触发：下限价单 + 交给 executor 监听成交
    # ─────────────────────────────────────────────────────────────────────────

    async def _on_btc_price(self, price: float, ts) -> None:
        """[v2-5] BTC 价格 → btc_guard；状态要求撤某方向时撤对应 pending 挂单。"""
        try:
            from shared import btc_guard as _bg
            _bg.update_price(price, ts)
            d = _bg.get_guard().cancel_direction()
            st = _bg.get_state()
        except Exception as e:
            logger.debug(f"[signal_watcher] btc_guard 更新失败: {e}")
            return
        if not d:
            return
        async with self._lock:
            victims = [(s_, e) for s_, e in list(self._watched.items())
                       if not e.triggered and e.cancel_state == "none"
                       and e.signal.get("direction", "LONG").upper() == d]
        if victims:
            logger.warning(f"[signal_watcher] btc_guard={st}，撤 {len(victims)} 个 {d} 挂单")
        for s_, e in victims:
            await self._cancel_and_remove(s_, e, f"btc_guard {st}")

    @staticmethod
    def _realtime_falsified(entry: 'WatchEntry', price: float) -> tuple:
        """[v2-2] 价格类硬性证伪实时检查（不等 recheck）。"""
        mode = entry.mode
        d = entry.signal.get("direction", "LONG").upper()
        atr = entry.atr_at_trigger or float((entry.signal.get("ind") or {}).get("atr_1h", 0) or 0)
        if atr <= 0:
            return False, ""
        if mode == "pullback":
            ema20 = entry.ema20_at_trigger
            if ema20 > 0:
                if d == "LONG" and price < ema20 - 1.0 * atr:
                    return True, f"pullback LONG 实时跌破 EMA20-1.0ATR={ema20 - atr:.6g}"
                if d == "SHORT" and price > ema20 + 1.0 * atr:
                    return True, f"pullback SHORT 实时涨破 EMA20+1.0ATR={ema20 + atr:.6g}"
        elif mode in ("reversal", "oi_divergence") and entry.trigger_price > 0:
            tp = entry.trigger_price
            if d == "LONG" and price < tp - 0.6 * atr:
                return True, f"{mode} LONG 实时跌破 触发价-0.6ATR={tp - 0.6 * atr:.6g}"
            if d == "SHORT" and price > tp + 0.6 * atr:
                return True, f"{mode} SHORT 实时涨破 触发价+0.6ATR={tp + 0.6 * atr:.6g}"
        return False, ""

    async def _check_sl_breach(self, symbol: str, price: float) -> None:
        """[v2-8] open 持仓止损踩空：只检测 + 告警，不平仓（平仓交给 position_manager）。持仓每 30s 刷新。"""
        now = time.time()
        if now - self._open_pos_ts >= 30:
            self._open_pos_ts = now
            try:
                from shared.state import get_all_positions as _gap
                self._open_pos_cache = [p_ for p_ in _gap() if p_.get("status") == "open"]
            except Exception:
                pass
        for p_ in self._open_pos_cache:
            if p_.get("symbol") != symbol:
                continue
            sl = float(p_.get("sl_price") or 0)
            d = str(p_.get("direction", "")).upper()
            if sl <= 0 or not ((d == "LONG" and price <= sl) or (d == "SHORT" and price >= sl)):
                continue
            if now - self._sl_breach_alerted.get(symbol, 0) < 300:
                continue
            self._sl_breach_alerted[symbol] = now
            logger.error(f"[SL-BREACH] {symbol} 止损价已被越过，请检查仓位 (price={price} sl={sl} dir={d})")
            try:
                from shared.notifier import notify as _ntf
                await asyncio.get_running_loop().run_in_executor(
                    None, lambda: _ntf(f"[SL-BREACH] {symbol} 止损价已被越过，请检查仓位\n现价 {price} 止损 {sl} {d}"))
            except Exception:
                pass

    async def _trigger(self, symbol: str, price: float, entry: WatchEntry) -> None:
        """加锁确认 → 下限价单 → 交给 executor 的 WS 监听链路。"""
        async with self._lock:
            # 二次确认（防止并发重复触发）
            e = self._watched.get(symbol)
            if e is None or e.triggered or e.is_expired():
                return
            if e is not entry:   # [v3.89 #41] 队列里已是别的条目（被重挂/淘汰替换），旧引用不得触发
                logger.info(f"[signal_watcher] {symbol} 触发跳过：条目已被替换 entry_id={entry.entry_id}")
                return
            if e.cancel_state != "none":   # [v3.89 #40] 撤单进行中/结果未知，不得触发
                return
            if not e.price_in_range(price):
                return
            e.triggered = True  # 先标记，防止后续重入

        elapsed = time.time() - entry.added_ts
        logger.info(
            f"[signal_watcher] ✅ {symbol} 触发！ mode={entry.mode} "
            f"当前价={price:.6g} 挂单价={entry.entry_price:.6g} "
            f"等待了 {elapsed:.1f}s"
        )

        # ── v3.33/v3.42 5分钟内同方向同策略触发上限 ─────────────────────────────
        # 防止 BTC 下跌时多个相关信号集中触发 → 集中止损
        # v3.42 修复问题37：冷却 key 改为 direction+mode，各策略冷却独立，
        # 避免 range_bounce LONG 封锁 pullback LONG（两种策略逻辑完全不同）
        _direction = entry.signal.get("direction", "").upper()
        _mode_key  = entry.mode   # pullback / reversal / breakout / range_bounce 等
        _dir_key   = f"{_direction}_{_mode_key}" if _direction else ""
        _now_ts = time.time()
        _last_dir_ts = self._dir_last_trigger.get(_dir_key, 0)
        if _dir_key and (_now_ts - _last_dir_ts) < self._DIR_TRIGGER_COOLDOWN:
            _wait_s = self._DIR_TRIGGER_COOLDOWN - (_now_ts - _last_dir_ts)
            logger.warning(
                f"[signal_watcher] {symbol} {_dir_key} 已在 5min 内触发过同方向同策略，"
                f"跳过本次触发（还需等 {_wait_s:.0f}s）"
            )
            # 触发已标记，直接返回不挂单
            async with self._lock:
                # [v3.89 #102-partial] pop 前核对身份：队列里若已是别的条目（被重挂/淘汰替换）不得误删
                if self._watched.get(symbol) is entry:
                    self._watched.pop(symbol, None)
            return
        if _dir_key:
            self._dir_last_trigger[_dir_key] = _now_ts

        try:
            from executor import PreOrderError as _PreOrderError   # [v3.89 #102]
        except Exception:
            class _PreOrderError(Exception):   # 兜底，避免因导入失败把可重试错误当未知处理
                pass

        _keep_entry = False   # [v3.89 #102] True 表示本次触发异常，条目保留（可重试或待对账）

        try:
            # ── 卡片追加：价格触发挂单行 ─────────────────────────────────────
            try:
                from shared.notifier import card_append, flush_signal, fmt_node_trigger
                _sig_id_t = entry.signal.get("_sig_id", "")
                if _sig_id_t:
                    # [fix-2] 卡片追加/flush 放到后台线程，2s 超时静默丢弃，不阻塞触发链路
                    _line_t = fmt_node_trigger(price, entry.entry_price)

                    def _card_job(_sid=_sig_id_t, _ln=_line_t):
                        card_append(_sid, _ln)
                        flush_signal(_sid)

                    async def _card_bg():
                        try:
                            await asyncio.wait_for(asyncio.to_thread(_card_job), timeout=2.0)
                        except Exception:
                            pass
                    asyncio.get_running_loop().create_task(_card_bg())
            except Exception as _tc_e:
                logger.debug(f"[signal_watcher] 触发卡片追加失败: {_tc_e}")

            # ── 触发前滑点合理性检查（v3.24）──────────────────────────────
            # 防止 WebSocket 延迟导致触发价与当前价偏离过大（高波动行情常见）
            _skip = False
            try:
                _mp = _get_hub_or_rest(symbol, "mark")   # [v2-1] 每次取最新 markPrice
                _mark_now = float(_mp.get("markPrice", 0))
                # 用 WebSocket 实际触发时的 price（函数参数），而非预算入场价
                _trigger_p = price   # _trigger(symbol, price, entry) 里的 price
                _atr = float(entry.signal.get("ind", {}).get("atr_1h", 0)) or _mark_now * 0.03
                # [v2-1] 价格一次跳过区间超过 0.3ATR → 放弃入场
                _dir_t = entry.signal.get("direction", "LONG").upper()
                if _mark_now > 0:
                    _gap_t = (entry.entry_low - _mark_now) if _dir_t == "LONG" else (_mark_now - entry.entry_high)
                    if _gap_t > 0.3 * _atr:
                        logger.info(f"[trigger-skip] {symbol} {_dir_t} 价格跳过区间 {_gap_t / (_atr + 1e-9):.2f}ATR "
                                    f"(mark={_mark_now:.6g} 区间=[{entry.entry_low:.6g},{entry.entry_high:.6g}])，放弃入场")
                        _skip = True
                if not _skip and _mark_now > 0 and _trigger_p > 0:
                    _deviation = abs(_mark_now - _trigger_p) / (_atr + 1e-9)
                    if _deviation > 0.5:
                        logger.warning(
                            f"[signal_watcher] {symbol} 滑点过大放弃入场: "
                            f"触发价={_trigger_p:.6g} 当前mark={_mark_now:.6g} "
                            f"偏离={_deviation:.2f} ATR > 0.5，跳过本次"
                        )
                        _skip = True
            except Exception as _se:
                logger.debug(f"[signal_watcher] {symbol} 滑点检查失败，降级直接执行: {_se}")

            # ── 触发前强平意图判断（v3.49）——开关默认关闭（v3.50）─────────────
            # 原理：
            #   - breakout LONG  刺穿前高 → 期待大量 SHORT 被强平（空头爆仓推价格）
            #     若 ws_listener 缓存里 SHORT 强平量 < 阈值，说明突破无人爆仓，可能是假突破
            #   - breakout SHORT 刺穿前低 → 期待大量 LONG 被强平
            #   - pullback / reversal 模式不做强平过滤
            # 开关：BREAKOUT_LIQ_FILTER_ENABLED（config.py 可覆盖）
            #   默认 False：等实盘数据足够后再开启
            #   True：启用强平量过滤，ATR×价格×20 为动态阈值
            if not _skip and entry.mode == "breakout" and _BREAKOUT_LIQ_ENABLED:
                _direction_liq = entry.signal.get("direction", "").upper()
                # breakout LONG 刺穿前高 → 应有 SHORT 被强平；SHORT 刺穿前低 → 应有 LONG 被强平
                _liq_side = "SHORT" if _direction_liq == "LONG" else "LONG"
                try:
                    _ws = self._ws_listener
                    if _ws is not None and hasattr(_ws, "get_liquidation_volume"):
                        _atr_v = float(entry.signal.get("ind", {}).get("atr_1h", 0)) or price * 0.02
                        _liq_threshold = _atr_v * price * 20  # 动态阈值（约 40~60k USDT 量级）
                        _liq_vol = await asyncio.wait_for(   # [fix-2] 2s 超时 → 走原降级逻辑
                            _ws.get_liquidation_volume(symbol, _liq_side, minutes=15), timeout=2.0)
                        if _liq_vol < _liq_threshold:
                            logger.warning(
                                f"[signal_watcher] {symbol} breakout {_direction_liq} "
                                f"强平量不足，放弃入场: "
                                f"{_liq_side} 强平={_liq_vol/1000:.1f}k < 阈值={_liq_threshold/1000:.1f}k USDT"
                            )
                            _skip = True
                        else:
                            logger.info(
                                f"[signal_watcher] {symbol} breakout {_direction_liq} "
                                f"强平量充足，确认入场: "
                                f"{_liq_side} 强平={_liq_vol/1000:.1f}k >= 阈值={_liq_threshold/1000:.1f}k USDT"
                            )
                except Exception as _le:
                    logger.debug(f"[signal_watcher] {symbol} 强平意图查询失败，降级直接入场: {_le}")

            if not _skip:
                # [v2-3] 下单与注册共用 symbol 锁
                _tl = self._sym_locks.setdefault(symbol, asyncio.Lock())
                async with _tl:
                    await entry.on_trigger(entry.signal)
        except _PreOrderError as _pe:
            # [v3.89 #102] 下单前失败：交易所确认没有订单，复位触发标记保留条目，等下一次价格触发重试
            _keep_entry = True
            async with self._lock:
                if self._watched.get(symbol) is entry:
                    entry.triggered = False
                    entry.trigger_state = "none"
            logger.warning(f"[signal_watcher] {symbol} 下单前失败，已保留信号等待下次触发重试: {_pe}")
        except Exception as e_:
            # [v3.89 #102] 其他异常可能发生在已下单之后（结果未知）：标记待对账，不自动清理
            _keep_entry = True
            logger.error(f"[signal_watcher] {symbol} on_trigger 异常（触发结果未知）: {e_}", exc_info=True)
            async with self._lock:
                if self._watched.get(symbol) is entry:
                    entry.trigger_state = "unknown"
            _oid = entry.limit_order_id
            try:
                self._append_trigger_reconcile({
                    "ts": time.time(), "symbol": symbol, "entry_id": entry.entry_id,
                    "error": repr(e_), "order_id_if_known": _oid,
                    "mode": entry.mode, "direction": entry.signal.get("direction", ""),
                    "entry_price": entry.entry_price, "reason": "on_trigger 异常，触发结果未知",
                })
            except Exception:
                pass
            try:
                from shared.notifier import notify as _notify
                import threading as _thr
                _thr.Thread(target=_notify, daemon=True, args=(
                    f"🚨 {symbol} 触发后执行异常，无法确认是否已下单（order_id={_oid}），"
                    f"已登记对账并保留条目，请人工核查！",)).start()
            except Exception:
                pass
        finally:
            async with self._lock:
                # [v3.89 #102-partial] pop 前核对身份，避免删掉同品种被替换进来的新条目
                if (not _keep_entry) and self._watched.get(symbol) is entry:
                    self._watched.pop(symbol, None)

    async def _handle_expired(self, symbol: str, entry: WatchEntry) -> None:
        """处理过期信号：撤单 + 移除。"""
        waited_min = (time.time() - entry.added_ts) / 60
        logger.info(
            f"[signal_watcher] {symbol} 信号过期，撤单并移除 "
            f"(mode={entry.mode} 等待了 {waited_min:.1f}min)"
        )

        # [v3.89 撤单检查] 不再重复实现撤单逻辑，统一复用 _cancel_and_remove
        #   （撤单确认终态；结果未知时条目保留并登记 _orphan_orders，由 _reconcile_orphans 继续跟踪）
        await self._cancel_and_remove(symbol, entry, "信号过期")

        # [v3.90 通知聚合] 过期信号不再补写超时卡片行，改为静默丢弃缓冲区
        try:
            from shared.notifier import discard_signal
            _sig_id = entry.signal.get("_sig_id", "")
            if _sig_id:
                discard_signal(_sig_id)
        except Exception as _sw_e:
            logger.debug(f"[signal_watcher] 超时卡片丢弃失败: {_sw_e}")

    async def _cancel_order(self, symbol: str, order_id: int) -> None:
        """异步撤单（在主 loop 里通过 run_in_executor 调同步 API）。"""
        loop = asyncio.get_running_loop()
        try:
            from shared import binance_api as _api
            # [v3.89 #40] 返回交易所响应：binance_api.cancel_order 失败时返回 {} 而不抛异常，
            #   调用方必须看返回的 status，不能把「没抛异常」当成撤单成功。
            return await loop.run_in_executor(None, lambda: _api.cancel_order(symbol, order_id))
        except Exception as e:
            raise RuntimeError(f"cancel_order({symbol},{order_id}): {e}")

    async def _query_order(self, symbol: str, order_id: int) -> dict:
        """[v3.89 #40] 查询订单状态（失败返回 {}，由 _classify_order 归为「未能确认」）。
        [fix #46] get_order 失败改为抛异常，此处捕获并保持原有 {} 语义。"""
        loop = asyncio.get_running_loop()
        from shared import binance_api as _api
        try:
            return await loop.run_in_executor(None, lambda: _api.get_order(symbol, order_id))
        except Exception as e:
            logger.warning(f"[sw] 查询订单失败 {symbol} {order_id}: {e}")
            return {}

    @staticmethod
    def _classify_order(d: Any) -> Optional[str]:
        """[v3.89 #40] 订单响应归类：'filled'（有成交，含部分成交）/ 'confirmed'（已撤销，无成交）/ None（未能确认）。"""
        if not isinstance(d, dict) or not d:
            return None
        st = str(d.get("status", "")).upper()
        try:
            executed = float(d.get("executedQty") or 0)
        except (TypeError, ValueError):
            executed = 0.0
        if st == "FILLED" or executed > 0:
            return "filled"
        if st in ("CANCELED", "CANCELLED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED"):
            return "confirmed"
        return None

    async def _cancel_exchange_order(self, symbol: str, order_id: int) -> str:
        """
        [v3.89 #40] 撤交易所订单并确认终态。返回：
          'confirmed' 已撤销（无成交）；'filled' 订单已（部分）成交，不是撤销；'unknown' 无法确认。
        先看撤单响应的 status；响应不明确（含失败返回 {}）再用 get_order 查终态。
        """
        res = None
        try:
            res = await self._cancel_order(symbol, order_id)
        except Exception as e:
            logger.warning(f"[signal_watcher] 撤单异常 {symbol} id={order_id}: {e}")
        c = self._classify_order(res)
        if c:
            return c
        try:
            q = await self._query_order(symbol, order_id)
        except Exception as e:
            q = {}
            logger.warning(f"[signal_watcher] 撤单后查询订单失败 {symbol} id={order_id}: {e}")
        return self._classify_order(q) or "unknown"

    @staticmethod
    def _append_reconcile_log(rec: dict) -> None:
        """[v3.89 #40] 追加写 logs/cancel_reconcile.jsonl（与 pos_state.json 隔离）。"""
        try:
            p = Path(__file__).parent.parent / "logs" / "cancel_reconcile.jsonl"
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"[signal_watcher] 写对账记录失败: {e}")

    @staticmethod
    def _append_trigger_reconcile(rec: dict) -> None:
        """[v3.89 #102] 追加写 logs/trigger_reconcile.jsonl（触发结果未知的对账记录）。"""
        try:
            p = Path(__file__).parent.parent / "logs" / "trigger_reconcile.jsonl"
            p.parent.mkdir(parents=True, exist_ok=True)
            with open(p, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        except Exception as e:
            logger.warning(f"[signal_watcher] 写 trigger_reconcile 失败: {e}")

    def _register_orphan(self, symbol: str, order_id: int, reason: str,
                         entry: WatchEntry, on_confirmed: str) -> None:
        """
        [v3.89 #40] 登记「撤单结果未知」订单，独立于监控条目跟踪到终态。
        on_confirmed: 'remove'（对账确认已撤后移除条目）/ 'keep_watching'（清空 limit_order_id，继续等待触发）
        """
        key = (symbol, int(order_id))
        if key in self._orphan_orders:
            return
        self._orphan_orders[key] = {"symbol": symbol, "order_id": int(order_id), "reason": reason,
                                    "attempts": 0, "first_ts": time.time(),
                                    "entry": entry, "on_confirmed": on_confirmed}
        logger.error(f"[signal_watcher] 撤单结果未知，登记对账 {symbol} id={order_id} 原因={reason}")
        try:
            asyncio.get_running_loop().create_task(self._guard_unknown_fill(symbol, entry))
        except RuntimeError:
            pass
        self._append_reconcile_log({"ts": time.time(), "event": "registered", "symbol": symbol,
                                    "order_id": int(order_id), "reason": reason,
                                    "entry_id": entry.entry_id})

    async def _guard_unknown_fill(self, symbol: str, entry: 'WatchEntry') -> bool:
        """[v2-7] 撤单结果未知：立即 REST 查持仓；有持仓就用 pre_params.sl_price + 持仓数量补挂止损。"""
        from shared import binance_api as _api
        loop = asyncio.get_running_loop()
        try:
            pos = await loop.run_in_executor(None, lambda: _api.get_position(symbol)) or []
        except Exception as e:
            logger.warning(f"[CANCEL-UNKNOWN] {symbol} 查持仓失败: {e}")
            return False
        d = entry.signal.get("direction", "LONG").upper()
        for p_ in pos:
            amt = float(p_.get("positionAmt", 0) or 0)
            ps = str(p_.get("positionSide", "BOTH")).upper()
            if amt == 0 or ps not in ("BOTH", d):
                continue
            sl = float((entry.signal.get("pre_params") or {}).get("sl_price") or 0)
            if sl <= 0:
                logger.error(f"[CANCEL-UNKNOWN] {symbol} 可能已成交但缺少 pre_params.sl_price，无法补止损，请人工处理")
                return False
            side = "SELL" if d == "LONG" else "BUY"
            pside = ps if ps != "BOTH" else d
            try:
                # [fix #27] 与 executor 相同的取整逻辑；成功判断改为检查返回的 algoId 是否有效
                from executor import _get_precision, _round_price, _round_qty
                _prec = _get_precision(symbol)
                sl_r  = _round_price(sl, _prec["tick"])
                qty_r = _round_qty(abs(amt), _prec["step"])
                if sl_r <= 0 or qty_r <= 0:
                    logger.error(f"[CANCEL-UNKNOWN] {symbol} 取整后止损价/数量无效 sl={sl_r} qty={qty_r}，请人工处理")
                    return False
                _resp = await loop.run_in_executor(None, lambda: _api.place_sl(symbol, side, pside, sl_r, qty_r))
                _aid = (_resp or {}).get("algoId") or (_resp or {}).get("orderId")
                try:
                    _aid_ok = int(_aid) > 0
                except (TypeError, ValueError):
                    _aid_ok = False
                if not _aid_ok:
                    logger.error(f"[CANCEL-UNKNOWN] {symbol} 补止损未得到有效 algoId，resp={_resp}，请人工处理")
                    return False
                logger.warning(f"[CANCEL-UNKNOWN] {symbol} 可能已成交，已补止损 algoId={_aid} (sl={sl_r} qty={qty_r})")
                return True
            except Exception as e:
                logger.error(f"[CANCEL-UNKNOWN] {symbol} 补止损失败: {e}")
                return False
        return False

    async def _reconcile_orphans(self) -> None:
        """[v3.89 #40] 对账撤单结果未知的订单；每个最多重试 5 次，超限 logger.error 并移除。"""
        if not self._orphan_orders:
            return
        for key, rec in list(self._orphan_orders.items()):
            sym, oid, entry = rec["symbol"], rec["order_id"], rec["entry"]
            try:
                outcome = self._classify_order(await self._query_order(sym, oid))
                if outcome is None:   # 仍在挂单或查询失败 → 再尝试撤一次
                    outcome = await self._cancel_exchange_order(sym, oid)
                    if outcome == "unknown":
                        outcome = None
            except Exception as e:
                outcome = None
                logger.warning(f"[signal_watcher] 对账异常 {sym} id={oid}: {e}")
            rec["attempts"] += 1
            if outcome is None:
                if rec["attempts"] >= 5:
                    logger.error(f"[signal_watcher] 对账 {sym} id={oid} 重试 {rec['attempts']} 次仍无法确认，"
                                 f"放弃自动对账并移出对账表，请人工核查")
                    self._append_reconcile_log({"ts": time.time(), "event": "gave_up", "symbol": sym,
                                                "order_id": oid, "attempts": rec["attempts"]})
                    self._orphan_orders.pop(key, None)
                continue
            async with self._lock:
                if outcome == "confirmed":
                    if self._watched.get(sym) is entry:
                        if rec["on_confirmed"] == "remove":
                            self._watched.pop(sym, None)
                        else:
                            entry.limit_order_id = None
                            entry.cancel_state = "none"
                else:   # filled
                    if self._watched.get(sym) is entry:
                        entry.cancel_state = "none"
                    logger.error(f"[signal_watcher] 对账发现 {sym} id={oid} 已（部分）成交，请核对持仓")
            self._append_reconcile_log({"ts": time.time(), "event": outcome, "symbol": sym,
                                        "order_id": oid, "attempts": rec["attempts"]})
            self._orphan_orders.pop(key, None)
            if outcome == "confirmed" and rec["on_confirmed"] == "remove" and self._ws_listener:
                self._ws_listener.unsubscribe(sym)

    # ─────────────────────────────────────────────────────────────────────────
    # REST 轮询模式（替代 WS，在 WS 不可用时使用）
    # ─────────────────────────────────────────────────────────────────────────

    async def start_polling(self, interval_sec: float = 1.5) -> None:
        """
        启动 REST 轮询，定期拉取所有待触发标的的 markPrice 并触发条件检查。
        用于 WS markPrice stream 不可用的环境（代理不透传 WS 帧）。
        interval_sec: 轮询间隔，默认 1.5 秒（山寨币 1 分钟 3% 波动，1.5s 足够）
        """
        if self._polling_task and not self._polling_task.done():
            return
        self._polling_task = asyncio.create_task(
            self._poll_loop(interval_sec), name="signal_watcher_poll"
        )
        logger.info(f"[signal_watcher] REST 轮询已启动（间隔 {interval_sec}s）")

    async def stop_polling(self) -> None:
        if hasattr(self, '_polling_task') and self._polling_task and not self._polling_task.done():
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass

    async def _poll_loop(self, interval_sec: float) -> None:
        """轮询主循环：一次拉取全量 markPrice，按 pending_symbols 过滤，批量触发 on_price_update。
        v3.6 优化：原串行逐标的 HTTP（10 标的约 2~5s）改为单次 get_all_mark_prices()（1 次请求覆盖全量），
        消除串行延迟累积，breakout 信号不再因轮询超时错过入场区间。
        """
        try:
            from shared import binance_api as _api
        except ImportError:
            import sys
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from shared import binance_api as _api  # type: ignore

        loop = asyncio.get_running_loop()

        while True:
            try:
                await asyncio.sleep(interval_sec)
                syms = self.pending_symbols()
                if not syms:
                    continue

                sym_set = set(syms)

                # 一次请求拉全量标记价格，再按待触发标的过滤（避免串行 N 次 HTTP）
                try:
                    all_mp = await loop.run_in_executor(None, _api.get_all_mark_prices)
                except Exception as _bulk_e:
                    logger.debug(f"[signal_watcher] get_all_mark_prices 失败: {_bulk_e}")
                    all_mp = []
                # [v3.89 #101] 事件时间取「拉取完成时」，而不是请求发出前：慢请求不应让刚拿到的价格被判为过期
                ts = int(time.time() * 1000)

                if all_mp:
                    mp_map = {
                        item["symbol"]: float(item.get("markPrice", 0))
                        for item in all_mp
                        if item.get("symbol") in sym_set
                    }
                    for sym in syms:
                        price = mp_map.get(sym, 0)
                        if price > 0:
                            try:
                                await self.on_price_update(sym, price, ts)
                            except Exception as _e:
                                logger.debug(f"[signal_watcher] on_price_update {sym} 失败: {_e}")
                else:
                    # 全量接口失败时降级：逐标的串行拉取（保底路径）
                    for sym in syms:
                        try:
                            mp    = await loop.run_in_executor(None, lambda s=sym: _get_hub_or_rest(s, 'mark'))
                            price = float(mp.get("markPrice", 0))
                            ts    = int(time.time() * 1000)   # [v3.89 #101] 每个标的拉取完成后重新取事件时间
                            if price > 0:
                                await self.on_price_update(sym, price, ts)
                        except Exception as _e:
                            logger.debug(f"[signal_watcher] 轮询 {sym} 失败（降级）: {_e}")

            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[signal_watcher] 轮询异常: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # GC（后台清理过期 + 已触发条目）
    # ─────────────────────────────────────────────────────────────────────────

    async def start_gc(self, interval_sec: float = _GC_INTERVAL) -> None:
        if self._gc_task and not self._gc_task.done():
            return
        self._gc_task = asyncio.create_task(
            self._gc_loop(interval_sec), name="signal_watcher_gc"
        )
        logger.info("[signal_watcher] GC 任务已启动")

    async def stop_gc(self) -> None:
        if self._gc_task and not self._gc_task.done():
            self._gc_task.cancel()
            try:
                await self._gc_task
            except asyncio.CancelledError:
                pass

    async def _gc_loop(self, interval_sec: float) -> None:
        while True:
            try:
                await asyncio.sleep(interval_sec)
                await self._gc_pass()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[signal_watcher] GC 异常: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # ① pending 挂单健康检查（指标失效撤单 + 清算区位移调价）
    # ─────────────────────────────────────────────────────────────────────────

    async def start_recheck(self, interval_sec: float = 60.0) -> None:
        """每 interval_sec 秒对所有 pending 挂单做一次指标复核。默认 15min。"""
        if hasattr(self, '_recheck_task') and self._recheck_task and not self._recheck_task.done():
            return
        self._recheck_task = asyncio.create_task(
            self._recheck_loop(interval_sec), name="signal_watcher_recheck"
        )
        logger.info("[signal_watcher] pending-recheck 任务已启动 (间隔=%.0fs)" % interval_sec)

    async def stop_recheck(self) -> None:
        t = getattr(self, '_recheck_task', None)
        if t and not t.done():
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

    async def _recheck_loop(self, interval_sec: float) -> None:
        await asyncio.sleep(interval_sec)   # 首次等一个周期再开始
        while True:
            try:
                await self._recheck_pass()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[signal_watcher] recheck 异常: {e}", exc_info=True)
            await asyncio.sleep(interval_sec)

    async def _recheck_pass(self) -> None:
        """
        对每个 pending 挂单检查：
        A. 指标失效 → 撤单移除
           - reversal SHORT: RSI_1h 已降回 < 65（超买消退）
           - reversal LONG:  RSI_1h 已回升 > 35（超卖消退）
           - pullback LONG:  1H EMA20 < EMA50（趋势翻转）
           - pullback SHORT: 1H EMA20 > EMA50（趋势翻转）
           - 任意:           BTC 4H EMA20 < EMA50 时对 LONG 挂单撤单（大盘走弱）
        B. 清算区位移 > 0.5 ATR → 撤旧单 → 重算 → 下新限价单
        """
        loop = asyncio.get_running_loop()

        # 快照当前 pending 列表（不持锁做 IO）
        async with self._lock:
            # [v3.89 #41] 快照只含正常条目：撤单进行中/结果未知的条目交给撤单与对账流程，recheck 不再碰
            entries = [(sym, e) for sym, e in self._watched.items()
                       if not e.triggered and not e.is_expired() and e.cancel_state == "none"]

        if not entries:
            return

        # 获取 BTC 4H 趋势（一次性，供所有 entries 用）
        btc_4h_bull = True
        try:
            from shared import binance_api as _api
            from shared.indicators import ema as _ema
            _kbtc = await loop.run_in_executor(None, lambda: _get_hub_or_rest('BTCUSDT', 'klines', '4h', 55))
            _c4h  = [float(k[4]) for k in _kbtc]
            _e20b = _ema(_c4h, 20)
            _e50b = _ema(_c4h, 50)
            btc_4h_bull = _e20b > _e50b
        except Exception as _be:
            logger.debug(f"[recheck] BTC 4H 获取失败（跳过 BTC 过滤）: {_be}")

        for sym, entry in entries:
            try:
                await self._recheck_one(sym, entry, btc_4h_bull, loop)
            except Exception as _e:
                logger.warning(f"[recheck] {sym} 单个检查异常（跳过）: {_e}")


    @staticmethod
    def _survival_check(entry: 'WatchEntry', price: float, rsi_1h: float, atr: float,
                        realtime_ema20: float = 0.0, close_1h: float = 0.0,
                        post_high: float = 0.0, post_low: float = 0.0,
                        oi_now: float = 0.0, ema20_4h: float = 0.0, ema50_4h: float = 0.0,
                        atr_4h: float = 0.0, close_4h: float = 0.0) -> tuple[bool, str]:
        """v3.67 挂单存活检查——只检查「核心理由是否被证伪」，不重新跑完整 judge。

        开仓条件和存活条件分离：
        - 开仓条件：严格多维度 judge，确保入场时机正确
        - 存活条件：只看当初决定入场的核心理由是否还在
        - 其他指标（vol_ratio、15M形态、OI、RSI小幅变化）在存活判断中全部忽略

        返回 (should_cancel, reason)
        """
        mode = entry.mode
        direction = entry.signal.get('direction', 'LONG').upper()
        trigger_price = entry.trigger_price
        atr_trigger = entry.atr_at_trigger if entry.atr_at_trigger > 0 else atr

        if mode == 'pullback':
            # 核心理由：价格在 EMA20 支撑/压力区间 + RSI 未进入极端区
            # v3.78 fix: 优先使用实时 EMA20（调用方传入），快照值只做降级兜底
            # 背景：触发挂单后价格持续移动，触发时快照 EMA20 会严重偏离实际压力/支撑位
            ema20 = realtime_ema20 if realtime_ema20 > 0 else entry.ema20_at_trigger
            if ema20 <= 0:
                # 没有 EMA20 快照，降级为不撤（保守）
                return False, ""
            if direction == 'LONG':
                # 价格跌破支撑（EMA20 下方 1.5 ATR）→ 撤单
                # [v2-9] 1.5→1.0 ATR；用 1H 收盘价判断（防插针）；RSI<28 需与跌破同时满足
                _cp = close_1h if close_1h > 0 else price
                support = ema20 - atr_trigger * 1.0
                if _cp < support:
                    if rsi_1h < 28:
                        return True, f"pullback LONG RSI1H={rsi_1h:.1f}<28 且收盘跌破 EMA20-1.0ATR={support:.6g}"
                    return True, f"pullback LONG 1H收盘跌破支撑位 EMA20-1.0ATR={support:.6g} (EMA20={ema20:.6g})"
            else:
                # 价格涨破压力（EMA20 上方 1.5 ATR）→ 撤单
                _cp = close_1h if close_1h > 0 else price
                resistance = ema20 + atr_trigger * 1.0
                if _cp > resistance:
                    return True, f"pullback SHORT 1H收盘涨破压力位 EMA20+1.0ATR={resistance:.6g} (EMA20={ema20:.6g})"
                # RSI 进入超买（> 72）说明已不是回踩而是新一轮拉升，做空时机失效
                if rsi_1h > 72:
                    return True, f"pullback SHORT RSI1H={rsi_1h:.1f}>72，已超买，回踩逻辑失效"

        elif mode == 'breakout':
            # 核心理由：价格仍在突破位上方/下方
            ref = entry.breakout_ref
            if ref <= 0:
                # 没有突破基准价快照，降级为不撤
                return False, ""
            if direction == 'LONG':
                # 价格跌破突破位（允许 0.3 ATR 缓冲防灯芯）→ 假突破确认 → 撤单
                if price < ref - atr_trigger * 0.3:
                    return True, f"breakout LONG 价格跌破突破位 {ref:.6g}，假突破确认"
            else:
                # 价格涨破突破位 → 撤单
                if price > ref + atr_trigger * 0.3:
                    return True, f"breakout SHORT 价格涨破突破位 {ref:.6g}"

        elif mode == 'reversal':
            # 核心理由：价格未走反 + 超买/超卖未消退
            if trigger_price <= 0:
                return False, ""
            _rsi_sc, _rsi_lc = _SURVIVAL_RSI_SC, _SURVIVAL_RSI_LC
            if direction == 'SHORT':
                # 价格反向下跌超过 1 ATR → reversal SHORT 信号证伪
                # [v2-9] 按新规则：涨破触发价+0.6ATR 或创触发后新高
                reversal_threshold = trigger_price + atr_trigger * 0.6
                if price > reversal_threshold:
                    return True, f"reversal SHORT 价格涨破 触发价+0.6ATR={reversal_threshold:.6g}"
                if post_high > 0 and price > post_high:
                    return True, f"reversal SHORT 创触发后新高 {price:.6g} > {post_high:.6g}"
                # RSI 回落至门槛以下 → 超买消退
                if rsi_1h < _rsi_sc:
                    return True, f"reversal SHORT RSI1H={rsi_1h:.1f} 回落至 {_rsi_sc} 以下，超买消退"
            else:
                # 价格反向上涨超过 1 ATR → reversal LONG 信号证伪
                reversal_threshold = trigger_price - atr_trigger * 0.6
                if price < reversal_threshold:
                    return True, f"reversal LONG 价格跌破 触发价-0.6ATR={reversal_threshold:.6g}"
                if post_low > 0 and price < post_low:
                    return True, f"reversal LONG 创触发后新低 {price:.6g} < {post_low:.6g}"
                # RSI 回升至门槛以上 → 超卖消退（45 而非 40，避免正常反弹初期误撤）
                if rsi_1h > _rsi_lc:
                    return True, f"reversal LONG RSI1H={rsi_1h:.1f} 回升至 {_rsi_lc} 以上，超卖消退"

        elif mode == 'range_bounce':
            # 核心理由：区间结构未被破坏（保留原有逻辑，已足够精简）
            _reason_dict = entry.signal.get('reason', {}) or {}
            if direction == 'LONG':
                c_low = float(_reason_dict.get('c_low', 0))
                if c_low > 0 and price < c_low * 0.995:
                    return True, f"range_bounce 区间下沿跌破 (price={price:.5f} < c_low={c_low:.5f} × 0.995)"
            elif direction == 'SHORT':
                c_high = float(_reason_dict.get('c_high', 0))
                if c_high > 0 and price > c_high * 1.005:
                    return True, f"range_bounce SHORT 区间上沿突破 (price={price:.5f} > c_high={c_high:.5f} × 1.005)"

        elif mode == 'stop_hunt':
            # 核心理由：二探未过深（保留原有逻辑）
            _jd_rc = entry.signal.get('judge_detail') or {}
            _sweep_rc = float(_jd_rc.get('sweep_extreme') or _jd_rc.get('pierce_extreme') or 0)
            if _sweep_rc > 0:
                if direction == 'LONG' and price < _sweep_rc * 1.01:
                    return True, f"sweep_retest_too_deep: LONG 二探过深 (price={price:.4g} < sweep={_sweep_rc:.4g}×1.01)"
                elif direction == 'SHORT' and price > _sweep_rc * 0.99:
                    return True, f"sweep_retest_too_deep: SHORT 二探过深 (price={price:.4g} > sweep={_sweep_rc:.4g}×0.99)"

        elif mode == 'oi_divergence':
            # 核心理由：OI 背离信号——价格接近低点/高点但 OI 异常，等待价格继续走背离方向
            # 撤单条件：价格已经走反了超过 1 ATR（背离窗口关闭）
            # oi_divergence 入场是 pending_order 模式（挂在回落位），
            # 若价格反向超过 1 ATR，说明背离结构已失效
            # [v2-9] 1.0→0.6 ATR；另加 OI 恢复到触发前水平（signal.ind.oi_1h_ago，±1%）→ 撤单
            if direction == 'LONG' and price < trigger_price - atr_trigger * 0.6:
                return True, f"oi_divergence LONG 价格走反超 0.6ATR: {price:.6g} < 触发价-0.6ATR={trigger_price - atr_trigger * 0.6:.6g}"
            elif direction == 'SHORT' and price > trigger_price + atr_trigger * 0.6:
                return True, f"oi_divergence SHORT 价格走反超 0.6ATR: {price:.6g} > 触发价+0.6ATR={trigger_price + atr_trigger * 0.6:.6g}"
            _oi_pre = float((entry.signal.get("ind") or {}).get("oi_1h_ago") or 0)
            if oi_now > 0 and _oi_pre > 0 and abs(oi_now - _oi_pre) / _oi_pre <= 0.01:
                return True, f"oi_divergence OI 已恢复到触发前水平 (oi={oi_now:.0f} 触发前={_oi_pre:.0f})"

        elif mode == 'ema_trend':
            # 核心理由：EMA 趋势跟随——价格在 EMA20/EMA50 多头排列中回踩，等待反弹
            # 撤单条件：价格跌破/涨破 EMA20（趋势结构失效）
            # ema_trend 是 immediate 模式（基本立刻成交），此处仅作保底
            # [v3.87 STEP5-6] 与 pullback 对齐：优先使用实时 EMA20（调用方传入），触发时快照只做兜底。
            #   原先只用触发快照，价格移动后快照会偏离真实的 EMA20 支撑/压力位。
            # [v2-10] 改用 4H 数据：4H 收盘价 vs 4H EMA20 ± 0.5×ATR_4h；EMA20/EMA50 差距 <0.3% 也撤
            if ema20_4h > 0 and atr_4h > 0 and close_4h > 0:
                if direction == 'LONG' and close_4h < ema20_4h - 0.5 * atr_4h:
                    return True, f"ema_trend LONG 4H收盘跌破 EMA20_4h-0.5ATR_4h={ema20_4h - 0.5 * atr_4h:.6g}"
                if direction == 'SHORT' and close_4h > ema20_4h + 0.5 * atr_4h:
                    return True, f"ema_trend SHORT 4H收盘涨破 EMA20_4h+0.5ATR_4h={ema20_4h + 0.5 * atr_4h:.6g}"
                if ema50_4h > 0 and abs(ema20_4h - ema50_4h) / ema50_4h < 0.003:
                    return True, f"ema_trend 4H EMA20/EMA50 差距收窄至 {abs(ema20_4h - ema50_4h) / ema50_4h * 100:.2f}%<0.3%"

        # 以上条件都不满足 → 挂单存活
        return False, ""

    async def _recheck_one(
        self, sym: str, entry: WatchEntry,
        btc_4h_bull: bool, loop: asyncio.AbstractEventLoop
    ) -> None:
        from shared import binance_api as _api
        from shared.indicators import atr as _atr, ema as _ema, rsi as _rsi
        from shared.liq_cache import get_liq_zones_cached
        from shared.calc_sl_tp import _nearest_liq_above, _zone_size

        # [v3.89 #41] 快照可能已过期：进入慢速 IO 前先在锁内复核条目仍是队列里的同一个对象且状态正常
        async with self._lock:
            if self._watched.get(sym) is not entry or entry.triggered or entry.cancel_state != "none":
                logger.info(f'[recheck] {sym} 条目已变化（触发/替换/撤单中），跳过本次检查 entry_id={entry.entry_id}')
                return

        direction = entry.signal.get('direction', 'LONG').upper()
        mode      = entry.mode  # pullback / reversal / breakout

        k1h = await loop.run_in_executor(None, lambda: _get_hub_or_rest(sym, "klines", "1h", 50))
        if not k1h:
            return
        c1h = [float(k[4]) for k in k1h]   # 下游 RSI/新闻检查仍使用
        # [v2-2] 只用已收盘 K 线；有新收盘才重算指标，否则复用缓存
        _ck = self._ind_cache.get(sym) or {}
        _last_ot = k1h[-2][0] if len(k1h) >= 2 else k1h[-1][0]
        if _ck.get("ot_1h") != _last_ot:
            _c = [float(k[4]) for k in k1h[:-1]]
            _ck = {"ot_1h": _last_ot, "atr": _atr(k1h[:-1]), "ema20": _ema(_c, 20), "rsi": _rsi(_c),
                   "close_1h": _c[-1] if _c else 0.0}
            self._ind_cache[sym] = _ck
        atr_v, ema20, rsi_v, close_1h = _ck["atr"], _ck["ema20"], _ck["rsi"], _ck["close_1h"]
        _mpd = await loop.run_in_executor(None, lambda: _get_hub_or_rest(sym, "mark"))
        mark_mp = float((_mpd or {}).get('markPrice', 0) or 0)
        if mark_mp <= 0:
            return
        # 触发后新高/新低（reversal 用）：注册后的已收盘 1H 高低点
        _added_ms = entry.added_ts * 1000
        _post = [k for k in k1h[:-1] if float(k[0]) >= _added_ms]
        post_high = max((float(k[2]) for k in _post), default=0.0)
        post_low = min((float(k[3]) for k in _post), default=0.0)
        # [v2-10] ema_trend 单独拉 4H
        e20_4h = e50_4h = a4h = c4h_close = 0.0
        if mode == "ema_trend":
            k4h = await loop.run_in_executor(None, lambda: _get_hub_or_rest(sym, "klines", "4h", 60))
            if k4h and len(k4h) >= 52:
                _c4 = [float(k[4]) for k in k4h[:-1]]
                e20_4h, e50_4h, a4h, c4h_close = _ema(_c4, 20), _ema(_c4, 50), _atr(k4h[:-1]), _c4[-1]
        _oi_now_v = 0.0
        if mode == "oi_divergence":
            try:
                _oi_now_v = float((await loop.run_in_executor(None, lambda: _api.get_open_interest(sym)) or {})
                                  .get("openInterest", 0) or 0)
            except Exception:
                _oi_now_v = 0.0

        cancel_reason: str | None = None

        # ── A. v3.67 挂单存活检查（_survival_check）────────────────────────────
        # 开仓条件和存活条件分离：只检查「核心理由是否被证伪」，
        # 不重新跑完整 judge，避免 vol_ratio/15M形态等瞬时指标波动导致误撤。
        should_cancel, survival_reason = self._survival_check(
            entry, mark_mp, rsi_v, atr_v, realtime_ema20=ema20, close_1h=close_1h,
            post_high=post_high, post_low=post_low, oi_now=_oi_now_v,
            ema20_4h=e20_4h, ema50_4h=e50_4h, atr_4h=a4h, close_4h=c4h_close)
        if should_cancel:
            cancel_reason = survival_reason
        else:
            # ── v3.72 低分撤单（时效耗尽保护）──────────────────────────────
            # 核心理由仍在，但时效性分已严重衰减 → 主动释放槽位
            try:
                from config import PENDING_SCORE_ABANDON as _abandon_score
            except ImportError:
                _abandon_score = 30
            _new_score = _calc_pending_score(entry)
            entry.pending_score = _new_score  # 同步更新 WatchEntry
            if _new_score < _abandon_score and not entry.is_reorder:
                cancel_reason = f"时效耗尽（分数{_new_score:.0f}<{_abandon_score}），主动释放槽位"
                logger.info(f"[recheck] {sym} abandoned_low_score: {cancel_reason}")

        # BTC 大盘结构：BTC 4H 空头时，非 SHORT 挂单全撤
        # 豁免（v3.7）：独立行情信号保留——满足以下任一则豁免撤单：
        #   ① 该币 24H 涨跌幅绝对値 > 20%（独立行情）
        #   ② 该币资金费率 > 0.03%（市场强烈定价多方）
        if cancel_reason is None and not btc_4h_bull and direction == 'LONG':
            # 尝试拉取该币实时 chg24 和 funding 判断是否豁免
            exempt = False
            exempt_reason = ''
            try:
                _t24 = await loop.run_in_executor(
                    None, lambda: _api.get_ticker_24h(sym)
                )
                _t24 = _t24[0] if isinstance(_t24, list) else _t24
                _chg24 = abs(float(_t24.get('priceChangePercent', 0)))
                _mp2   = await loop.run_in_executor(
                    None, lambda: _get_hub_or_rest(sym, 'mark')
                )
                _funding = abs(float(_mp2.get('lastFundingRate', 0)))
                if _chg24 > 20:
                    exempt = True
                    exempt_reason = f'24H涨跌 {_chg24:.1f}%>20% 独立行情，豁免 BTC空头撤单'
                elif _funding > 0.0003:
                    exempt = True
                    exempt_reason = f'资金费率 {_funding*100:.3f}%>0.03%，豁免 BTC空头撤单'
            except Exception as _fe:
                logger.debug(f'[recheck] {sym} 豁免数据获取失败: {_fe}')

            # 新闻豁免 / 否决（v3.8 + caution v3.10）
            if not exempt and _SW_NEWS_OK:
                try:
                    _nb = _sw_get_news_bias(sym.replace('USDT', ''))
                    if _nb == 'bullish':
                        exempt = True
                        exempt_reason = f'利好新闻豁免 BTC空头撤单 (news_bias=bullish)'
                    elif _nb == 'caution':
                        # 攻击+已快速响应：不撤单，保留挂单
                        exempt = True
                        exempt_reason = 'caution新闻（攻击+已快速响应），不撤单'
                        logger.info(f'[recheck] {sym} LONG挂单 caution，保留')
                    elif _nb == 'bearish':
                        # 超跌解锁检查（v3.9）——已超卖且新闻 > 48h，允许保留挂单
                        try:
                            from news_signal import get_news_bias_detail as _ns_detail
                            import time as _time
                            _nd = _ns_detail(sym.replace('USDT', ''))
                            _news_age_h = _nd.get('news_age_h', 0.0)
                        except Exception:
                            _news_age_h = 0

                        _ticker_24h_val = {}
                        try:
                            _ticker_24h_val = await loop.run_in_executor(
                                None, lambda: _api.get_ticker_24h(sym)
                            )
                            _ticker_24h_val = _ticker_24h_val[0] if isinstance(_ticker_24h_val, list) else _ticker_24h_val
                        except Exception:
                            pass

                        _rsi_now = _rsi(c1h[-15:]) if len(c1h) >= 15 else 50
                        _chg24_now = float(_ticker_24h_val.get('priceChangePercent', 0))
                        _is_deep_oversold = _rsi_now < 25 and _chg24_now > 0 and _news_age_h > 48

                        if _is_deep_oversold:
                            logger.warning(
                                f'[recheck] {sym} bearish新闻超跌解锁，保留 LONG挂单: '
                                f'RSI={_rsi_now:.1f}<25, 24H={_chg24_now:+.1f}%, 新闻已{_news_age_h:.0f}h前'
                            )
                            exempt = True
                            exempt_reason = f'bearish新闻超跌解锁 RSI={_rsi_now:.1f}<25'
                        else:
                            cancel_reason = f'利空新闻一票否决 LONG挂单 (bearish news veto)'
                            logger.warning(f'[recheck] {sym} LONG挂单被利空新闻一票否决')
                except Exception as _ne:
                    logger.debug(f'[recheck] {sym} 新闻信号查询失败: {_ne}')

            if cancel_reason is None:  # 利空新闻一票否决未触发
                if exempt:
                    logger.info(f'[recheck] {sym} LONG挂单豁免 BTC空头撤单: {exempt_reason}')
                else:
                    cancel_reason = 'BTC 4H 均线空头，撤 LONG 挂单'

        if cancel_reason:
            logger.info(f'[recheck] {sym} 撤单: {cancel_reason}')
            await self._cancel_and_remove(sym, entry, cancel_reason)
            return

        # ── B1. 价格位移重挂（v3.72：reversal/breakout 入场价偏离 > 1 ATR 时重算）──
        # 问题：5分钟扫描间隔内价格可能已走 > 1 ATR，原挂单入场价失效，RR 变差
        # 解法：价格相对原入场价偏离 > SIGNAL_PRICE_SHIFT_REORDER_ATR 时撤旧挂新
        try:
            from config import SIGNAL_PRICE_SHIFT_REORDER_ATR, SIGNAL_PRICE_SHIFT_REORDER_TYPES
            _shift_types = tuple(SIGNAL_PRICE_SHIFT_REORDER_TYPES) if SIGNAL_PRICE_SHIFT_REORDER_TYPES else ()
            if mode in _shift_types and atr_v > 0:
                old_ep  = entry.entry_price
                _price_shift_atr = abs(mark_mp - old_ep) / atr_v
                if _price_shift_atr > SIGNAL_PRICE_SHIFT_REORDER_ATR:
                    logger.info(
                        f'[recheck] {sym} 价格位移 {_price_shift_atr:.2f}ATR > {SIGNAL_PRICE_SHIFT_REORDER_ATR}，'
                        f'重算入场参数并重挂 (mark={mark_mp:.6g} old_ep={old_ep:.6g})'
                    )
                    # 重新计算入场参数
                    try:
                        from shared.calc_sl_tp import calc_sl_tp as _cst_sw
                        from shared.liq_cache import get_liq_zones_cached as _lgc
                        _liq_long  = await loop.run_in_executor(None, lambda: _lgc(sym, 'LONG',  mark_mp, range_='24h'))
                        _liq_short = await loop.run_in_executor(None, lambda: _lgc(sym, 'SHORT', mark_mp, range_='24h'))
                        _new_sig   = dict(entry.signal)
                        _new_params = _cst_sw(
                            signal=_new_sig, mark_price=mark_mp, atr=atr_v,
                            liq_zones={'long': _liq_long, 'short': _liq_short},
                            k1h=k1h, ema20=ema20,
                        )
                        if 'error' not in _new_params:
                            _new_sig['pre_params']      = _new_params
                            _new_sig['pre_entry_price'] = _new_params['entry_price']
                            from shared.signal_watcher import compute_entry_range as _cer
                            _el, _eh, _ep = _cer(_new_sig, mark_mp, liq_long=_liq_long, liq_short=_liq_short)
                            # 按 mode 使用对应有效期
                            try:
                                from config import ORDER_EXPIRE_BREAKOUT_MIN as _exp_bo, ORDER_EXPIRE_PULLBACK_MIN as _exp_pb
                            except Exception:
                                _exp_bo, _exp_pb = 30, 240
                            if mode == "breakout":
                                _exp_min = _exp_bo          # 30 分钟
                            elif mode == "reversal":
                                _exp_min = int(_exp_pb * 1.5)  # 360 分钟
                            else:
                                _exp_min = _exp_pb           # 240 分钟
                            # [v3.87 STEP5-8] 参数与清算区位移重挂对齐：补 on_trigger 和触发快照字段，去掉不存在的 loop；
                            #   先注册新单、成功后才撤旧单（见 _reorder_replace）。
                            await self._reorder_replace(
                                sym, entry, _new_sig,
                                entry_low=_el, entry_high=_eh, entry_price=_ep,
                                expire_ts=time.time() + _exp_min * 60,
                                on_trigger=entry.on_trigger,
                                trigger_price=mark_mp,
                                trigger_rsi_1h=rsi_v,
                                breakout_ref=entry.breakout_ref,
                                ema20_at_trigger=ema20,
                                atr_at_trigger=atr_v,
                                is_reorder=True,
                                reorder_count=entry.reorder_count + 1,
                                score_created_at=entry.score_created_at,
                                score_original_expire=entry.score_original_expire,
                            )
                            return  # 重挂完成，本次 recheck 结束
                    except Exception as _pe_sw:
                        logger.warning(f'[recheck] {sym} 价格位移重挂失败，保留原挂单: {_pe_sw}')  # [v3.87 STEP5-8] debug→warning，失败不再被淹没
        except Exception as _pse:
            logger.debug(f'[recheck] {sym} 价格位移检查异常: {_pse}')

        # ── B. 清算区位移检查（仅 reversal SHORT，清算区锚点定价的场景）────────
        if mode == 'reversal' and direction == 'SHORT':
            liq_short = await loop.run_in_executor(
                None, lambda: get_liq_zones_cached(sym, 'SHORT', mark_mp, range_='24h')
            )
            z = _nearest_liq_above(mark_mp, atr_v, liq_short, dist_min=0.3, dist_max=5.0)
            if z:
                new_entry_p = (z['price_low'] + z['price_high']) / 2
                # ── v3.68 改动一：重挂单 dist 保护 ──────────────────────────────
                # 新入场价可能再次超过 1.5 ATR（清算区本身就很远），
                # 与 calc_sl_tp._entry_reversal 的保护逻辑保持一致
                # [v3.87 STEP5-7] 删除未使用的 _entry_clamped 标记
                _original_entry = new_entry_p
                if abs(new_entry_p - mark_mp) > atr_v * 1.5:
                    new_entry_p = mark_mp + atr_v * 0.5
                    logger.warning(
                        f'[recheck] {sym} 清算区位移重挂 dist 保护: '
                        f'原清算区中点 {_original_entry:.6g} 距 mark {mark_mp:.6g} '
                        f'= {abs(_original_entry - mark_mp)/(atr_v+1e-9):.1f}×ATR > 1.5，'
                        f'降级为 mark+0.5ATR={new_entry_p:.6g}'
                    )
                old_entry_p = entry.entry_price
                shift_atr   = abs(new_entry_p - old_entry_p) / (atr_v + 1e-9)
                if shift_atr >= 0.5:
                    # recheck 改价重挂：不单独推送，属于内部监控机制（静默）
                    logger.info(f'[recheck] {sym} 清算区位移 {shift_atr:.2f}ATR，撤旧单下新单')
                    # 撤旧单
                    await self._cancel_and_remove(sym, entry, '清算区位移重新挂单')

                    # 重新注册信号（用新入场价）
                    new_sig = dict(entry.signal)
                    new_sig['pre_entry_price'] = new_entry_p
                    # 计算新 sl/tp
                    try:
                        from shared.calc_sl_tp import calc_sl_tp as _cst
                        liq_long = await loop.run_in_executor(
                            None, lambda: get_liq_zones_cached(sym, 'LONG', mark_mp, range_='24h')
                        )
                        new_params = _cst(
                            signal=new_sig, mark_price=mark_mp, atr=atr_v,
                            liq_zones={'long': liq_long, 'short': liq_short},
                            k1h=k1h, ema20=ema20,
                        )
                        if 'error' not in new_params:
                            new_sig['pre_params'] = new_params
                            new_sig['pre_entry_price'] = new_params['entry_price']
                    except Exception as _pe:
                        logger.debug(f'[recheck] {sym} 重算 sl_tp 失败: {_pe}')

                    liq_long_r = await loop.run_in_executor(
                        None, lambda: get_liq_zones_cached(sym, 'LONG', mark_mp, range_='24h')
                    )
                    e_low, e_high, e_p = compute_entry_range(
                        new_sig, mark_mp, liq_long=liq_long_r, liq_short=liq_short
                    )
                    expire_sec = _PULLBACK_EXP_MIN * 60
                    expire_ts  = time.time() + expire_sec
                    # 已在主 loop 协程里，直接 await（run_coroutine_threadsafe 仅用于子线程）
                    await self.add_signal(
                        new_sig,
                        entry_low=e_low, entry_high=e_high,
                        entry_price=e_p, expire_ts=expire_ts,
                        on_trigger=entry.on_trigger,
                        # v3.67 触发快照字段（用清算区位移时的实时值）
                        trigger_price=mark_mp,
                        trigger_rsi_1h=rsi_v,
                        breakout_ref=entry.breakout_ref,
                        ema20_at_trigger=ema20,
                        atr_at_trigger=atr_v,
                        # v3.72 重挂标记：保留原始时效，不参与竞争
                        is_reorder=True,
                        reorder_count=entry.reorder_count + 1,
                        score_created_at=entry.score_created_at,
                        score_original_expire=entry.score_original_expire,
                    )
                    logger.info(f'[recheck] {sym} 新挂单已注册 entry={e_p:.5f}')

    async def _reorder_replace(self, sym: str, old_entry: WatchEntry, new_sig: dict, **add_kwargs) -> None:
        """
        价格位移重挂。
        [v3.89 #42] 顺序改为「先撤旧单并确认，再注册新单」：
          原写法（v3.87）先注册新单再撤旧单，旧单撤失败只记 warning，交易所上会同时存在新旧两张挂单。
          现在：旧条目置 cancelling（留在队列，_trigger 不触发）→ 撤交易所订单并确认终态 →
            confirmed：清空旧条目 limit_order_id，再 add_signal(replace=True) 注册新单；
            filled / unknown：不注册新单，抛异常由调用方记 warning（unknown 登记对账，确认已撤后旧条目继续等待）。
          确认已撤后 add_signal 抛异常：旧条目留在队列、limit_order_id 已置空，记 error，不静默丢信号。
        """
        oid = old_entry.limit_order_id
        async with self._lock:
            if self._watched.get(sym) is not old_entry or old_entry.triggered or old_entry.cancel_state != "none":
                raise RuntimeError(f"{sym} 旧条目已变化（触发/替换/撤单中），放弃重挂 entry_id={old_entry.entry_id}")
            if oid:
                old_entry.cancel_state = "cancelling"
        if oid:
            outcome = await self._cancel_exchange_order(sym, oid)
            async with self._lock:
                if outcome == "confirmed":
                    old_entry.limit_order_id = None
                    old_entry.cancel_state = "none"
                elif outcome == "filled":
                    old_entry.cancel_state = "none"
                else:
                    old_entry.cancel_state = "cancel_unknown"
                    self._register_orphan(sym, oid, "价格位移重挂", old_entry, on_confirmed="keep_watching")
            if outcome != "confirmed":
                if outcome == "filled":
                    logger.error(f'[recheck] 价格位移重挂：旧单 {sym} id={oid} 已（部分）成交，不注册新单，请核对持仓')
                raise RuntimeError(f"{sym} 旧单 id={oid} 撤销未确认({outcome})，不注册新单")
        try:
            await self.add_signal(new_sig, replace=True, **add_kwargs)
        except Exception as e:
            logger.error(f'[recheck] 价格位移重挂：旧单已撤但新单注册失败 {sym}，旧条目保留继续监控（limit_order_id 已置空）: {e}')
            raise

    async def cancel_signal(self, symbol: str, reason: str = "") -> bool:
        """[fix #32] 公开接口：撤销某标的的监控挂单并移除条目（供 scanner 竞争替换使用）。
        复用 _cancel_and_remove 的撤单确认流程；撤单失败/结果未知/已成交时条目保留。
        返回 True 表示条目已确认移除（可安全占用该槽位），False 表示未移除。"""
        async with self._lock:
            entry = self._watched.get(symbol)
        if entry is None:
            return True
        await self._cancel_and_remove(symbol, entry, reason or "外部请求撤销")
        async with self._lock:
            return symbol not in self._watched

    async def _cancel_and_remove(self, sym: str, entry: WatchEntry, reason: str) -> None:
        """
        撤销 entry 的限价单并从监控队列移除。
        [v3.89 #40] 原写法先从队列删除再撤单，撤单失败时条目没了、交易所订单还活着且无人跟踪。
        现改为：条目留在队列并置 cancelling（_trigger 见此状态不触发）→ 撤单并确认终态 →
          confirmed：锁内复核后删除、取消订阅；
          filled：订单已（部分）成交，保留条目，只记录，不新增交易逻辑；
          unknown：保留条目（cancel_unknown），登记对账，由 _gc_pass 重试。
        """
        oid = entry.limit_order_id
        async with self._lock:
            if sym not in self._watched:
                return
            # [v3.89 #41] 身份与状态复核：队列里必须还是传入的这个条目，且未触发、未在撤单中；
            #   否则可能误删同品种的新条目，或对已成交/已触发的条目撤单。
            if self._watched.get(sym) is not entry or entry.triggered or entry.cancel_state != "none":
                logger.info(f'[signal_watcher] {sym} 撤单跳过：条目已变化 entry_id={entry.entry_id} '
                            f'triggered={entry.triggered} cancel_state={entry.cancel_state} 原因={reason}')
                return
            if not oid:
                self._watched.pop(sym, None)   # 没有交易所订单，无需确认
            else:
                entry.cancel_state = "cancelling"
        if not oid:
            if self._ws_listener:
                self._ws_listener.unsubscribe(sym)
            logger.info(f'[signal_watcher] {sym} 已移除（无挂单）: {reason}')
            return
        outcome = await self._cancel_exchange_order(sym, oid)
        async with self._lock:
            if outcome == "confirmed":
                if self._watched.get(sym) is entry:
                    self._watched.pop(sym, None)
                entry.cancel_state = "none"
            elif outcome == "filled":
                entry.cancel_state = "none"
            else:
                entry.cancel_state = "cancel_unknown"
                self._register_orphan(sym, oid, reason, entry, on_confirmed="remove")
        if outcome == "confirmed":
            if self._ws_listener:
                self._ws_listener.unsubscribe(sym)
            logger.info(f'[signal_watcher] {sym} 已撤单并移除: {reason}')
        elif outcome == "filled":
            logger.error(f'[signal_watcher] {sym} 撤单时发现订单 id={oid} 已（部分）成交，条目保留，请核对持仓: {reason}')
            self._append_reconcile_log({"ts": time.time(), "event": "filled_on_cancel", "symbol": sym,
                                        "order_id": oid, "reason": reason})
        else:
            logger.error(f'[signal_watcher] {sym} 撤单结果未知 id={oid}，条目保留待对账: {reason}')

    # ─────────────────────────────────────────────────────────────────────────
    # ② BTC 大跌护卫（独立 task，1H 跌幅触发）
    # ─────────────────────────────────────────────────────────────────────────

    async def start_btc_guard(self, check_interval_sec: float = 300.0) -> None:
        """每 5 分钟检查 BTC 1H 跌幅，触发时撤所有非 SHORT 挂单。"""
        if hasattr(self, '_btc_guard_task') and self._btc_guard_task and not self._btc_guard_task.done():
            return
        self._btc_guard_task = asyncio.create_task(
            self._btc_guard_loop(check_interval_sec), name="signal_watcher_btc_guard"
        )
        logger.info("[signal_watcher] BTC 大跌护卫已启动")

    async def stop_btc_guard(self) -> None:
        t = getattr(self, '_btc_guard_task', None)
        if t and not t.done():
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

    async def _btc_guard_loop(self, interval_sec: float) -> None:
        await asyncio.sleep(60)   # 启动 1 分钟后开始第一次检查
        while True:
            try:
                await self._btc_guard_check()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[signal_watcher] BTC 护卫异常: {e}")
            await asyncio.sleep(interval_sec)

    async def _btc_guard_check(self) -> None:
        """[v2-5] 改为读 btc_guard 统一状态（不再独立拉 K 线判断）。"""
        try:
            from shared import btc_guard as _bg
            _d = _bg.get_guard().cancel_direction()
            _st = _bg.get_state()
        except Exception:
            return
        if not _d:
            return
        async with self._lock:
            _vic = [(s_, e) for s_, e in list(self._watched.items())
                    if not e.triggered and e.cancel_state == "none"
                    and e.signal.get('direction', 'LONG').upper() == _d]
        for s_, e in _vic:
            await self._cancel_and_remove(s_, e, f'btc_guard {_st}')

    async def _btc_guard_check_legacy(self) -> None:
        """旧实现（BTC 1H 跌超 3% → 撤 LONG），保留不再调用。"""
        loop = asyncio.get_running_loop()
        try:
            from shared import binance_api as _api
            k1h_btc = await loop.run_in_executor(
                None, lambda: _api.get_klines('BTCUSDT', '1h', 3)
            )
            if len(k1h_btc) < 3:
                return
            # 用两根已闭合 K 线（-3/-2），避免未闭合 K 线实时价格触发误撤单
            prev_close = float(k1h_btc[-3][4])
            cur_close  = float(k1h_btc[-2][4])
            chg_pct    = (cur_close - prev_close) / (prev_close + 1e-9) * 100
        except Exception as _e:
            logger.debug(f"[btc_guard] 获取 BTC K 线失败: {_e}")
            return

        if chg_pct >= -3.0:
            return  # 没有大跌，不动

        # 触发：撤所有 LONG 方向 pending 挂单
        async with self._lock:
            victims = [
                (sym, e) for sym, e in list(self._watched.items())
                if not e.triggered
                and e.signal.get('direction', 'LONG').upper() == 'LONG'
            ]

        if not victims:
            return

        logger.warning(
            f"[btc_guard] BTC 1H 跌幅 {chg_pct:.2f}% 触发护卫，"
            f"撤销 {len(victims)} 个 LONG pending 挂单: {[s for s,_ in victims]}"
        )
        for sym, entry in victims:
            await self._cancel_and_remove(sym, entry, f'BTC 1H 跌 {chg_pct:.2f}%')

    # ─────────────────────────────────────────────────────────────────────────
    # GC（后台清理过期 + 已触发条目）
    # ─────────────────────────────────────────────────────────────────────────

    async def _gc_pass(self) -> None:
        """GC 一轮：清理已触发条目；过期条目调 _handle_expired 撤单。"""
        expired_entries = []
        async with self._lock:
            for sym, e in list(self._watched.items()):
                # [v3.89 #102/#44] 触发结果未知的条目保留待人工核查；撤单进行中/结果未知的条目由撤单与对账流程负责
                if e.trigger_state == "unknown" or e.cancel_state != "none":
                    continue
                if e.triggered:
                    self._watched.pop(sym, None)
                elif e.is_expired():
                    expired_entries.append((sym, e))

        for sym, e in expired_entries:
            await self._handle_expired(sym, e)

        # [v3.90 通知聚合] 每轮 GC 兜底 flush 超时未决的信号缓冲
        try:
            from shared.notifier import tick_signal_buffer
            tick_signal_buffer()
        except Exception as _tb_e:
            logger.debug(f"[signal_watcher] 信号缓冲兜底 flush 失败: {_tb_e}")

        if expired_entries:
            logger.info(f"[signal_watcher] GC 清理过期信号: {[s for s,_ in expired_entries]}")

        # [v3.89 #40] 对账撤单结果未知的订单
        try:
            await self._reconcile_orphans()
        except Exception as _re:
            logger.warning(f"[signal_watcher] 撤单对账异常: {_re}")
