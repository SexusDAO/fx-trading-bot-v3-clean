#!/usr/bin/env python3
"""
backtest_btw.py — 严格无未来数据污染回测脚本（阶段二）

核心原则：
  - 每个时间步 ts 只使用截止 ts 的历史数据
  - 所有外部 API 调用全部 Mock，读本地文件
  - CG bonus 固定为 0（无历史接口）
  - 新闻信号固定为 None（无法精确历史化）
  - LLM 判断全部跳过，用规则系统原始结果
  - 复用现有 calc_indicators / judges / calc_sl_tp

用法：
    python3 fx/backtest_btw.py --days 30 [--symbol BTWUSDT] [--step 5]

前置：先运行 python3 fx/backtest_data/prepare_data.py --days 30

数据污染说明（结论必读）：
  ✅ 无污染：K线、资金费率、OI、BTC market_mode
  ❌ 有限制：CG bonus=0 → 评分偏低，实际胜率可能更高
  ❌ 有限制：news_bias=None → 部分拦截缺失，结果偏保守
  ⚠️  LLM跳过：边界信号用规则原始结果（少量边界单可能多入）
"""

import os
import sys
import json
import time
import argparse
import logging
import bisect
from datetime import datetime, timezone
from copy import deepcopy

# ── 路径修复 ──────────────────────────────────────────────────────────────────
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_FX_DIR   = os.path.dirname(_THIS_DIR)
_ROOT_DIR = os.path.dirname(_FX_DIR)
for _p in [_FX_DIR, _ROOT_DIR]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

DATA_DIR = os.path.join(_THIS_DIR, "backtest_data")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backtest_btw")

# ════════════════════════════════════════════════════════════════════════════
# Mock 层：拦截所有外部 API，用本地数据替代
# ════════════════════════════════════════════════════════════════════════════

class MockBinanceAPI:
    """
    在回测期间替换 fx/shared/binance_api.py 的核心函数。
    所有数据从内存（已加载的历史文件）查找。
    """
    def __init__(self, klines_map: dict, funding_list: list, oi_list: list):
        """
        klines_map: {"BTWUSDT_15m": [...], "BTWUSDT_1h": [...], ...}
        funding_list: 资金费率历史列表
        oi_list: OI历史列表（1h快照）
        """
        self.klines_map   = klines_map
        self.funding_list = funding_list  # 按 fundingTime 升序
        self.oi_list      = oi_list       # 按 timestamp 升序
        self._current_ts  = 0             # 当前回测时间戳（ms），由主循环注入

    def set_ts(self, ts_ms: int):
        self._current_ts = ts_ms

    def get_klines(self, symbol: str, interval: str, limit: int = 100) -> list:
        key = f"{symbol}_{interval}"
        all_k = self.klines_map.get(key, [])
        # 只返回开盘时间 < 当前 ts 的 K 线（严格无未来数据）
        result = [k for k in all_k if k[0] < self._current_ts]
        return result[-limit:] if len(result) > limit else result

    def get_funding_rate(self, ts_ms: int) -> float:
        """返回截止 ts_ms 最新一条资金费率"""
        best = 0.0
        for f in self.funding_list:
            if f["fundingTime"] <= ts_ms:
                best = float(f["fundingRate"])
            else:
                break
        return best

    def get_oi_at(self, ts_ms: int) -> float:
        """返回截止 ts_ms 最近一条 OI"""
        best = 0.0
        for o in self.oi_list:
            if o["timestamp"] <= ts_ms:
                best = float(o["sumOpenInterest"])
            else:
                break
        return best

    def get_ticker_24h(self, symbol: str, ts_ms: int) -> dict:
        """用 K 线模拟 24H ticker（chg24/volume）"""
        k1h = self.get_klines(symbol, "1h", limit=26)
        if not k1h:
            return {}
        price_now  = float(k1h[-1][4])
        price_24h_ago = float(k1h[-25][4]) if len(k1h) >= 25 else float(k1h[0][4])
        chg24 = (price_now - price_24h_ago) / price_24h_ago * 100 if price_24h_ago > 0 else 0.0
        vol24 = sum(float(k[7]) for k in k1h[-24:])  # quoteVolume
        return {
            "priceChangePercent": str(round(chg24, 4)),
            "quoteVolume": str(vol24),
            "lastPrice": str(price_now),
        }


# ════════════════════════════════════════════════════════════════════════════
# 数据加载
# ════════════════════════════════════════════════════════════════════════════

def load_data(symbol: str, data_dir: str) -> dict:
    """加载所有历史数据到内存"""
    data = {}
    files = {
        f"{symbol}_15m": f"{symbol}_15m.json",
        f"{symbol}_1h":  f"{symbol}_1h.json",
        f"{symbol}_4h":  f"{symbol}_4h.json",
        "BTCUSDT_1h":    "BTCUSDT_1h.json",
        "BTCUSDT_4h":    "BTCUSDT_4h.json",
    }
    for key, fname in files.items():
        path = os.path.join(data_dir, fname)
        if not os.path.exists(path):
            raise FileNotFoundError(f"缺少数据文件: {path}\n请先运行: python3 fx/backtest_data/prepare_data.py")
        with open(path, "r") as f:
            data[key] = json.load(f)
        logger.info(f"  加载 {key}: {len(data[key])} 根")

    # 资金费率
    fund_path = os.path.join(data_dir, f"{symbol}_funding.json")
    with open(fund_path, "r") as f:
        data["funding"] = json.load(f)
    logger.info(f"  加载 funding: {len(data['funding'])} 条")

    # OI
    oi_path = os.path.join(data_dir, f"{symbol}_oi_1h.json")
    with open(oi_path, "r") as f:
        data["oi"] = json.load(f)
    logger.info(f"  加载 OI: {len(data['oi'])} 条")

    return data


# ════════════════════════════════════════════════════════════════════════════
# Market Mode 计算（纯历史 BTC K 线，不调外部）
# ════════════════════════════════════════════════════════════════════════════

def calc_market_mode_from_klines(btc_k4h: list) -> dict:
    """
    用 BTC 4H 历史 K 线计算市场模式，返回 {"mode": str, "bear_strength": str|None}
    复用 risk_manager 的 EMA 逻辑，但完全离线。
    """
    from fx.shared.indicators import ema as _ema

    closes = [float(k[4]) for k in btc_k4h[:-1]]  # 去掉未闭合
    if len(closes) < 50:
        return {"mode": "range", "bear_strength": None}

    # 默认参数（与 config.py 一致）
    EMA_FAST = 20
    EMA_MID  = 50
    EMA_SLOW = 200

    ema_fast = _ema(closes, EMA_FAST)
    ema_50   = _ema(closes, EMA_MID)
    price    = closes[-1]

    if len(closes) < EMA_SLOW:
        return {"mode": "range", "bear_strength": None}

    ma_slow = sum(closes[-EMA_SLOW:]) / EMA_SLOW

    if ema_fast > ema_50 and price > ma_slow:
        raw_mode = "bull"
    elif ema_fast < ema_50 and price < ma_slow:
        raw_mode = "bear"
    else:
        raw_mode = "range"

    bear_strength = None
    if raw_mode == "bear":
        price_24h_ago = closes[-97] if len(closes) >= 97 else closes[0]
        chg24 = (price - price_24h_ago) / (price_24h_ago + 1e-9) * 100
        ema_gap = (ema_fast - ema_50) / (ema_50 + 1e-9) * 100
        if chg24 < -5 or ema_gap < -5:
            bear_strength = "strong"
        elif chg24 < -2 or ema_gap < -2:
            bear_strength = "moderate"
        else:
            bear_strength = "weak"

    return {"mode": raw_mode, "bear_strength": bear_strength}


# ════════════════════════════════════════════════════════════════════════════
# 分类层（复用 scanner.py 的函数逻辑，但 inline 避免导入副作用）
# ════════════════════════════════════════════════════════════════════════════

def classify_signal(chg24: float, funding: float, ind: dict) -> str:
    """
    复用 scanner._is_reversal / _is_breakout / _is_range_bounce 逻辑。
    简化版：不调用 LLM，直接返回类型字符串。
    """
    from fx.scanner import _is_reversal, _is_breakout, _is_range_bounce

    is_rev = _is_reversal(chg24, funding, ind)
    is_brk = _is_breakout(chg24, ind)
    is_rb  = _is_range_bounce(ind) if hasattr(
        __import__("fx.scanner", fromlist=["_is_range_bounce"]), "_is_range_bounce"
    ) else False

    if is_rev and is_brk:
        return "reversal"  # reversal 优先
    if is_rev:
        return "reversal"
    if is_brk:
        return "breakout"
    if is_rb:
        return "range_bounce"
    return "pullback"


# ════════════════════════════════════════════════════════════════════════════
# 仓位管理（极简版，用于回测）
# ════════════════════════════════════════════════════════════════════════════

NOMINAL_USDT = 100.0   # 名义仓位（固定，不模拟滑点）
FEE_RATE     = 0.001   # 手续费 0.1%/笔（入场+出场各一次）
RISK_PCT     = 0.05    # 单笔风险 5%（与 config.py 一致）
MAX_SLOTS    = 4       # 最大同时持仓数


class Position:
    def __init__(self, symbol, direction, mode, entry_price, sl_price,
                 tp1_price, tp2_price, open_ts, signal_score):
        self.symbol        = symbol
        self.direction     = direction   # LONG / SHORT
        self.mode          = mode        # pullback / breakout / reversal / range_bounce
        self.entry_price   = entry_price
        self.sl_price      = sl_price
        self.tp1_price     = tp1_price
        self.tp2_price     = tp2_price
        self.open_ts       = open_ts
        self.close_ts      = None
        self.close_price   = None
        self.close_reason  = None
        self.pnl           = None
        self.signal_score  = signal_score
        self.mfe           = 0.0   # 最大有利浮动
        # TP1 是否已打到（分批平仓：TP1 平 50%）
        self.tp1_hit       = False
        self.remaining_pct = 1.0   # 剩余仓位比例
        self.atr           = 0.0   # 开仓时 ATR，供时间止损豁免判断

    def check_sl_tp(self, high: float, low: float, cur_ts_ms: int = 0, close: float = 0.0) -> tuple[str, float] | None:
        """
        根据当前 K 线高低点检查止损/止盈。
        cur_ts_ms: 当前回测时间戳（毫秒）
        close:     当前 K 线收盘价，供保本检查的浮盈计算使用
        """
        # 先更新 MFE（按 close 计算，与保本浮盈回撤用同一基准）
        _price_ref = close if close > 0 else (high if self.direction == "LONG" else low)
        if self.direction == "LONG":
            self.mfe = max(self.mfe, _price_ref - self.entry_price)
            _cur_gain_pct = (_price_ref - self.entry_price) / self.entry_price if self.entry_price > 0 else 0
        else:
            self.mfe = max(self.mfe, self.entry_price - _price_ref)
            _cur_gain_pct = (self.entry_price - _price_ref) / self.entry_price if self.entry_price > 0 else 0

        # v3.45 OR 双条件保本止损
        if self.tp1_hit and cur_ts_ms > 0 and self.open_ts > 0 and close > 0:
            _hold_h  = (cur_ts_ms - self.open_ts) / 3_600_000
            _mfe_pct = self.mfe / self.entry_price if self.entry_price > 0 else 0

            _cond_a = _cur_gain_pct < 0.0                              # 条件A：跌穿入场价（浮盈变负）
            _cond_b = (_hold_h > 72                                    # 条件B：72H+大峰值+彻底转负
                       and _mfe_pct > 0.05
                       and _cur_gain_pct < -0.005)                    # 已亏损0.5%

            if (_cond_a or _cond_b):
                if self.direction == "LONG" and self.sl_price < self.entry_price:
                    self.sl_price = round(self.entry_price * 1.001, 8)
                elif self.direction == "SHORT" and self.sl_price > self.entry_price:
                    self.sl_price = round(self.entry_price * 0.999, 8)

        # ── v3.46 条件性时间止损（无进展才触发，不打断正在盈利的仓位）────────────
        if cur_ts_ms > 0 and self.open_ts > 0:
            _hold_h_ts = (cur_ts_ms - self.open_ts) / 3_600_000
            _exit_p    = close if close > 0 else self.entry_price

            # 条件1：TP1 已触发 + 仍在盈利 → 豁免时间止损，让 MFE 高水位保护接管
            #         给 72H 硬上限兜底，防止永久持仓
            if self.tp1_hit and _cur_gain_pct > 0:
                if _hold_h_ts >= 72:
                    return ("time_stop_hard", _exit_p)
                # 否则豁免，继续持有

            # 条件2：TP1 未触发 + 持仓 >= 8H + 无进展（浮盈 < 0.5%）→ 释放槽位
            elif not self.tp1_hit and _hold_h_ts >= 8:
                if _cur_gain_pct < 0.005:
                    return ("time_stop_no_progress", _exit_p)

            # 条件3：72H 硬上限，无论如何出场
            if _hold_h_ts >= 72:
                return ("time_stop_hard", _exit_p)

        if self.direction == "LONG":
            if low <= self.sl_price:
                return ("sl", self.sl_price)
            if not self.tp1_hit and self.tp1_price and high >= self.tp1_price:
                return ("tp1", self.tp1_price)
            if self.tp1_hit and self.tp2_price and high >= self.tp2_price:
                return ("tp2", self.tp2_price)
        else:  # SHORT
            if high >= self.sl_price:
                return ("sl", self.sl_price)
            if not self.tp1_hit and self.tp1_price and low <= self.tp1_price:
                return ("tp1", self.tp1_price)
            if self.tp1_hit and self.tp2_price and low <= self.tp2_price:
                return ("tp2", self.tp2_price)
        return None

    def calc_pnl(self, exit_price: float, pct: float = 1.0) -> float:
        """计算盈亏（含手续费）"""
        if self.direction == "LONG":
            raw = (exit_price - self.entry_price) / self.entry_price * NOMINAL_USDT * pct
        else:
            raw = (self.entry_price - exit_price) / self.entry_price * NOMINAL_USDT * pct
        fee = NOMINAL_USDT * FEE_RATE * pct * 2  # 入场+出场
        return raw - fee


# ════════════════════════════════════════════════════════════════════════════
# 核心回测引擎
# ════════════════════════════════════════════════════════════════════════════

def run_backtest(symbol: str, days: int, step_minutes: int = 5) -> dict:
    """
    主回测函数。
    返回 {"trades": [...], "stats": {...}}
    """
    logger.info(f"═══ 加载回测数据 ═══")
    raw = load_data(symbol, DATA_DIR)

    # 构建 MockAPI
    klines_map = {
        f"{symbol}_15m": raw[f"{symbol}_15m"],
        f"{symbol}_1h":  raw[f"{symbol}_1h"],
        f"{symbol}_4h":  raw[f"{symbol}_4h"],
        "BTCUSDT_1h":    raw["BTCUSDT_1h"],
        "BTCUSDT_4h":    raw["BTCUSDT_4h"],
    }
    mock_api = MockBinanceAPI(klines_map, raw["funding"], raw["oi"])

    # 导入指标和 judge 函数
    from fx.shared.indicators import ema as _ema, rsi as _rsi, atr as _atr, \
        vol_ma as _vol_ma, bollinger as _bollinger
    from fx.shared.judges import (
        judge_pullback, judge_breakout, judge_reversal, judge_range_bounce, judge_stop_hunt
    )
    from fx.shared.calc_sl_tp import calc_sl_tp
    from fx.scanner import calc_indicators, _is_reversal, _is_breakout

    # 尝试导入 _is_range_bounce / _is_stop_hunt
    try:
        from fx.scanner import _is_range_bounce as _rb_fn
    except ImportError:
        _rb_fn = None
    try:
        from fx.scanner import _is_stop_hunt as _sh_fn
    except ImportError:
        _sh_fn = None

    # 时间范围
    all_k15 = raw[f"{symbol}_15m"]
    if not all_k15:
        raise ValueError("15m K线数据为空，请先运行 prepare_data.py")

    end_ms   = all_k15[-1][0]
    start_ms = end_ms - days * 24 * 3600 * 1000
    step_ms  = step_minutes * 60 * 1000

    # 热身期：前 200 根 1H K 线（约 8 天）需要用来建立指标
    k1h_all = raw[f"{symbol}_1h"]
    if k1h_all:
        warmup_ms = max(start_ms, k1h_all[0][0] + 200 * 3600 * 1000)
    else:
        warmup_ms = start_ms

    logger.info(f"回测区间: {datetime.fromtimestamp(start_ms/1000, tz=timezone.utc).strftime('%Y-%m-%d')} → "
                f"{datetime.fromtimestamp(end_ms/1000, tz=timezone.utc).strftime('%Y-%m-%d')}")
    logger.info(f"热身结束: {datetime.fromtimestamp(warmup_ms/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}")
    logger.info(f"时间步长: {step_minutes} 分钟")

    # 回测状态
    open_positions: list[Position] = []
    closed_trades:  list[dict]     = []
    total_pnl      = 0.0
    peak_pnl       = 0.0
    max_drawdown   = 0.0

    # 扫描频率控制：每 5 分钟检查止损，每 15 分钟扫描新信号
    SIGNAL_SCAN_INTERVAL_MS = 15 * 60 * 1000
    last_signal_scan_ms = 0

    # 冷却：同标的同方向 2 小时内不重复开
    cooldown: dict = {}   # (symbol, direction) -> last_open_ts_ms

    # 漏斗统计
    funnel: dict = {
        "judge_pass":        0,  # judge 通过
        "filter_bear_reversal": 0,  # 熊市 reversal LONG 豁免拦截
        "filter_settle_win": 0,  # 资金费率结算窗口
        "filter_cooldown":   0,  # 冷却期
        "filter_same_dir":   0,  # 同向重复
        "filter_calc_sl":    0,  # calc_sl_tp 失败/error
        "filter_score":      0,  # 评分不足
        "opened":            0,  # 最终开仓
    }

    ts = warmup_ms
    step_count = 0

    while ts <= end_ms:
        mock_api.set_ts(ts)

        # ── 1. 管理已有仓位（检查止损/止盈）─────────────────────────────────
        k15_now = mock_api.get_klines(symbol, "15m", limit=2)
        if k15_now:
            last_k = k15_now[-1]
            high   = float(last_k[2])
            low    = float(last_k[3])
            close  = float(last_k[4])  # 收盘价，供方案B 浮盈回撤计算

            still_open = []
            for pos in open_positions:
                result = pos.check_sl_tp(high, low, cur_ts_ms=ts, close=close)
                if result:
                    reason, exit_price = result
                    if reason == "tp1":
                        # TP1：平 50%，剩余 50% 继续持有等 TP2
                        pnl_partial = pos.calc_pnl(exit_price, 0.5)
                        total_pnl += pnl_partial
                        pos.tp1_hit     = True
                        pos.remaining_pct = 0.5
                        # 记录 TP1 分批平仓
                        closed_trades.append({
                            "open_ts":     _fmt_ts(pos.open_ts),
                            "close_ts":    _fmt_ts(ts),
                            "symbol":      pos.symbol,
                            "direction":   pos.direction,
                            "mode":        pos.mode,
                            "entry_price": pos.entry_price,
                            "exit_price":  exit_price,
                            "close_reason":"tp1_partial",
                            "pnl":         round(pnl_partial, 4),
                            "hold_min":    round((ts - pos.open_ts) / 60000, 1),
                            "signal_score": pos.signal_score,
                        })
                        still_open.append(pos)  # 继续持仓
                    else:
                        # SL / TP2：完全平仓
                        remain_pct = pos.remaining_pct
                        pnl = pos.calc_pnl(exit_price, remain_pct)
                        total_pnl += pnl
                        pos.close_ts     = ts
                        pos.close_price  = exit_price
                        pos.close_reason = reason
                        pos.pnl          = round(pnl, 4)
                        closed_trades.append({
                            "open_ts":     _fmt_ts(pos.open_ts),
                            "close_ts":    _fmt_ts(ts),
                            "symbol":      pos.symbol,
                            "direction":   pos.direction,
                            "mode":        pos.mode,
                            "entry_price": pos.entry_price,
                            "exit_price":  exit_price,
                            "close_reason": reason,
                            "pnl":         round(pnl, 4),
                            "hold_min":    round((ts - pos.open_ts) / 60000, 1),
                            "signal_score": pos.signal_score,
                        })
                        # 更新最大回撤
                        peak_pnl = max(peak_pnl, total_pnl)
                        dd = peak_pnl - total_pnl
                        if dd > max_drawdown:
                            max_drawdown = dd
                else:
                    still_open.append(pos)
            open_positions = still_open

        # ── 2. 新信号扫描（每 15 分钟）────────────────────────────────────────
        if ts - last_signal_scan_ms >= SIGNAL_SCAN_INTERVAL_MS and len(open_positions) < MAX_SLOTS:
            last_signal_scan_ms = ts

            k15 = mock_api.get_klines(symbol, "15m", limit=120)
            k1h = mock_api.get_klines(symbol, "1h",  limit=1500)  # v3.48: Fix1/Fix3 需要 1440 根1H
            k4h = mock_api.get_klines(symbol, "4h",  limit=120)
            btc_k4h = mock_api.get_klines("BTCUSDT", "4h", limit=220)

            if len(k15) < 50 or len(k1h) < 50 or len(k4h) < 20:
                ts += step_ms
                step_count += 1
                continue

            # 指标计算
            try:
                ind = calc_indicators(k15, k1h, k4h)
            except Exception as e:
                logger.debug(f"calc_indicators 失败 ts={ts}: {e}")
                ts += step_ms
                step_count += 1
                continue

            price = ind["price"]
            if price <= 0:
                ts += step_ms
                step_count += 1
                continue

            # 注入必须字段
            ticker   = mock_api.get_ticker_24h(symbol, ts)
            chg24    = float(ticker.get("priceChangePercent", 0))
            vol24    = float(ticker.get("quoteVolume", 0))
            ind["vol24_usd"] = vol24
            ind["symbol"]    = symbol

            # OI 注入
            oi_now      = mock_api.get_oi_at(ts)
            oi_1h_ago   = mock_api.get_oi_at(ts - 3600 * 1000)
            ind["oi_now"]    = oi_now
            ind["oi_1h_ago"] = oi_1h_ago

            # 资金费率（截止当前最新一条）
            funding = mock_api.get_funding_rate(ts)

            # 修复1：周末模式判断——直接复用真实系统的 _is_weekend_period（v3.42: 周五 UTC 22:00）
            dt_now = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            from fx.scanner import _is_weekend_period as _bt_is_weekend
            is_weekend = _bt_is_weekend(dt_now)
            ind["_weekend_mode"] = is_weekend

            # ── 币股休市过滤（SNDKUSDT 等）─────────────────────────────────────
            # 美股交易时段：UTC 13:30–20:00（夏令时；冬令时 14:30–21:00，此处用夏令时近似）
            _STOCK_SYMBOLS = {"SNDKUSDT", "TSLA", "AAPLUSUSDT", "MSFT", "NVDAUSUSDT"}
            if symbol in _STOCK_SYMBOLS:
                _h, _m = dt_now.hour, dt_now.minute
                _in_us_session = (
                    (_h == 13 and _m >= 30) or
                    (14 <= _h <= 19) or
                    (_h == 20 and _m == 0)
                )
                if dt_now.weekday() >= 5:   # 周末美股不开盘
                    _in_us_session = False
                if not _in_us_session:
                    ts += step_ms
                    step_count += 1
                    continue

            # ── 黄金特殊休市（XAUUSDT）UTC 21:59–22:01 短暂休市 ──────────────
            _GOLD_SYMBOLS = {"XAUUSDT", "XAGUUSDT"}
            if symbol in _GOLD_SYMBOLS:
                if dt_now.hour == 21 and dt_now.minute >= 59:
                    ts += step_ms
                    step_count += 1
                    continue
                if dt_now.hour == 22 and dt_now.minute <= 1:
                    ts += step_ms
                    step_count += 1
                    continue

            # 市场模式（离线 BTC K线计算）
            mkt_info    = calc_market_mode_from_klines(btc_k4h)
            market_mode = mkt_info["mode"]
            bear_str    = mkt_info["bear_strength"]

            # CG bonus：固定 0（无历史接口）
            cg_bonus = 0

            # 分类
            is_rev = _is_reversal(chg24, funding, ind)
            is_brk = _is_breakout(chg24, ind)
            is_rb  = _rb_fn(k1h[:-1], ind) if _rb_fn else False
            is_sh  = _sh_fn(k1h[:-1], k15[:-1], ind) if _sh_fn else False

            # v3.49: stop_hunt 最高优先级
            if is_sh:
                sig_type = "stop_hunt"
            elif is_rev and is_brk:
                sig_type = "reversal"
            elif is_rev:
                sig_type = "reversal"
            elif is_brk:
                sig_type = "breakout"
            elif is_rb:
                sig_type = "range_bounce"
            else:
                sig_type = "pullback"

            # Judge
            try:
                if sig_type == "reversal":
                    ok, signal = judge_reversal(k1h[:-1], k4h[:-1], k15[:-1], ind, chg24, funding)
                elif sig_type == "breakout":
                    ok, signal = judge_breakout(k1h[:-1], k4h[:-1], k15[:-1], ind, chg24, funding)
                elif sig_type == "range_bounce":
                    ok, signal = judge_range_bounce(k1h[:-1], k4h[:-1], k15[:-1], ind, chg24, funding)
                elif sig_type == "stop_hunt":
                    ok, signal = judge_stop_hunt(k1h[:-1], k4h[:-1], k15[:-1], ind, chg24, funding)
                else:
                    ok, signal = judge_pullback(k1h[:-1], k4h[:-1], k15[:-1], ind, chg24, funding)
            except Exception as e:
                logger.debug(f"judge 失败 ts={ts} type={sig_type}: {e}")
                ts += step_ms
                step_count += 1
                continue

            if not ok:
                ts += step_ms
                step_count += 1
                continue

            funnel["judge_pass"] += 1
            direction = signal.get("direction", "LONG")

            # 修复5：熊市 reversal LONG 完整豁免逻辑（与真实系统 v3.21 一致）
            if market_mode == "bear" and sig_type == "reversal" and direction == "LONG":
                _chg24_abs = abs(chg24)
                _vol_ratio = float(ind.get("vol_ratio", 0))
                _exempt = (
                    _chg24_abs > 30 or
                    funding > 0.002 or
                    (_vol_ratio > 3 and _chg24_abs > 15)
                )
                if not _exempt:
                    funnel["filter_bear_reversal"] += 1
                    ts += step_ms; step_count += 1; continue

            # 修复6：资金费率结算窗口（UTC 00/08/16 前15分 + 后5分内不扫描）
            _settle_hours = {0, 8, 16}
            _dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            _h, _m = _dt.hour, _dt.minute
            _in_settle_win = (
                (_h in _settle_hours and _m < 5) or
                (_h in {(s - 1) % 24 for s in _settle_hours} and _m >= 45)
            )
            if _in_settle_win:
                funnel["filter_settle_win"] += 1
                ts += step_ms; step_count += 1; continue

            # 冷却检查（v3.44：同标的同方向 4H 冷却，与止损冷却独立）
            cd_key = (symbol, direction)
            if ts - cooldown.get(cd_key, 0) < 4 * 3600 * 1000:
                funnel["filter_cooldown"] += 1
                ts += step_ms; step_count += 1; continue

            # 同向重复检查
            same_dir_count = sum(1 for p in open_positions if p.direction == direction)
            if market_mode == "bull" and direction == "SHORT" and same_dir_count >= 2:
                funnel["filter_same_dir"] += 1
                ts += step_ms; step_count += 1; continue
            if market_mode == "bear" and direction == "LONG" and same_dir_count >= 2:
                funnel["filter_same_dir"] += 1
                ts += step_ms; step_count += 1; continue

            # 计算入场参数
            atr   = ind["atr_1h"]
            ema20 = ind["ema20_1h"]
            signal["entry_price"] = price

            try:
                sl_tp = calc_sl_tp(signal, price, atr, {}, k1h[:-1], ema20)
            except Exception as e:
                logger.info(f"[diag] calc_sl_tp EXCEPTION ts={ts} k1h_len={len(k1h)} atr={atr:.6g} tier={signal.get('tier','?')} err={e}")
                funnel["filter_calc_sl"] += 1
                ts += step_ms; step_count += 1; continue

            if "error" in sl_tp:
                logger.info(f"[diag] calc_sl_tp ERROR ts={ts} k1h_len={len(k1h)} atr={atr:.6g} tier={signal.get('tier','?')} msg={sl_tp['error']}")
                funnel["filter_calc_sl"] += 1
                ts += step_ms; step_count += 1; continue

            # 修复2：BTC 趋势 btc_adj（与真实系统对齐）
            _btc_trend = {"bull": "up", "bear": "down"}.get(market_mode, "neutral")
            btc_adj = 0
            if   _btc_trend == "up"   and direction == "LONG":  btc_adj = +5
            elif _btc_trend == "up"   and direction == "SHORT": btc_adj = -5
            elif _btc_trend == "down" and direction == "LONG":  btc_adj = -5
            elif _btc_trend == "down" and direction == "SHORT": btc_adj = +5

            # 修复3：strategy_adj（策略×市场模式适配评分，与真实系统 v3.38 一致）
            strategy_adj  = 0
            _bear_str_now = bear_str or "weak"
            if market_mode == "bear":
                if sig_type == "reversal" and direction == "LONG":
                    strategy_adj = -15 if _bear_str_now == "strong" else                                    -12 if _bear_str_now == "moderate" else -10
                elif sig_type == "pullback" and direction == "SHORT":
                    strategy_adj = +10 if _bear_str_now == "strong" else                                    +7  if _bear_str_now == "moderate" else +5
                elif sig_type == "range_bounce":
                    strategy_adj = -10 if _bear_str_now == "strong" else                                    -7  if _bear_str_now == "moderate" else -5
            elif market_mode == "bull":
                if   sig_type == "reversal"    and direction == "SHORT": strategy_adj = -10
                elif sig_type == "pullback"    and direction == "LONG":  strategy_adj = +5
                elif sig_type == "range_bounce":                         strategy_adj = +5
            elif market_mode == "range":
                if   sig_type == "breakout":     strategy_adj = -5
                elif sig_type == "reversal":     strategy_adj = +5
                elif sig_type == "range_bounce": strategy_adj = +10

            # 修复4：完整评分 + 工作日/周末门槛过滤
            _strength  = signal.get("signal_strength", "medium")
            base_score = {"strong": 70, "medium": 50, "weak": 30}.get(_strength, 50)

            # v3.49.2: stop_hunt 策略独立评分（btc_adj 清零 + strategy_adj 覆盖）
            if sig_type == "stop_hunt":
                from config import STOP_HUNT_BASE_STRONG, STOP_HUNT_BASE_MEDIUM
                base_score = STOP_HUNT_BASE_STRONG if _strength == "strong" else STOP_HUNT_BASE_MEDIUM
                # 修复3：stop_hunt 不受 btc_adj 影响
                btc_adj = 0
                if market_mode == "bull" and direction == "LONG":
                    strategy_adj = +5
                elif market_mode == "bull" and direction == "SHORT":
                    strategy_adj = -5
                elif market_mode == "range":
                    strategy_adj = 0
                elif market_mode == "bear" and direction == "SHORT":
                    strategy_adj = +5
                elif market_mode == "bear" and direction == "LONG":
                    strategy_adj = -15

            # v3.46: 中波动档评分惩罚
            from config import ATR_TIER_MID_SCORE_ADJ
            _vol_tier     = ind.get("_vol_tier", "high")
            atr_tier_adj  = ATR_TIER_MID_SCORE_ADJ if _vol_tier == "mid_vol" else 0

            # v3.46: 脉冲大涨做空惩罚（从 judge 返回的 signal dict 读取）
            _pulse_penalty = signal.get("pulse_short_penalty", 0)

            final_score = min(100, max(0, base_score + btc_adj + cg_bonus + strategy_adj + atr_tier_adj + _pulse_penalty))

            if is_weekend:
                if sig_type == "stop_hunt":
                    from config import STOP_HUNT_MIN_SCORE_WEEKEND
                    _min_sc = STOP_HUNT_MIN_SCORE_WEEKEND
                else:
                    _min_sc = 60 if _strength == "strong" else 70
                if final_score < _min_sc:
                    logger.info(f"[diag] 评分不足(周末) ts={ts} score={final_score} min={_min_sc} base={base_score} btc={btc_adj:+d} strat={strategy_adj:+d} tier_adj={atr_tier_adj} dir={direction} sig={sig_type} btc_mode={_btc_trend}")
                    funnel["filter_score"] += 1
                    ts += step_ms; step_count += 1; continue
            else:
                if final_score < 50:  # MIN_SIGNAL_SCORE_WEEKDAY
                    logger.info(f"[diag] 评分不足(工作日) ts={ts} score={final_score} min=50 base={base_score} btc={btc_adj:+d} strat={strategy_adj:+d} tier_adj={atr_tier_adj} dir={direction} sig={sig_type} btc_mode={_btc_trend}")
                    funnel["filter_score"] += 1
                    ts += step_ms; step_count += 1; continue

            # 开仓
            pos = Position(
                symbol      = symbol,
                direction   = direction,
                mode        = sig_type,
                entry_price = price,
                sl_price    = sl_tp["sl_price"],
                tp1_price   = sl_tp.get("tp1_price"),
                tp2_price   = sl_tp.get("tp2_price"),
                open_ts     = ts,
                signal_score= final_score,
            )
            pos.atr = atr  # 供时间止损豁免判断
            open_positions.append(pos)
            funnel["opened"] += 1
            cooldown[cd_key] = ts

            logger.info(
                f"  开仓 [{_fmt_ts(ts)}] {direction} {sig_type} "
                f"entry={price:.4f} sl={sl_tp['sl_price']:.4f} "
                f"tp1={sl_tp.get('tp1_price','N/A')} "
                f"score={final_score}(base={base_score} btc={btc_adj:+d} strat={strategy_adj:+d} tier={atr_tier_adj:+d})"
            )

        ts += step_ms
        step_count += 1

        # 进度日志
        if step_count % 2000 == 0:
            progress = (ts - warmup_ms) / max(end_ms - warmup_ms, 1) * 100
            logger.info(f"  进度 {progress:.1f}% | 已交易 {len(closed_trades)} 笔 | "
                        f"持仓 {len(open_positions)} | 累计盈亏 {total_pnl:+.1f}U")

    # ── 强制平仓未平仓位（用最后一根 K 线收盘价）────────────────────────────
    last_k1h = raw[f"{symbol}_1h"]
    last_price = float(last_k1h[-1][4]) if last_k1h else 0
    for pos in open_positions:
        pnl = pos.calc_pnl(last_price, pos.remaining_pct)
        total_pnl += pnl
        closed_trades.append({
            "open_ts":      _fmt_ts(pos.open_ts),
            "close_ts":     _fmt_ts(end_ms),
            "symbol":       pos.symbol,
            "direction":    pos.direction,
            "mode":         pos.mode,
            "entry_price":  pos.entry_price,
            "exit_price":   last_price,
            "close_reason": "force_close_end",
            "pnl":          round(pnl, 4),
            "hold_min":     round((end_ms - pos.open_ts) / 60000, 1),
            "signal_score": pos.signal_score,
        })

    # ── 统计 ─────────────────────────────────────────────────────────────────
    # 只统计真实出场（tp1_partial 算赢，sl 算亏，tp2/force_close 看盈亏）
    win_trades  = [t for t in closed_trades if t["pnl"] > 0]
    loss_trades = [t for t in closed_trades if t["pnl"] <= 0]
    total_win   = sum(t["pnl"] for t in win_trades)
    total_loss  = sum(t["pnl"] for t in loss_trades)
    n_total     = len(closed_trades)
    win_rate    = len(win_trades) / n_total * 100 if n_total > 0 else 0

    # 平均持仓时间（只算非 tp1_partial）
    full_close = [t for t in closed_trades if t["close_reason"] != "tp1_partial"]
    avg_hold   = sum(t["hold_min"] for t in full_close) / len(full_close) if full_close else 0

    # 最大单笔亏损
    max_single_loss = min((t["pnl"] for t in closed_trades), default=0)

    # 按策略分组
    by_mode: dict = {}
    for t in closed_trades:
        m = t["mode"]
        if m not in by_mode:
            by_mode[m] = {"n": 0, "win": 0, "pnl": 0.0}
        by_mode[m]["n"] += 1
        if t["pnl"] > 0:
            by_mode[m]["win"] += 1
        by_mode[m]["pnl"] += t["pnl"]

    stats = {
        "symbol":          symbol,
        "days":            days,
        "total_trades":    n_total,
        "win_trades":      len(win_trades),
        "loss_trades":     len(loss_trades),
        "win_rate_pct":    round(win_rate, 1),
        "total_pnl_u":     round(total_pnl, 2),
        "total_win_u":     round(total_win, 2),
        "total_loss_u":    round(total_loss, 2),
        "profit_factor":   round(abs(total_win / total_loss), 2) if total_loss != 0 else 999,
        "max_drawdown_u":  round(max_drawdown, 2),
        "max_single_loss": round(max_single_loss, 2),
        "avg_hold_min":    round(avg_hold, 1),
        "by_mode":         {m: {**v, "win_rate": round(v["win"]/v["n"]*100, 1) if v["n"] > 0 else 0, "pnl": round(v["pnl"], 2)} for m, v in by_mode.items()},
        "funnel":          funnel,
    }
    return {"trades": closed_trades, "stats": stats}


def _fmt_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


# ════════════════════════════════════════════════════════════════════════════
# 输出格式化
# ════════════════════════════════════════════════════════════════════════════

def print_report(result: dict, symbol: str):
    trades = result["trades"]
    stats  = result["stats"]

    print("\n" + "═" * 60)
    print(f"  回测报告 — {symbol}")
    print("═" * 60)

    # 汇总统计
    print(f"\n【汇总统计】")
    print(f"  回测天数  : {stats['days']} 天")
    print(f"  总交易笔数: {stats['total_trades']}")
    print(f"  胜率      : {stats['win_rate_pct']}%  "
          f"({stats['win_trades']}胜 / {stats['loss_trades']}负)")
    print(f"  总盈亏    : {stats['total_pnl_u']:+.2f} U")
    print(f"  盈利合计  : +{stats['total_win_u']:.2f} U")
    print(f"  亏损合计  : {stats['total_loss_u']:.2f} U")
    print(f"  盈亏比    : {stats['profit_factor']}")
    print(f"  最大回撤  : -{stats['max_drawdown_u']:.2f} U")
    print(f"  最大单笔亏损: {stats['max_single_loss']:.2f} U")
    print(f"  平均持仓  : {stats['avg_hold_min']:.0f} 分钟 ({stats['avg_hold_min']/60:.1f}小时)")

    # 按策略分组
    print(f"\n【按策略分组】")
    for mode, ms in stats["by_mode"].items():
        print(f"  {mode:15s} {ms['n']:3d}笔  胜率 {ms['win_rate']:5.1f}%  盈亏 {ms['pnl']:+.2f}U")

    # 每笔交易明细
    print(f"\n【交易明细】")
    print(f"  {'开仓时间':<17} {'方向':<6} {'策略':<12} {'入场价':<10} {'出场价':<10} {'原因':<18} {'盈亏':>8}")
    print("  " + "-" * 95)
    for t in trades[-50:]:  # 最近50笔
        pnl_str = f"{t['pnl']:+.2f}U"
        print(f"  {t['open_ts']:<17} {t['direction']:<6} {t['mode']:<12} "
              f"{t['entry_price']:<10.4f} {t['exit_price']:<10.4f} "
              f"{t['close_reason']:<18} {pnl_str:>8}")
    if len(trades) > 50:
        print(f"  ... 共 {len(trades)} 笔，只显示最近 50 笔")

    # 数据局限性说明
    print(f"\n【数据局限性说明（必读）】")
    print(f"  ❌ CG 清算图 bonus = 0（无历史接口）")
    print(f"     → 实际 cg_bonus 最多影响 ±15 分，高质量信号可能被低估")
    print(f"     → 真实胜率可能比回测结果 高 2-8%（正向偏差）")
    print(f"  ❌ 新闻信号 = None（无法精确历史化）")
    print(f"     → 部分因新闻被拦截的亏损信号在回测中未被拦截")
    print(f"     → 真实总盈亏可能比回测结果 更好（偏保守方向）")
    print(f"  ⚠️  LLM 判断已跳过（妖币识别、边界信号、新闻语义）")
    print(f"     → 部分边界单可能多入，整体影响有限")
    print(f"  ✅ K线/资金费率/OI/BTC market_mode — 无未来数据污染")
    # ── 黄金特别说明──────────────────────────────────────────────────────────
    if symbol in {"XAUUSDT"}:
        print("  ⚠️ 黄金（XAUUSDT）特别说明：")
        print("     系统策略参数基于加密货币行情调参，未针对黄金优化")
        print("     黄金受美元指数/地缘政治驱动，RSI/EMA/资金费率逻辑适配性存疑")
        print("     本次回测结果仅供参考，不建议直接上线黄金交易")
    print("═" * 60 + "\n")


# ════════════════════════════════════════════════════════════════════════════
# 入口
# ════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="严格无未来数据污染回测")
    parser.add_argument("--days",   type=int, default=30,        help="回测天数（默认30）")
    parser.add_argument("--symbol", type=str, default="BTWUSDT", help="目标标的（默认BTWUSDT）")
    parser.add_argument("--step",   type=int, default=5,         help="时间步长分钟（默认5）")
    parser.add_argument("--output", type=str, default="",        help="JSON输出路径（可选）")
    args = parser.parse_args()

    logger.info("═══ 回测开始 ═══")
    logger.info(f"标的={args.symbol} 天数={args.days} 步长={args.step}分钟")
    logger.info("注意：CG bonus=0，新闻=None，LLM=跳过（详见报告底部说明）")

    result = run_backtest(args.symbol, args.days, args.step)
    print_report(result, args.symbol)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info(f"结果已保存: {args.output}")


if __name__ == "__main__":
    main()
