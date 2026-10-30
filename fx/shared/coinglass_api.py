"""
coinglass_api.py — 已迁移至 Clawby API（2026-08-28）

原 CoinData/CG 代理已替换为 Clawby Pro（api.openclawby.com）
速率提升：30 req/min → 1200 req/min（40 倍）

本文件现在是 clawby_adapter 的薄包装层，保持所有对外接口签名不变。
所有实现逻辑已迁移到 fx/shared/clawby_adapter.py。

✨ 新增接口（clawby_adapter 直接导出）：
  get_whale_index()        — 巨鲸指数（合约工作流）
  get_cvd()                — CVD 累计成交量差（合约工作流）
  get_coinbase_premium()   — Coinbase 溢价（合约工作流）
  get_fear_greed()         — 贪婪恐惧指数（合约工作流）
  get_boll()               — 布林带（合约工作流）
  get_liq_max_pain()       — 清算 Max Pain（合约工作流）
  get_funding_arb_opps()   — 资金费率套利机会榜（套利工作流）
  get_funding_exchange_list() — 跨所费率快照（套利工作流）
  get_oi_exchange_list()   — 跨所 OI（套利工作流）
  get_economic_calendar()  — 宏观经济日历（套利工作流）
  get_btc_etf_flow()       — BTC ETF 净流入（bstocks 工作流）
  get_btc_etf_aum()        — BTC ETF AUM（bstocks 工作流）
  get_hl_whale_alerts()    — HL 巨鲸新开仓（bstocks 工作流）
  get_hl_whale_positions() — HL 巨鲸持仓（bstocks 工作流）
  get_altcoin_season()     — 山寨季指数（bstocks 工作流）
  get_bull_market_peak()   — 牛市顶峰指标
"""

import requests
import time
import threading
import logging

_cg_logger = logging.getLogger("fx.coinglass")

# ── Coinglass 健康监控（同步自 paper_scanner.py v3.2 功能九）────────────────
_CG_HEALTH: dict = {"ok": 0, "fail_404": 0, "fail_timeout": 0, "hour_start": time.time()}
_CG_HEALTH_LOCK = threading.Lock()

# ────────────────────────────────────────────────────────────────────────────
# 全量导入 clawby_adapter — 所有对外接口均从此层提供
# ────────────────────────────────────────────────────────────────────────────
import os as _os
import sys as _sys
import logging

_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
# 让顶层名 clawby_adapter 与 shared.clawby_adapter 是「同一个模块实例」：
#   否则同一文件会被加载两次，各有一份内存缓存和一个后台落盘线程（缓存改为纯内存后会互相看不到）。
try:
    from shared import clawby_adapter as _ca_single
    _sys.modules["clawby_adapter"] = _ca_single
except Exception:
    pass
from clawby_adapter import (  # noqa: F401  将全部公共接口再导出
    # 清算热力图
    get_liq_heatmap, get_liq_density, get_liq_zones,
    get_liquidation_zone, get_liq_zones_list,
    # 爆仓历史
    _get_liq_history, get_liq_summary_1h, get_liq_orders, get_liq_coin_list,
    # Binance fapi 公开接口（无需 Clawby）
    _get_top_lsr, _get_taker, _get_oi_history, _get_funding_rate,
    # 衍生品信号汇总
    get_derivatives_signals,
    # ✨ 新增 — 合约工作流（get_whale_index/get_boll 已停用但保留签名）
    get_whale_index, get_cvd, get_coinbase_premium, get_fear_greed,
    get_boll, get_liq_max_pain, get_liq_aggregated_heatmap, get_bull_market_peak,
    # CG中转 LSR 专用接口（带令牌桶限速）
    get_lsr_signal, cg_get,
    # ✨ 新增 — 套利工作流
    get_funding_arb_opps, get_funding_exchange_list,
    get_oi_exchange_list, get_economic_calendar,
    # ✨ 新增 — bstocks 工作流
    get_btc_etf_flow, get_btc_etf_aum,
    get_hl_whale_alerts, get_hl_whale_positions,
    get_altcoin_season,
    # 底层工具
    clawby_relay, test_connection, _cb_record as _cg_record,
    CACHE_TTL, _cache_get, _cache_set, _sym_full, _sym_base,
)

_cg_logger = logging.getLogger("fx.coinglass")

# ── 向后兼容：保留原 get_whale_signals / get_kol_signals 存根 ─────────────────
def get_whale_signals(symbol: str = None) -> dict:
    """
    已迁移：使用 get_hl_whale_positions() + get_hl_whale_alerts() 替代。
    此存根保持向后兼容，返回 HL 巨鲸方向摘要。
    """
    try:
        positions = get_hl_whale_positions(10)
        if not positions:
            return {}
        if symbol:
            coin = symbol.upper().replace("USDT", "").replace("BUSD", "")
            positions = [p for p in positions if p.get("symbol", "").upper() == coin]
        if not positions:
            return {}
        long_sz  = sum(float(p.get("position_size", 0))
                       for p in positions if str(p.get("side", "")).lower() in ("long", "buy"))
        short_sz = sum(float(p.get("position_size", 0))
                       for p in positions if str(p.get("side", "")).lower() in ("short", "sell"))
        total = long_sz + short_sz
        if total == 0:
            return {}
        long_pct = long_sz / total
        if long_pct > 0.65:
            whale_dir, conf = "LONG", "high" if long_pct > 0.75 else "medium"
        elif long_pct < 0.35:
            whale_dir, conf = "SHORT", "high" if long_pct < 0.25 else "medium"
        else:
            whale_dir, conf = None, "low"
        return {
            "coin": symbol or "ALL",
            "long_sz": round(long_sz, 2), "short_sz": round(short_sz, 2),
            "whale_direction": whale_dir, "confidence": conf,
        }
    except Exception as e:
        _cg_logger.debug(f"[get_whale_signals] error: {e}")
        return {}


def get_kol_signals(symbol: str = None, exchange: str = "okx") -> dict:
    """
    已迁移：原 KOL 接口依赖 CG 代理，Clawby 无对应接口。
    暂返回空 dict，不影响主流程评分。
    """
    return {}


# ── 评分计算（从原 coinglass_api.py 保留，依赖 get_derivatives_signals）──────
def _calc_deriv_score(direction: str, sig: dict,
                      whale_sig: dict = None, kol_sig: dict = None):
    """衍生品维度评分 + 巨鲸方向修正（与原版完全一致）"""
    delta   = 0
    reasons = []

    if whale_sig and not whale_sig.get("_error") and whale_sig.get("whale_direction"):
        wd  = whale_sig["whale_direction"]
        wc  = whale_sig.get("confidence", "low")
        w_pts = 5 if wc == "high" else 3
        if wd == direction:
            delta += w_pts;  reasons.append(f"巨鲸{wd}盈利({wc}):{+w_pts}")
        else:
            delta -= w_pts;  reasons.append(f"巨鲸逆向{wd}({wc}):{-w_pts}")

    if kol_sig and not kol_sig.get("_error") and kol_sig.get("kol_direction"):
        kd = kol_sig["kol_direction"]
        kc = kol_sig.get("confidence", "low")
        if kc != "low":
            if kd == direction:
                delta += 2;  reasons.append(f"KOL{kd}共识({kc}):+2")
            else:
                delta -= 2;  reasons.append(f"KOL逆向{kd}({kc}):-2")

    fr        = sig.get("fr_latest")
    fr_trend  = sig.get("fr_trend", "neutral")
    lsr_sig   = sig.get("lsr_signal", "neutral")
    taker     = sig.get("taker_signal", "neutral")
    oi_sig    = sig.get("oi_signal", "neutral")
    liq_sig   = sig.get("liq_signal", "neutral")

    # ✨ 新增: whale_index / cvd_delta / cb_premium 补充评分
    whale_idx = sig.get("whale_index")
    cvd_delta = sig.get("cvd_delta")
    cb_prem   = sig.get("cb_premium")

    if direction == "LONG":
        if lsr_sig == "long_friendly":
            delta += 5; reasons.append(f"空头占比>{sig.get('short_pct',0):.0%}:+5")
        if liq_sig == "heavy_long_liq":
            delta += 3; reasons.append("多头爆仓超卖反弹:+3")
        if oi_sig == "deleverage":
            delta += 4; reasons.append("OI降去杠杆超跌:+4")
        if taker == "strong_buy":
            delta += 5; reasons.append(f"大单主买{sig.get('buy_vol_pct',0):.0%}:+5")
        if fr_trend == "falling":
            delta += 3; reasons.append("资金率持续下行:+3")
        if lsr_sig == "short_friendly":
            delta -= 5; reasons.append(f"多头拥挤{sig.get('long_pct',0):.0%}:-5")
        if fr_trend == "rising":
            delta -= 3; reasons.append("资金率上行成本高:-3")
        if taker == "strong_sell":
            delta -= 3; reasons.append(f"大单主卖{1-sig.get('buy_vol_pct',0.5):.0%}:-3")
        # ✨ 新增评分
        if whale_idx is not None and whale_idx > 150:
            delta += 4; reasons.append(f"巨鲸指数高({whale_idx:.0f}):+4")
        if cvd_delta is not None and cvd_delta > 0:
            delta += 3; reasons.append(f"CVD净买入(+{cvd_delta/1e6:.1f}M):+3")
        elif cvd_delta is not None and cvd_delta < -50_000_000:
            delta -= 3; reasons.append(f"CVD净卖出({cvd_delta/1e6:.1f}M):-3")
        # cb_prem 是百分比格式（-0.11 = -0.11%），阈值 0.1 = ±0.1%
        if cb_prem is not None and cb_prem > 0.1:
            delta += 2; reasons.append(f"Coinbase溢价+{cb_prem:.2f}%:+2")
        elif cb_prem is not None and cb_prem < -0.1:
            delta -= 2; reasons.append(f"Coinbase折价{cb_prem:.2f}%:-2")

    elif direction == "SHORT":
        if lsr_sig == "short_friendly":
            delta += 5; reasons.append(f"多头拥挤{sig.get('long_pct',0):.0%}:+5")
        if liq_sig == "heavy_short_liq":
            delta += 3; reasons.append("空头爆仓超涨回落:+3")
        if oi_sig == "accumulate":
            delta += 4; reasons.append("OI升多头超买:+4")
        if taker == "strong_sell":
            delta += 5; reasons.append(f"大单主卖{1-sig.get('buy_vol_pct',0.5):.0%}:+5")
        if fr_trend == "rising":
            delta += 4; reasons.append("资金率上行空头收益:+4")
        if lsr_sig == "long_friendly":
            delta -= 5; reasons.append(f"空头拥挤{sig.get('short_pct',0):.0%}:-5")
        if fr is not None and fr < -0.003:
            delta -= 8; reasons.append(f"资金率极度负{fr:.4%}禁空:-8")
        if taker == "strong_buy":
            delta -= 5; reasons.append(f"大单主买强势{sig.get('buy_vol_pct',0):.0%}:-5")
        if oi_sig == "deleverage":
            delta -= 4; reasons.append("OI降空头平仓推涨:-4")
        # ✨ 新增评分
        if whale_idx is not None and whale_idx < 50:
            delta += 3; reasons.append(f"巨鲸指数低({whale_idx:.0f}):+3")
        if cvd_delta is not None and cvd_delta < 0:
            delta += 3; reasons.append(f"CVD净卖出({cvd_delta/1e6:.1f}M):+3")
        if cb_prem is not None and cb_prem < -0.1:
            delta += 2; reasons.append(f"Coinbase折价{cb_prem:.2f}%:+2")

    # [refactor-B11] 衍生品总分负向下限 -5：保留费率/OI/多空比极端拥挤的风险信息，
    #   但不让它单独把信号压死。正向加分不受影响。
    delta = max(delta, -5)
    return delta, reasons


def get_signal_for_open(symbol: str, direction: str,
                        current_price: float = None,
                        exchange: str = "Binance") -> dict:
    """综合开仓信号（与原版完全一致，已集成 whale_index/cvd/cb_premium 新维度）"""
    import requests as _req
    try:
        if current_price is None or current_price <= 0:
            sym = _sym_full(symbol)
            r = _req.get("https://fapi.binance.com/fapi/v1/ticker/price",
                         params={"symbol": sym}, timeout=5).json()
            current_price = float(r.get("price", 0))

        zones        = get_liq_zones(symbol, current_price, exchange)
        heatmap_bias = zones["bias"]

        if direction == "LONG":
            if heatmap_bias == "long":
                heatmap_score = 10
                h_reason = "✅ 热力图顺势（上方空头密集）:+10"
            elif heatmap_bias == "short":
                # [refactor-B11] 原 -20。CG 清算图只做加分项：逆势不再扣分，
                #   方向性否决交给策略门槛与 BTC 趋势过滤，避免多项扣分叠加压死信号。
                heatmap_score = 0
                h_reason = f"热力图逆势（下方多头密集 {zones['ratio']:.2f}，不扣分）:0"
            else:
                heatmap_score = 0
                h_reason = "热力图中性:0"
        else:
            if heatmap_bias == "short":
                heatmap_score = 10
                h_reason = "✅ 热力图顺势（下方多头密集）:+10"
            elif heatmap_bias == "long":
                # [refactor-B11] 原 -15，同上：逆势不扣分
                heatmap_score = 0
                h_reason = f"热力图逆势（上方空头密集 {zones['ratio']:.2f}，不扣分）:0"
            else:
                heatmap_score = 0
                h_reason = "热力图中性:0"

        deriv_sig  = get_derivatives_signals(symbol, current_price, exchange)
        whale_sig  = get_whale_signals(symbol)
        kol_sig    = get_kol_signals(symbol)

        deriv_delta, deriv_reasons = _calc_deriv_score(
            direction, deriv_sig, whale_sig, kol_sig)

        score_adjust = heatmap_score + deriv_delta
        bias_label   = "LONG_BIAS" if score_adjust > 5 else (
                        "SHORT_BIAS" if score_adjust < -5 else "NEUTRAL")
        reason_str   = h_reason + " | " + " | ".join(deriv_reasons)

        liq_z = zones
        resist = liq_z["above"][0][0] if liq_z.get("above") else current_price * 1.03
        sl_ref = liq_z["below"][0][0] if liq_z.get("below") else current_price * 0.97
        if direction == "LONG":
            tp1 = round(current_price * 1.025, 6)
            tp2 = round(min(resist * 0.998, current_price * 1.05), 6)
            sl  = round(max(sl_ref  * 0.998, current_price * 0.95), 6)
        else:
            tp1 = round(current_price * 0.975, 6)
            tp2 = round(max(sl_ref  * 1.002, current_price * 0.95), 6)
            sl  = round(min(resist  * 1.002, current_price * 1.05), 6)

        return {
            "score_adjust":  score_adjust,
            "heatmap_score": heatmap_score,
            "deriv_score":   deriv_delta,
            "bias":          bias_label,
            "reason":        reason_str,
            "tp1": tp1, "tp2": tp2, "sl": sl,
            "zones":         liq_z,
            "deriv_sig":     deriv_sig,
            "resist":        resist,
        }
    except Exception as e:
        # 套餐限制的币种静默降级，不写日志防止刷屏
        err_str = str(e).lower()
        if not any(k in err_str for k in ("upgrade", "plan", "401", "402")):
            _cg_logger.warning(f"[get_signal_for_open] {symbol} {direction} 失败: {e}")
        return {
            "score_adjust": 0, "heatmap_score": 0, "deriv_score": 0,
            "bias": "NEUTRAL", "reason": f"信号获取失败: {e}",
            "tp1": 0, "tp2": 0, "sl": 0, "zones": {}, "deriv_sig": {}, "resist": 0,
        }


def adjust_score_by_cg(symbol: str, direction: str, current_price: float,
                        details: dict) -> int:
    """供 fx/scanner.py 调用，向 details 写入 CG 衍生品分析结果（兼容原版签名）"""
    try:
        result = get_signal_for_open(symbol, direction, current_price)
        details["cg_heatmap"]   = result["heatmap_score"]
        details["cg_deriv"]     = result["deriv_score"]
        details["cg_zones"]     = result.get("zones", {})
        details["cg_bias"]      = result.get("bias", "neutral")
        details["cg_reason"]    = result.get("reason", "")
        details["cg_deriv_sig"] = result.get("deriv_sig", {})
        return result["score_adjust"]
    except Exception as e:
        _cg_logger.warning(f"[adjust_score_by_cg] {symbol} 失败: {e}")
        return 0


# ── score_symbol_cg_bonus（paper_scanner 调用入口，原在根目录旧版里）────────────
def score_symbol_cg_bonus(symbol: str, direction: str,
                           current_price: float, details: dict) -> int:
    """
    供 paper_scanner / paper_scanner_test 的 score_symbol_cg_bonus() 调用。
    等同于 adjust_score_by_cg，统一接口签名。
    """
    try:
        result = get_signal_for_open(symbol, direction, current_price)
        details["cg_bonus"]     = result["score_adjust"]
        details["cg_heatmap"]   = result["heatmap_score"]
        details["cg_deriv"]     = result["deriv_score"]
        details["cg_zones"]     = result.get("zones", {})
        details["cg_bias"]      = result.get("bias", "neutral")
        details["cg_reason"]    = result.get("reason", "")
        details["cg_deriv_sig"] = result.get("deriv_sig", {})
        return result["score_adjust"]
    except Exception as e:
        _cg_logger.warning(f"[score_symbol_cg_bonus] {symbol} 失败: {e}")
        return 0
