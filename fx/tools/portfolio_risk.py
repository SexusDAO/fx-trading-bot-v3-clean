#!/usr/bin/env python3
"""
tools/portfolio_risk.py — 组合 Beta / Delta 每日快照

功能：
  - 计算整体持仓对 BTC 的 Beta（敏感度）
  - 估算 BTC 跌10%的账户亏损
  - 实时告警：Beta > 2 或单仓集中度 > 50%

调用方式：
    from tools.portfolio_risk import calc_portfolio_risk, run
    result = calc_portfolio_risk()   # 实时计算
    run()                            # 从 archiver / main.py 调用，含告警
"""
from __future__ import annotations
import logging
import time
from pathlib import Path
import sys

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

logger = logging.getLogger("fx.portfolio_risk")


def _get_daily_returns(symbol: str, days: int = 30) -> list[float]:
    """拉过去 N 天的日收益率（1D 收盘价变化率），失败返回 []。"""
    try:
        from shared import binance_api as api
        klines = api.get_klines(symbol, "1d", limit=days + 2)
        closes = [float(k[4]) for k in klines]
        if len(closes) < 2:
            return []
        returns = [
            (closes[i] - closes[i - 1]) / closes[i - 1]
            for i in range(1, len(closes))
        ]
        return returns[-days:]
    except Exception as e:
        logger.debug(f"[portfolio_risk] {symbol} 日收益率获取失败: {e}")
        return []


def _covariance(x: list[float], y: list[float]) -> float:
    n = min(len(x), len(y))
    if n < 2:
        return 0.0
    x, y = x[:n], y[:n]
    mx, my = sum(x) / n, sum(y) / n
    return sum((x[i] - mx) * (y[i] - my) for i in range(n)) / (n - 1)


def _variance(x: list[float]) -> float:
    n = len(x)
    if n < 2:
        return 0.0
    mx = sum(x) / n
    return sum((v - mx) ** 2 for v in x) / (n - 1)


def _pearson(x: list[float], y: list[float]) -> float:
    cov = _covariance(x, y)
    sx = _variance(x) ** 0.5
    sy = _variance(y) ** 0.5
    if sx < 1e-9 or sy < 1e-9:
        return 0.0
    return cov / (sx * sy)


def calc_portfolio_risk(days: int = 30) -> dict:
    """
    计算组合 Beta/Delta。

    返回:
    {
        "portfolio_beta":        float,   # 组合 Beta 值（相对 BTC）
        "btc_10pct_drop_loss_pct": float, # BTC 跌10% 的预估亏损%
        "net_direction":         str,     # LONG / SHORT / NEUTRAL
        "max_single_position_pct": float, # 最大单仓权重%
        "total_notional":        float,   # 总名义仓位 U
        "equity":                float,   # 账户权益 U
        "correlation_matrix":    dict,    # 标的对相关性（N≤4才计算）
        "positions":             list,    # 各仓位明细
        "error":                 str,     # 异常时有值
    }
    """
    try:
        from shared.state import get_all_positions
        from shared import binance_api as api
        from config import PORTFOLIO_BETA_WARN_THRESH, PORTFOLIO_SINGLE_POS_MAX_PCT

        positions = [p for p in get_all_positions() if p.get("status") == "open"]
        if not positions:
            return {"portfolio_beta": 0.0, "btc_10pct_drop_loss_pct": 0.0,
                    "net_direction": "NEUTRAL", "max_single_position_pct": 0.0,
                    "total_notional": 0.0, "equity": 0.0,
                    "correlation_matrix": {}, "positions": []}

        # 账户权益
        try:
            acc  = api.get_account()
            equity = float(acc.get("totalMarginBalance", 0) or 0)
        except Exception:
            equity = 1.0

        # BTC 日收益率
        btc_returns = _get_daily_returns("BTCUSDT", days)
        if not btc_returns:
            return {"error": "无法获取 BTC 日收益率"}

        btc_var = _variance(btc_returns)
        if btc_var < 1e-12:
            return {"error": "BTC 方差过小，无法计算 Beta"}

        # 各仓位名义价值
        pos_data = []
        for pos in positions:
            qty     = float(pos.get("quantity", 0) or 0)
            entry_p = float(pos.get("entry_price", 0) or 0)
            notional = qty * entry_p
            direction_sign = 1.0 if pos.get("direction", "long").upper() == "LONG" else -1.0
            pos_data.append({
                "symbol":    pos["symbol"],
                "direction": pos.get("direction", "long").upper(),
                "notional":  notional,
                "direction_sign": direction_sign,
            })

        total_notional = sum(p["notional"] for p in pos_data)
        if total_notional <= 0:
            return {"error": "总名义仓位为零"}

        # 计算各仓位 Beta，加权汇总组合 Beta
        portfolio_beta = 0.0
        pos_returns_map: dict[str, list[float]] = {}

        for pd_ in pos_data:
            symbol = pd_["symbol"]
            if symbol == "BTCUSDT":
                beta = 1.0
            else:
                returns = _get_daily_returns(symbol, days)
                if len(returns) < 5:
                    # 数据不足时按高 Beta 保守估计
                    logger.debug(f"[portfolio_risk] {symbol} 数据不足，Beta 用 1.2 近似")
                    beta = 1.2
                else:
                    cov  = _covariance(returns, btc_returns[-len(returns):])
                    beta = cov / btc_var
                    pos_returns_map[symbol] = returns

            weight   = pd_["notional"] / total_notional
            portfolio_beta += beta * weight * pd_["direction_sign"]
            pd_["beta"] = round(beta, 4)
            pd_["weight_pct"] = round(weight * 100, 2)

        # 组合 Delta：BTC 跌10%时账户亏损
        btc_drop_10pct = 0.10
        btc_drop_loss_notional = portfolio_beta * btc_drop_10pct * total_notional
        btc_10pct_drop_loss_pct = (
            -btc_drop_loss_notional / equity * 100 if equity > 0 else 0.0
        )

        # 净方向
        if portfolio_beta > 0.1:
            net_direction = "LONG"
        elif portfolio_beta < -0.1:
            net_direction = "SHORT"
        else:
            net_direction = "NEUTRAL"

        # 最大单仓权重
        max_single_pct = max(pd_["weight_pct"] for pd_ in pos_data)

        # 相关性矩阵（N≤6 才计算）
        corr_matrix: dict = {}
        syms_with_returns = [s for s in pos_returns_map if pos_returns_map[s]]
        if 2 <= len(syms_with_returns) <= 6:
            for i, s1 in enumerate(syms_with_returns):
                for s2 in syms_with_returns[i + 1:]:
                    r1, r2 = pos_returns_map[s1], pos_returns_map[s2]
                    n = min(len(r1), len(r2))
                    if n >= 5:
                        corr = round(_pearson(r1[-n:], r2[-n:]), 3)
                        corr_matrix[f"{s1}-{s2}"] = corr

        result = {
            "portfolio_beta":          round(portfolio_beta, 4),
            "btc_10pct_drop_loss_pct": round(btc_10pct_drop_loss_pct, 2),
            "net_direction":           net_direction,
            "max_single_position_pct": round(max_single_pct, 2),
            "total_notional":          round(total_notional, 2),
            "equity":                  round(equity, 2),
            "correlation_matrix":      corr_matrix,
            "positions":               [
                {k: v for k, v in pd_.items()}
                for pd_ in pos_data
            ],
        }
        return result

    except Exception as e:
        logger.warning(f"[portfolio_risk] calc_portfolio_risk 失败: {e}")
        return {"error": str(e)}


def run(notifier_fn=None) -> dict:
    """
    计算并检查告警阈值，发出告警通知（try/except 全包，不影响主策略）。
    notifier_fn: 同步通知函数 notify(text)，传 None 时只写日志。
    返回 calc_portfolio_risk() 结果。
    """
    try:
        from config import PORTFOLIO_BETA_WARN_THRESH, PORTFOLIO_SINGLE_POS_MAX_PCT
    except Exception:
        PORTFOLIO_BETA_WARN_THRESH  = 2.0
        PORTFOLIO_SINGLE_POS_MAX_PCT = 0.50

    result = calc_portfolio_risk()
    if "error" in result:
        logger.debug(f"[portfolio_risk] 跳过告警，计算失败: {result['error']}")
        return result

    beta     = result.get("portfolio_beta", 0.0)
    max_pct  = result.get("max_single_position_pct", 0.0)
    drop_pct = result.get("btc_10pct_drop_loss_pct", 0.0)

    alerts = []
    if abs(beta) >= PORTFOLIO_BETA_WARN_THRESH:
        alerts.append(
            f"⚠️ 组合 Beta={beta:.2f}，BTC 跌10%预估亏损 {abs(drop_pct):.1f}%"
        )
    if max_pct >= PORTFOLIO_SINGLE_POS_MAX_PCT * 100:
        alerts.append(
            f"⚠️ 单仓集中度过高：最大单仓占比 {max_pct:.1f}%（阈值{PORTFOLIO_SINGLE_POS_MAX_PCT*100:.0f}%）"
        )

    for alert in alerts:
        logger.warning(f"[portfolio_risk] {alert}")
        if notifier_fn:
            try:
                notifier_fn(alert)
            except Exception:
                pass

    logger.info(
        f"[portfolio_risk] Beta={beta:.2f} MaxPos={max_pct:.1f}% "
        f"BTC10pctDrop={drop_pct:.1f}% Equity={result.get('equity',0):.0f}U"
    )
    return result


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    r = calc_portfolio_risk()
    print(json.dumps(r, ensure_ascii=False, indent=2))
