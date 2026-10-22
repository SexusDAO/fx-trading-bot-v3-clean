"""
backtest_stop_hunt.py — stop_hunt 纯形态回测（bypass 强平量验证）

用法:
    python3 backtest_stop_hunt.py XRPUSDT 60
    python3 backtest_stop_hunt.py BNBUSDT 60

逻辑:
  - 每根 1H K线闭合后模拟一次 judge_stop_hunt 扫描
  - 强平量验证临时 bypass（ind['_ws_listener'] = None，降级直接通过）
  - chg24 限制保留（过滤极端行情）
  - 4H EMA 限制保留（strategy_adj 软惩罚，不影响触发）
  - 出场规则：SL止损 / TP1（1.5R）/ 最长持有 8H
  - 输出：每笔信号明细 + 汇总胜率
"""

import sys, os, requests
sys.path.insert(0, os.path.dirname(__file__))

from shared import binance_api as api
from shared.judges import judge_stop_hunt
from scanner import calc_indicators
from config import PROXY_URL, BINANCE_BASE_URL
from datetime import datetime, timezone, timedelta

proxies = {'https': PROXY_URL, 'http': PROXY_URL} if PROXY_URL else {}

def ts(k):
    return datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc)

def get_klines_range(sym, interval, start_dt, end_dt):
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int(end_dt.timestamp()   * 1000)
    url = f"{BINANCE_BASE_URL}/fapi/v1/klines"
    all_k = []
    while True:
        r = requests.get(url, params={
            'symbol': sym, 'interval': interval,
            'startTime': start_ms, 'endTime': end_ms, 'limit': 1500
        }, proxies=proxies, timeout=20)
        batch = r.json()
        if not batch or not isinstance(batch, list):
            break
        all_k.extend(batch)
        if len(batch) < 1500:
            break
        start_ms = int(batch[-1][0]) + 1
    return all_k


def run_backtest(symbol: str, days: int = 60):
    print(f"\n{'='*65}")
    print(f"  {symbol}  stop_hunt 纯形态回测  {days}天  (bypass 强平量验证)")
    print(f"{'='*65}")

    end_dt   = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=days + 5)   # 多 5 天做 warmup

    print(f"拉取数据 {start_dt.strftime('%Y-%m-%d')} ~ {end_dt.strftime('%Y-%m-%d')} ...")
    k1h_all = get_klines_range(symbol, '1h',  start_dt, end_dt)
    k15_all = get_klines_range(symbol, '15m', start_dt, end_dt)
    k4h_all = get_klines_range(symbol, '4h',  start_dt, end_dt)
    print(f"  k1h={len(k1h_all)} k15={len(k15_all)} k4h={len(k4h_all)}")

    # 回测起点：warmup 后第 5 天（需要足够 1H 历史给 Fix1/Fix3）
    backtest_start = start_dt + timedelta(days=5)

    signals = []
    last_signal_ts = 0   # 防止同方向 1H 内重复触发

    # 逐根 1H K线扫描（用闭合K线截片）
    for i in range(len(k1h_all)):
        t_bar = ts(k1h_all[i])
        if t_bar < backtest_start:
            continue

        # 截片：模拟该 1H 刚闭合时的视角
        k1h = k1h_all[:i + 1]          # 含当前闭合根
        if len(k1h) < 50:
            continue

        # 找对应的 k15 截片（当前 1H 结束时刻的 15M）
        bar_end_ms = int(k1h_all[i][0]) + 3600_000
        k15_idx = next((j for j, k in enumerate(k15_all)
                        if int(k[0]) >= bar_end_ms), None)
        if k15_idx is None:
            continue
        k15 = k15_all[:k15_idx + 1]    # +1：末根未闭合

        # k4h 截片
        k4h_idx = next((j for j, k in enumerate(k4h_all)
                        if int(k[0]) >= bar_end_ms), None)
        k4h = k4h_all[:k4h_idx] if k4h_idx else k4h_all

        price = float(k1h[-1][4])

        ind = calc_indicators(k15, k1h, k4h)
        ind['price']        = price
        ind['_ws_listener'] = None   # bypass 强平量验证
        ind['btc_trend']    = 'neutral'

        chg24 = (price - float(k1h[-25][4])) / float(k1h[-25][4]) * 100 \
                if len(k1h) >= 25 else 0.0

        try:
            ok, detail = judge_stop_hunt(k1h, k4h, k15, ind,
                                         chg24=chg24, funding=0.0)
        except Exception as e:
            continue

        if not ok:
            continue

        direction    = detail['direction']
        pivot_level  = detail['pivot_level']
        pierce_pct   = detail['pierce_pct']
        vol_ratio    = detail['vol_ratio']
        recover_bars = detail['recover_bars']
        sl_hint      = detail['sl_price_hint']
        strength     = detail['signal_strength']
        strategy_adj = detail.get('strategy_adj', 0)

        # 冷却：同方向 4H 内不重复（避免同一段行情刷多笔）
        bar_ts = int(k1h_all[i][0]) / 1000
        if bar_ts - last_signal_ts < 4 * 3600:
            continue
        last_signal_ts = bar_ts

        # ── 出场模拟 ────────────────────────────────────────────────────
        # 入场价 = 当前 1H 收盘价（下一根开盘时成交）
        entry = price
        sl    = sl_hint if sl_hint else (
            entry * 0.97 if direction == 'LONG' else entry * 1.03
        )
        sl_dist = abs(entry - sl)
        tp1   = entry + sl_dist * 1.5 if direction == 'LONG' else entry - sl_dist * 1.5

        # 在后续最多 8H 的 1H K线里找出场
        exit_price = None
        exit_type  = None
        exit_time  = None

        future_k1h = k1h_all[i + 1: i + 9]   # 后续 8 根 1H
        for fk in future_k1h:
            fh = float(fk[2])
            fl = float(fk[3])
            fc = float(fk[4])
            ft = ts(fk)

            if direction == 'LONG':
                if fl <= sl:
                    exit_price, exit_type, exit_time = sl, 'SL', ft
                    break
                if fh >= tp1:
                    exit_price, exit_type, exit_time = tp1, 'TP1', ft
                    break
            else:  # SHORT
                if fh >= sl:
                    exit_price, exit_type, exit_time = sl, 'SL', ft
                    break
                if fl <= tp1:
                    exit_price, exit_type, exit_time = tp1, 'TP1', ft
                    break

        if exit_price is None and future_k1h:
            # 时间止损：8H 后以最后一根收盘价出场
            last_fk = future_k1h[-1]
            exit_price = float(last_fk[4])
            exit_type  = 'TIME'
            exit_time  = ts(last_fk)

        if exit_price is None:
            continue

        # 盈亏（以 100 USDT 名义仓位计算）
        notional = 100.0
        if direction == 'LONG':
            pnl = (exit_price - entry) / entry * notional
        else:
            pnl = (entry - exit_price) / entry * notional

        signals.append({
            'time':         t_bar.strftime('%m-%d %H:%M'),
            'direction':    direction,
            'pivot':        round(pivot_level, 4),
            'pierce_pct':   round(pierce_pct, 2),
            'vol_ratio':    round(vol_ratio, 2),
            'recover_bars': recover_bars,
            'strength':     strength,
            'strategy_adj': strategy_adj,
            'entry':        round(entry, 4),
            'sl':           round(sl, 4),
            'tp1':          round(tp1, 4),
            'exit_price':   round(exit_price, 4),
            'exit_type':    exit_type,
            'exit_time':    exit_time.strftime('%m-%d %H:%M') if exit_time else '-',
            'pnl_u':        round(pnl, 2),
        })

    # ── 输出 ────────────────────────────────────────────────────────────
    if not signals:
        print(f"\n  ⚠️  {days}天内未发现任何 stop_hunt 信号")
        return

    print(f"\n共发现 {len(signals)} 笔信号\n")
    header = f"{'时间':12s} {'方向':5s} {'前高/低':8s} {'刺穿%':6s} {'量比':5s} {'收回根':6s} {'强度':6s} {'adj':4s} {'入场':8s} {'出场':8s} {'出场类型':6s} {'盈亏U':6s}"
    print(header)
    print('-' * len(header))

    wins = losses = adj_neg = 0
    total_pnl = 0.0
    for s in signals:
        marker = ' ★' if s['strength'] == 'strong' else ''
        adj_m  = f"[adj{s['strategy_adj']}]" if s['strategy_adj'] != 0 else ''
        print(
            f"{s['time']:12s} {s['direction']:5s} {s['pivot']:8.4f} "
            f"{s['pierce_pct']:6.2f}% {s['vol_ratio']:5.2f}x {s['recover_bars']:3d}根  "
            f"{s['strength']:6s} {adj_m:8s} "
            f"{s['entry']:8.4f} {s['exit_price']:8.4f} "
            f"{s['exit_type']:6s}  {s['pnl_u']:+.2f}U{marker}"
        )
        total_pnl += s['pnl_u']
        if s['pnl_u'] > 0:
            wins += 1
        else:
            losses += 1
        if s['strategy_adj'] < 0:
            adj_neg += 1

    total = wins + losses
    winrate = wins / total * 100 if total else 0
    print(f"\n{'─'*65}")
    print(f"  总笔数: {total}   胜: {wins}   负: {losses}")
    print(f"  胜率:   {winrate:.1f}%")
    print(f"  总盈亏: {total_pnl:+.2f}U  (100U 名义仓位)")
    print(f"  逆势信号(adj<0): {adj_neg} 笔")

    if total > 0:
        wins_strong   = sum(1 for s in signals if s['strength'] == 'strong' and s['pnl_u'] > 0)
        total_strong  = sum(1 for s in signals if s['strength'] == 'strong')
        wins_medium   = sum(1 for s in signals if s['strength'] == 'medium' and s['pnl_u'] > 0)
        total_medium  = sum(1 for s in signals if s['strength'] == 'medium')
        print(f"\n  strong 信号: {total_strong}笔  胜率 {wins_strong/total_strong*100:.1f}%" if total_strong else "  strong: 0笔")
        print(f"  medium 信号: {total_medium}笔  胜率 {wins_medium/total_medium*100:.1f}%" if total_medium else "  medium: 0笔")

    print()
    return signals


if __name__ == '__main__':
    symbol = sys.argv[1] if len(sys.argv) > 1 else 'XRPUSDT'
    days   = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    run_backtest(symbol, days)
