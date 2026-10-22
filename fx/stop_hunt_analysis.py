#!/usr/bin/env python3
"""
Stop Hunt Statistics Analysis
- 高波动: BTWUSDT, RAVEUSDT — 找各自成交量最大的连续30天
- 中波动: SOLUSDT, HYPEUSDT — 2026-07-01 ~ 2026-10-18
- 低波动: BTCUSDT, ETHUSDT  — 2026-07-01 ~ 2026-10-18
"""

from pathlib import Path
import sys, requests, json, math
from datetime import datetime, timezone, timedelta
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import PROXY_URL, BINANCE_BASE_URL

proxies = {'https': PROXY_URL, 'http': PROXY_URL} if PROXY_URL else {}

# ─── Data Fetching ───────────────────────────────────────────────────────────

def get_klines(sym, interval, start_dt, end_dt):
    start_ms = int(start_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms   = int(end_dt.replace(tzinfo=timezone.utc).timestamp()   * 1000)
    url = f"{BINANCE_BASE_URL}/fapi/v1/klines"
    all_k = []
    while True:
        r = requests.get(url, params={'symbol': sym, 'interval': interval,
            'startTime': start_ms, 'endTime': end_ms, 'limit': 1500},
            proxies=proxies, timeout=30)
        batch = r.json()
        if not batch or not isinstance(batch, list):
            break
        all_k.extend(batch)
        if len(batch) < 1500:
            break
        start_ms = int(batch[-1][0]) + 1
    return all_k

def get_daily_klines(sym, start_dt, end_dt):
    """Returns list of (open_time_ms, volume_float)"""
    raw = get_klines(sym, '1d', start_dt, end_dt)
    return [(int(k[0]), float(k[5])) for k in raw]

def get_1h_klines(sym, start_dt, end_dt):
    raw = get_klines(sym, '1h', start_dt, end_dt)
    # [open_time, open, high, low, close, volume, close_time, ...]
    return raw

def get_15m_klines(sym, start_dt, end_dt):
    raw = get_klines(sym, '15m', start_dt, end_dt)
    return raw

# ─── Find best 30-day window ─────────────────────────────────────────────────

def find_best_30d_window(sym):
    """Find the 30-day window with highest total volume."""
    print(f"  Fetching daily klines for {sym}...")
    # Fetch from beginning of data (try 2 years back)
    end_dt   = datetime(2026, 10, 18)
    start_dt = datetime(2024, 1, 1)
    daily = get_daily_klines(sym, start_dt, end_dt)
    if len(daily) < 30:
        print(f"  WARNING: only {len(daily)} daily bars for {sym}")
        return None, None, None
    
    best_sum = -1
    best_i   = 0
    vols = [v for _, v in daily]
    for i in range(len(daily) - 29):
        s = sum(vols[i:i+30])
        if s > best_sum:
            best_sum = s
            best_i   = i
    
    start_ms = daily[best_i][0]
    end_ms   = daily[best_i + 29][0]
    best_start = datetime.utcfromtimestamp(start_ms / 1000)
    best_end   = datetime.utcfromtimestamp(end_ms   / 1000)
    avg_daily_vol = best_sum / 30
    
    print(f"  {sym} best window: {best_start.date()} ~ {best_end.date()} (avg daily vol {avg_daily_vol/1000:.1f}k)")
    return best_start, best_end + timedelta(days=1), avg_daily_vol

# ─── Find pivot high/low in 1H closes ────────────────────────────────────────

def find_prev_pivots(recent_1h_closes):
    """
    From last 20 closed 1H candles, find:
    - prev_high: global max close that has 3 bars lower on each side (within the 20)
    - prev_low:  global min close that has 3 bars lower on each side (within the 20)
    Returns (prev_low, prev_high) or (None, None) if not found
    """
    closes = recent_1h_closes  # length ~20
    n = len(closes)
    if n < 7:
        return None, None
    
    best_high = None
    best_low  = None
    
    for i in range(3, n - 3):
        c = closes[i]
        # Check pivot high: all 3 bars on each side are lower
        if all(closes[i-j] < c for j in range(1, 4)) and all(closes[i+j] < c for j in range(1, 4)):
            if best_high is None or c > best_high:
                best_high = c
        # Check pivot low: all 3 bars on each side are higher
        if all(closes[i-j] > c for j in range(1, 4)) and all(closes[i+j] > c for j in range(1, 4)):
            if best_low is None or c < best_low:
                best_low = c
    
    return best_low, best_high

# ─── Core stop-hunt event detection ─────────────────────────────────────────

def detect_stop_hunts(sym, start_dt, end_dt):
    """
    Detect stop-hunt (spike through pivot then revert) events.
    Returns list of event dicts.
    """
    print(f"  Fetching 1H klines for {sym}...")
    # Need extra lookback for pivot detection: 20 1H bars before start
    lookback_start = start_dt - timedelta(hours=24)
    raw_1h = get_1h_klines(sym, lookback_start, end_dt + timedelta(hours=4))
    print(f"  Got {len(raw_1h)} 1H bars")
    
    if not raw_1h:
        print(f"  ERROR: no 1H data for {sym}")
        return []
    
    print(f"  Fetching 15M klines for {sym}...")
    raw_15m = get_15m_klines(sym, lookback_start, end_dt + timedelta(hours=4))
    print(f"  Got {len(raw_15m)} 15M bars")
    
    if not raw_15m:
        print(f"  ERROR: no 15M data for {sym}")
        return []
    
    # Parse 1H bars
    h1_bars = []
    for k in raw_1h:
        h1_bars.append({
            'open_ms':  int(k[0]),
            'close_ms': int(k[6]),
            'open':     float(k[1]),
            'high':     float(k[2]),
            'low':      float(k[3]),
            'close':    float(k[4]),
            'vol':      float(k[5]),
        })
    
    # Parse 15M bars
    m15_bars = []
    for k in raw_15m:
        m15_bars.append({
            'open_ms':  int(k[0]),
            'close_ms': int(k[6]),
            'open':     float(k[1]),
            'high':     float(k[2]),
            'low':      float(k[3]),
            'close':    float(k[4]),
            'vol':      float(k[5]),
        })
    
    # Build index: for each 15M bar, get the 20 most recent CLOSED 1H bars
    start_ms = int(start_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms   = int(end_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    
    events = []
    used_pivots = {}  # pivot_level -> last event open_ms (for dedup: 1H period)
    
    for i, bar in enumerate(m15_bars):
        # Only process bars within the target date range
        if bar['open_ms'] < start_ms or bar['open_ms'] >= end_ms:
            continue
        
        # Get 20 most recent CLOSED 1H bars before this 15M bar's open time
        # A 1H bar is "closed" if its close_ms < bar['open_ms']
        closed_1h = [h for h in h1_bars if h['close_ms'] < bar['open_ms']]
        if len(closed_1h) < 20:
            continue
        recent_1h = closed_1h[-20:]
        recent_1h_closes = [h['close'] for h in recent_1h]
        
        prev_low, prev_high = find_prev_pivots(recent_1h_closes)
        
        # Volume: past 20 15M bars (closed before this bar)
        prev_15m = [b for b in m15_bars if b['close_ms'] < bar['open_ms']]
        if len(prev_15m) < 20:
            continue
        recent_20_vols = [b['vol'] for b in prev_15m[-20:]]
        avg_vol = sum(recent_20_vols) / 20 if recent_20_vols else 0
        if avg_vol == 0:
            continue
        
        vol_mult = bar['vol'] / avg_vol
        
        # Check bullish spike (long stop hunt): low pierces prev_low
        if prev_low is not None and bar['low'] <= prev_low * 0.997 and vol_mult >= 1.5:
            pierce_pct = (prev_low - bar['low']) / prev_low * 100
            
            # Dedup: same prev_low level, within same 1H window
            level_key = f"low_{round(prev_low, 8)}"
            bar_1h_slot = bar['open_ms'] // (3600 * 1000)
            if level_key in used_pivots and used_pivots[level_key] == bar_1h_slot:
                pass  # skip duplicate
            else:
                used_pivots[level_key] = bar_1h_slot
                
                # Look for revert: next 8 bars close back above prev_low
                future_bars = [b for b in m15_bars if b['open_ms'] > bar['open_ms']][:8]
                reverted = None
                for j, fb in enumerate(future_bars):
                    if fb['close'] >= prev_low:
                        reverted = j + 1
                        break
                
                # Post-revert: 4H move (16 bars after pierce bar)
                all_future = [b for b in m15_bars if b['open_ms'] > bar['open_ms']]
                post_bars = all_future[:16]
                post_change = None
                if post_bars:
                    post_change = (post_bars[-1]['close'] - bar['close']) / bar['close'] * 100
                
                events.append({
                    'type':       'bull',
                    'bar_ms':     bar['open_ms'],
                    'pierce_pct': pierce_pct,
                    'reverted':   reverted,
                    'vol_mult':   vol_mult,
                    'post_change': post_change,
                    'prev_level': prev_low,
                })
        
        # Check bearish spike (short stop hunt): high pierces prev_high
        if prev_high is not None and bar['high'] >= prev_high * 1.003 and vol_mult >= 1.5:
            pierce_pct = (bar['high'] - prev_high) / prev_high * 100
            
            level_key = f"high_{round(prev_high, 8)}"
            bar_1h_slot = bar['open_ms'] // (3600 * 1000)
            if level_key in used_pivots and used_pivots[level_key] == bar_1h_slot:
                pass
            else:
                used_pivots[level_key] = bar_1h_slot
                
                future_bars = [b for b in m15_bars if b['open_ms'] > bar['open_ms']][:8]
                reverted = None
                for j, fb in enumerate(future_bars):
                    if fb['close'] <= prev_high:
                        reverted = j + 1
                        break
                
                all_future = [b for b in m15_bars if b['open_ms'] > bar['open_ms']]
                post_bars = all_future[:16]
                post_change = None
                if post_bars:
                    post_change = (bar['close'] - post_bars[-1]['close']) / bar['close'] * 100
                
                events.append({
                    'type':       'bear',
                    'bar_ms':     bar['open_ms'],
                    'pierce_pct': pierce_pct,
                    'reverted':   reverted,
                    'vol_mult':   vol_mult,
                    'post_change': post_change,
                    'prev_level': prev_high,
                })
    
    return events

# ─── Statistics computation ───────────────────────────────────────────────────

def pct(x, total):
    if total == 0: return 0
    return x / total * 100

def stats_block(events):
    """Compute and return stats dict for a list of events."""
    n = len(events)
    reverted_events = [e for e in events if e['reverted'] is not None]
    not_reverted    = [e for e in events if e['reverted'] is None]
    
    n_rev = len(reverted_events)
    n_not = len(not_reverted)
    
    pierce_pcts  = [e['pierce_pct'] for e in events]
    revert_times = [e['reverted']   for e in reverted_events]
    vol_mults    = [e['vol_mult']   for e in events]
    
    # Post-revert directional accuracy
    correct = 0
    post_valid = 0
    for e in events:
        if e['post_change'] is not None:
            post_valid += 1
            # bull: expect price to go up (positive change), bear: expect price to go down (we stored as positive)
            if e['post_change'] > 0:
                correct += 1
    
    def safe_stats(arr):
        if not arr:
            return {'mean': float('nan'), 'median': float('nan'), 'p90': float('nan')}
        a = np.array(arr, dtype=float)
        return {
            'mean':   float(np.mean(a)),
            'median': float(np.median(a)),
            'p90':    float(np.percentile(a, 90)),
        }
    
    return {
        'total':       n,
        'reverted':    n_rev,
        'not_reverted': n_not,
        'pierce_stats':  safe_stats(pierce_pcts),
        'revert_stats':  safe_stats(revert_times),
        'vol_stats':     safe_stats(vol_mults),
        'post_accuracy': pct(correct, post_valid) if post_valid > 0 else float('nan'),
    }

def format_block(title, symbols_info, events_list):
    """Format one volatility tier block."""
    lines = []
    lines.append(f"=== {title} ===")
    for sym, info in symbols_info.items():
        lines.append(f"{sym}: {info}")
    
    all_events = []
    for evs in events_list:
        all_events.extend(evs)
    
    st = stats_block(all_events)
    n  = st['total']
    nr = st['reverted']
    nn = st['not_reverted']
    
    lines.append(f"总事件数: {n}")
    lines.append(f"  其中已收回事件: {nr} ({pct(nr, n):.0f}%)")
    lines.append(f"  其中未收回(>8根): {nn} ({pct(nn, n):.0f}%)")
    lines.append("")
    
    ps = st['pierce_stats']
    rs = st['revert_stats']
    vs = st['vol_stats']
    
    def fmt(v):
        if math.isnan(v): return "N/A"
        return f"{v:.2f}"
    def fmti(v):
        if math.isnan(v): return "N/A"
        return f"{v:.1f}"
    
    lines.append(f"刺穿幅度%:   均值={fmt(ps['mean'])}  中位={fmt(ps['median'])}  90分位={fmt(ps['p90'])}")
    lines.append(f"收回时间(根): 均值={fmti(rs['mean'])}   中位={fmt(rs['median'])}     90分位={fmt(rs['p90'])}")
    lines.append(f"量能倍数:    均值={fmt(vs['mean'])}  中位={fmt(vs['median'])}  90分位={fmt(vs['p90'])}")
    
    acc = st['post_accuracy']
    acc_str = f"{acc:.0f}%" if not math.isnan(acc) else "N/A"
    lines.append(f"后续走势正确率: {acc_str} (收回后4H价格延续收回方向)")
    lines.append("")
    
    return "\n".join(lines)

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    output_lines = []
    
    # ── 高波动: BTWUSDT + RAVEUSDT ──────────────────────────────────────────
    print("\n=== 高波动层: BTWUSDT + RAVEUSDT ===")
    
    high_info = {}
    high_events = []
    
    for sym in ['BTWUSDT', 'RAVEUSDT']:
        print(f"\n[{sym}] Finding best 30-day window...")
        best_start, best_end, avg_vol = find_best_30d_window(sym)
        if best_start is None:
            print(f"  Skipping {sym}: insufficient data")
            high_info[sym] = "数据不足，已跳过"
            high_events.append([])
            continue
        
        short = sym.replace('USDT', '')
        high_info[sym] = f"最佳30天: {best_start.date()} ~ {(best_end - timedelta(days=1)).date()}  (日均成交量 {avg_vol/1000:.1f}k)"
        
        print(f"[{sym}] Detecting stop hunts {best_start.date()} ~ {(best_end - timedelta(days=1)).date()}...")
        events = detect_stop_hunts(sym, best_start, best_end)
        print(f"[{sym}] Found {len(events)} events")
        high_events.append(events)
    
    block_high = format_block("高波动层（BTWUSDT + RAVEUSDT）", high_info, high_events)
    output_lines.append(block_high)
    print(block_high)
    
    # ── 中波动: SOLUSDT + HYPEUSDT ──────────────────────────────────────────
    print("\n=== 中波动层: SOLUSDT + HYPEUSDT ===")
    mid_start = datetime(2026, 7, 1)
    mid_end   = datetime(2026, 10, 18)
    
    mid_info   = {}
    mid_events = []
    
    for sym in ['SOLUSDT', 'HYPEUSDT']:
        print(f"\n[{sym}] Detecting stop hunts {mid_start.date()} ~ {mid_end.date()}...")
        try:
            events = detect_stop_hunts(sym, mid_start, mid_end)
            print(f"[{sym}] Found {len(events)} events")
        except Exception as ex:
            print(f"[{sym}] ERROR: {ex}")
            events = []
            mid_info[sym] = f"获取失败: {ex}"
        mid_events.append(events)
        if sym not in mid_info:
            mid_info[sym] = f"{mid_start.date()} ~ {mid_end.date()}"
    
    block_mid = format_block("中波动层（SOLUSDT + HYPEUSDT）", mid_info, mid_events)
    output_lines.append(block_mid)
    print(block_mid)
    
    # ── 低波动: BTCUSDT + ETHUSDT ──────────────────────────────────────────
    print("\n=== 低波动层: BTCUSDT + ETHUSDT ===")
    low_start = datetime(2026, 7, 1)
    low_end   = datetime(2026, 10, 18)
    
    low_info   = {}
    low_events = []
    
    for sym in ['BTCUSDT', 'ETHUSDT']:
        print(f"\n[{sym}] Detecting stop hunts {low_start.date()} ~ {low_end.date()}...")
        try:
            events = detect_stop_hunts(sym, low_start, low_end)
            print(f"[{sym}] Found {len(events)} events")
        except Exception as ex:
            print(f"[{sym}] ERROR: {ex}")
            events = []
            low_info[sym] = f"获取失败: {ex}"
        low_events.append(events)
        if sym not in low_info:
            low_info[sym] = f"{low_start.date()} ~ {low_end.date()}"
    
    block_low = format_block("低波动层（BTCUSDT + ETHUSDT）", low_info, low_events)
    output_lines.append(block_low)
    print(block_low)
    
    # ── Write output ─────────────────────────────────────────────────────────
    out_path = str(Path(__file__).resolve().parent / 'stop_hunt_stats.txt')
    full_output = "\n".join(output_lines)
    with open(out_path, 'w') as f:
        f.write(full_output)
    print(f"\n✅ Results written to {out_path}")
    print("\n=== FINAL OUTPUT ===")
    print(full_output)

if __name__ == '__main__':
    main()
