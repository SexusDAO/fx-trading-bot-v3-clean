#!/usr/bin/env python3
"""一次性工作流：账户检查 → 候选池刷新 → 实盘扫描 → 开仓"""
import sys, os, time, logging
# 确保只用 fx/ 目录下的模块
_FX = os.path.dirname(os.path.abspath(__file__))
sys.path = [_FX] + [p for p in sys.path if p != os.path.dirname(_FX)]

logging.basicConfig(level=logging.WARNING, format='%(levelname)s %(name)s: %(message)s')
logging.getLogger('fx.executor').setLevel(logging.INFO)
logging.getLogger('fx.position_manager').setLevel(logging.INFO)
for n in ['urllib3','requests','fx.clawby','fx.liq_cache']:
    logging.getLogger(n).setLevel(logging.ERROR)

from shared.binance_api import get_account, get_position, reset_session
reset_session()

# ── 1. 账户状态 ───────────────────────────────────────────────────────────────
acc   = get_account()
avail = float(acc.get('availableBalance', 0))
total = float(acc.get('totalWalletBalance', 0))
margin = float(acc.get('totalInitialMargin', 0))
open_p = [p for p in get_position() if abs(float(p.get('positionAmt', 0))) > 0]
print(f'\n[账户] available={avail:.2f}U  total={total:.2f}U  已用保证金={margin:.2f}U')
print(f'[账户] 交易所当前持仓: {len(open_p)} 个')
for p in open_p:
    print(f"       {p['symbol']} {p['positionSide']} amt={p['positionAmt']} "
          f"upnl={float(p.get('unrealizedProfit',0)):+.2f}U")

if avail < 20:
    print('[预检] 余额不足 20U，终止。')
    sys.exit(1)

# ── 2. 候选池刷新 ─────────────────────────────────────────────────────────────
from scanner import refresh_pool, run_scan
pool = refresh_pool()
print(f'\n[候选池] {len(pool)} 个标的（含 Coinglass 清算热度 + Binance 量价）')

# ── 3. 实盘扫描 + 执行开仓 ───────────────────────────────────────────────────
print('[扫描] 开始实盘扫描（dry_run=False）...')
t0     = time.time()
result = run_scan(dry_run=False)
elapsed = time.time() - t0

scanned = result.get('scanned', 0)
raw     = result.get('signals_raw', 0)
final   = result.get('signals_final', 0)
placed  = result.get('placed', 0)

print(f'\n[扫描结果]')
print(f'  扫描标的: {scanned}  原始信号: {raw}  有效信号: {final}  已下单: {placed}  耗时: {elapsed:.1f}s')

from collections import Counter
rej = result.get('rejected', [])
if rej:
    reasons = Counter(r.get('reason', '')[:50] for r in rej)
    print('\n[主要拒绝原因 top5]')
    for r, c in reasons.most_common(5):
        print(f'  {c:3d}x  {r}')

# ── 4. 确认持仓 ───────────────────────────────────────────────────────────────
if placed and placed > 0:
    time.sleep(3)  # 等成交回写
    from shared.state import get_all_positions
    open_pos = [p for p in get_all_positions() if p.get('status') in ('open','pending')]
    print(f'\n[持仓确认] pos_state 中 {len(open_pos)} 个活跃仓位：')
    for p in open_pos:
        mp = 0
        try:
            from shared.binance_api import get_mark_price
            mp = float(get_mark_price(p['symbol']).get('markPrice', 0))
        except Exception:
            pass
        entry = float(p.get('entry_price', 0))
        sl    = float(p.get('sl_price', 0))
        tp1   = float(p.get('tp1_price', 0))
        pnl_pct = ((mp - entry) / entry * 100) if p.get('direction','').upper() == 'LONG' \
                  else ((entry - mp) / entry * 100) if mp > 0 else 0
        print(f"  {p['symbol']:12s} {p.get('direction',''):5s} {p.get('mode',''):10s} "
              f"strength={p.get('signal_strength','?'):6s} status={p.get('status','?')}")
        print(f"    entry={entry:.4g}  sl={sl:.4g}  tp1={tp1:.4g}  "
              f"mark={mp:.4g}  浮动={pnl_pct:+.2f}%")
    print('\n✅ 工作流完成。')
elif final == 0:
    print('\n📭 本轮无满足条件的信号，持仓维持为零。')
    print('   市场当前特征：大量标的 EMA 结构空头 + ATR 低波动，reversal 做空门槛较高。')
else:
    print(f'\n⚠️  找到 {final} 个信号但 placed={placed}，请检查 executor 日志。')
