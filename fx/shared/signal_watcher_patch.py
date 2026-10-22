# 临时测试：REST 轮询能否替代 WS
from pathlib import Path
import asyncio, sys, time
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from shared import binance_api as api
from shared.signal_watcher import SignalWatcher

async def test():
    watcher = SignalWatcher()
    results = []
    
    sig = {'symbol': 'STRKUSDT', 'mode': 'reversal', 'pre_entry_price': 0.0902954}
    async def on_trigger(s):
        results.append(s['symbol'])
        print(f"TRIGGERED: {s['symbol']}")
    
    await watcher.add_signal(sig, 0.0875865, 0.0930042, 0.0902954, time.time()+3600, on_trigger)
    print(f"已注册 STRKUSDT，开始轮询...")
    
    for i in range(5):
        price = float(api.get_mark_price('STRKUSDT').get('markPrice', 0))
        print(f"  轮询 #{i+1}: price={price:.6g}")
        await watcher.on_price_update('STRKUSDT', price, int(time.time() * 1000))   # [v3.89 #101] 毫秒
        await asyncio.sleep(1)
    
    print(f"触发结果: {results}")

asyncio.run(test())
