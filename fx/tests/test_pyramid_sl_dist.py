import asyncio
import config
import position_manager as pm


def _setup(monkeypatch, price, atr_ratio, entry=0.07655):
    """构造 SHORT 仓位的加仓场景。place_order 被调用即视为通过了加仓后止损距离检查。"""
    placed = []

    async def fake_run_sync(fn, *a, **k):
        name = getattr(fn, "__name__", "")
        if name == "get_klines":
            return [[0, 0, 0, 0, 1.0 + (i % 2) * 0.05] for i in range(20)]   # 交替涨跌,RSI 约 50
        if name == "get_all_positions":
            return []
        if name == "get_account":
            return {"availableBalance": 1e9}
        return None

    def fake_place(**kw):
        placed.append(kw)
        raise RuntimeError("stop-after-order")   # 到此即可,外层 try/except 会吞掉

    monkeypatch.setattr(pm, "_run_sync", fake_run_sync)
    monkeypatch.setattr(pm.api, "place_order", fake_place)
    import executor
    monkeypatch.setattr(executor, "_get_precision", lambda s: {"tick": 1e-5, "step": 1.0})
    pm._last_price["ZBTUSDT"] = price
    pos = {"pos_id": "p1", "symbol": "ZBTUSDT", "direction": "short", "entry_price": entry,
           "atr_at_entry": entry * atr_ratio, "quantity": 100.0, "quantity_remaining": 100.0,
           "leverage": 10, "pyramid_addup": 0}
    return pos, placed


def test_zbtusdt_case_skipped(monkeypatch):
    # 原入场0.07655,加仓价0.07431,ATR比例0.84%:新均价≈0.07580,止损距离≈0.98% < 1.5ATR=1.26% → 跳过
    pos, placed = _setup(monkeypatch, 0.07431, 0.0084)
    asyncio.run(pm._maybe_pyramid(pos))
    assert placed == []


def test_enough_distance_proceeds_to_order(monkeypatch):
    # ATR比例0.5%:1.5ATR=0.75% < 0.98% → 通过检查,进入下单
    pos, placed = _setup(monkeypatch, 0.07431, 0.005)
    asyncio.run(pm._maybe_pyramid(pos))
    assert len(placed) == 1 and placed[0]["side"] == "SELL"


def test_float_profit_threshold_is_15atr(monkeypatch):
    assert config.PYRAMID_FLOAT_PROFIT_ATR == 1.5
    entry = 0.07655
    # 浮盈 1.2ATR(<1.5)不加仓
    pos, placed = _setup(monkeypatch, entry * (1 - 1.2 * 0.005), 0.005)
    asyncio.run(pm._maybe_pyramid(pos))
    assert placed == []
    # 浮盈 2ATR 满足门槛,但按 50% 加仓比例,新均价离保本线仅约 0.67ATR(<1.5ATR)→ 被止损距离检查拦下
    pos, placed = _setup(monkeypatch, entry * (1 - 2.0 * 0.005), 0.005)
    asyncio.run(pm._maybe_pyramid(pos))
    assert placed == []
    # 浮盈 5ATR:新均价离保本线约 1.7ATR(>=1.5ATR)→ 下单
    pos, placed = _setup(monkeypatch, entry * (1 - 5.0 * 0.005), 0.005)
    asyncio.run(pm._maybe_pyramid(pos))
    assert len(placed) == 1


def test_tp_space_check(monkeypatch):
    entry = 0.07655
    price = entry * (1 - 5.0 * 0.005)          # 浮盈5ATR,止损距离检查可通过
    # TP1 离加仓价只剩 0.5ATR(<1.0ATR)→ 跳过
    pos, placed = _setup(monkeypatch, price, 0.005)
    pos["tp1_price"] = price * (1 - 0.5 * 0.005)
    asyncio.run(pm._maybe_pyramid(pos))
    assert placed == []
    # TP1 剩余 2ATR(>=1.0ATR)→ 下单
    pos, placed = _setup(monkeypatch, price, 0.005)
    pos["tp1_price"] = price * (1 - 2.0 * 0.005)
    asyncio.run(pm._maybe_pyramid(pos))
    assert len(placed) == 1
    # 无 tp1_price 时不做该检查
    pos, placed = _setup(monkeypatch, price, 0.005)
    pos["tp1_price"] = 0
    asyncio.run(pm._maybe_pyramid(pos))
    assert len(placed) == 1


def test_zbtusdt_tp_space_alone_would_pass():
    # ZBTUSDT:加仓价0.07431、TP1=0.07345 → 剩余(0.07431-0.07345)/0.07431=1.157% > 1.0×0.84%
    assert (0.07431 - 0.07345) / 0.07431 > 0.0084
