"""
test_pulse_candle.py
测试 _is_pulse_candle 函数的全部逻辑：
1. 1H 脉冲检测（BTW 场景）
2. 4H 脉冲检测（BR 场景）
3. 插针型大阳被 body_ratio 过滤
4. 量不够不触发
5. 涨幅不够不触发
6. 无脉冲时返回 None
7. K线不足时安全返回 None
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from shared.judges import _is_pulse_candle


def _make_klines(records):
    """records: list of (o, h, l, c, vol)，自动填充 open_time/close_time"""
    result = []
    for i, (o, h, l, c, vol) in enumerate(records):
        ts = str(1000000 + i * 3600000)
        close_ts = str(1000000 + i * 3600000 + 3599000)
        result.append([ts, str(o), str(h), str(l), str(c), str(vol), close_ts,
                        "0", "0", "0", "0", "0"])
    return result


def _flat_klines(n, base=1.0, vol=1000):
    """生成 n 根平静 K 线（小实体，普通量）"""
    return _make_klines([(base, base*1.002, base*0.998, base*1.001, vol)] * n)


# ─────────────────────────────────────────────────────────────────────────────
# 测试 1：1H 脉冲触发（BTW 场景：2.8x 量 + 14.5% 涨幅 + 实体比 > 50%）
# ─────────────────────────────────────────────────────────────────────────────
def test_1h_pulse_triggers():
    avg_vol = 10000
    # 5 根底量 + 1 根脉冲 + 最新1根回踩（共7根，lookback=2 取 [-3:-1]）
    k1h = _flat_klines(5, vol=avg_vol)
    # 脉冲K线：o=1.0 c=1.145（+14.5%），h=1.16，l=0.99，vol=avg*3.5
    pulse = _make_klines([(1.0, 1.16, 0.99, 1.145, avg_vol * 3.5)])
    latest = _flat_klines(1, base=1.1, vol=avg_vol)
    k1h_full = k1h + pulse + latest  # 7 根，脉冲在 [-2]

    k4h = _flat_klines(15, vol=5000)  # 4H 无脉冲

    result = _is_pulse_candle(k1h_full, k4h)
    assert result is not None, f"应检测到1H脉冲，返回: {result}"
    assert "1H" in result, f"应标注1H，实际: {result}"
    print(f"PASS test_1h_pulse_triggers: {result}")


# ─────────────────────────────────────────────────────────────────────────────
# 测试 2：4H 脉冲触发（BR 场景：23x 量 + 39% 涨幅）
# ─────────────────────────────────────────────────────────────────────────────
def test_4h_pulse_triggers():
    avg_vol = 7000000
    k1h = _flat_klines(15, vol=500000)  # 1H 无脉冲

    # 4H：5 根底量 + 1 根 +39% 爆量 + 2 根回落（共 8 根，lookback=3 取 [-4:-1]）
    k4h_base = _flat_klines(5, base=0.35, vol=avg_vol)
    pulse4h  = _make_klines([(0.3364, 0.494, 0.2918, 0.469, avg_vol * 23)])
    k4h_back = _flat_klines(2, base=0.42, vol=avg_vol)
    k4h_full = k4h_base + pulse4h + k4h_back  # 8 根

    result = _is_pulse_candle(k1h, k4h_full)
    assert result is not None, f"应检测到4H脉冲，返回: {result}"
    assert "4H" in result, f"应标注4H，实际: {result}"
    print(f"PASS test_4h_pulse_triggers: {result}")


# ─────────────────────────────────────────────────────────────────────────────
# 测试 3：插针型大阳被 body_ratio 过滤
# 上影线很长：o=1.0 h=1.50 l=0.99 c=1.08（实体+8%，但振幅51%，实体/振幅=15.7%）
# ─────────────────────────────────────────────────────────────────────────────
def test_needle_candle_filtered_by_body_ratio():
    avg_vol = 10000
    k1h = _flat_klines(5, vol=avg_vol)
    needle = _make_klines([(1.0, 1.50, 0.99, 1.08, avg_vol * 4.0)])
    latest = _flat_klines(1, base=1.05, vol=avg_vol)
    k1h_full = k1h + needle + latest

    k4h = _flat_klines(15, vol=5000)
    result = _is_pulse_candle(k1h_full, k4h)
    assert result is None, f"插针型应被过滤，实际返回: {result}"
    print("PASS test_needle_candle_filtered_by_body_ratio")


# ─────────────────────────────────────────────────────────────────────────────
# 测试 4：成交量不够（2.5x < 3x 门槛）不触发
# ─────────────────────────────────────────────────────────────────────────────
def test_low_volume_not_triggered():
    avg_vol = 10000
    # 底量K线需要至少 lookback+6=8 根，确保均量窗口不混入脉冲K线
    k1h = _flat_klines(8, vol=avg_vol)
    low_vol = _make_klines([(1.0, 1.14, 0.99, 1.13, avg_vol * 2.5)])   # 2.5x < 3.0x
    latest  = _flat_klines(1, base=1.1, vol=avg_vol)
    k1h_full = k1h + low_vol + latest

    k4h = _flat_klines(15, vol=5000)
    result = _is_pulse_candle(k1h_full, k4h)
    assert result is None, f"量不够应不触发，实际: {result}"
    print("PASS test_low_volume_not_triggered")


# ─────────────────────────────────────────────────────────────────────────────
# 测试 5：涨幅不够（4% < 5% 门槛）不触发
# ─────────────────────────────────────────────────────────────────────────────
def test_low_body_chg_not_triggered():
    avg_vol = 10000
    k1h = _flat_klines(5, vol=avg_vol)
    small = _make_klines([(1.0, 1.045, 0.995, 1.04, avg_vol * 4.0)])
    latest = _flat_klines(1, base=1.03, vol=avg_vol)
    k1h_full = k1h + small + latest

    k4h = _flat_klines(15, vol=5000)
    result = _is_pulse_candle(k1h_full, k4h)
    assert result is None, f"涨幅不够应不触发，实际: {result}"
    print("PASS test_low_body_chg_not_triggered")


# ─────────────────────────────────────────────────────────────────────────────
# 测试 6：无脉冲时正常返回 None（正常回踩场景不被误杀）
# ─────────────────────────────────────────────────────────────────────────────
def test_no_pulse_returns_none():
    k1h = _flat_klines(15, base=2.0, vol=10000)
    k4h = _flat_klines(15, base=2.0, vol=50000)
    result = _is_pulse_candle(k1h, k4h)
    assert result is None, f"无脉冲应返回 None，实际: {result}"
    print("PASS test_no_pulse_returns_none")


# ─────────────────────────────────────────────────────────────────────────────
# 测试 7：K 线不足时安全返回 None（不抛异常）
# ─────────────────────────────────────────────────────────────────────────────
def test_insufficient_klines_safe():
    result = _is_pulse_candle([], [])
    assert result is None
    result = _is_pulse_candle(_flat_klines(3), _flat_klines(3))
    assert result is None
    print("PASS test_insufficient_klines_safe")


# ─────────────────────────────────────────────────────────────────────────────
# 测试 8：阴线大跌不触发（body_chg 为负）
# ─────────────────────────────────────────────────────────────────────────────
def test_bear_candle_not_triggered():
    avg_vol = 10000
    k1h = _flat_klines(5, vol=avg_vol)
    bear = _make_klines([(1.2, 1.21, 0.99, 1.02, avg_vol * 5.0)])  # 大阴线
    latest = _flat_klines(1, base=1.05, vol=avg_vol)
    k1h_full = k1h + bear + latest

    k4h = _flat_klines(15, vol=5000)
    result = _is_pulse_candle(k1h_full, k4h)
    assert result is None, f"大阴线不应触发脉冲保护，实际: {result}"
    print("PASS test_bear_candle_not_triggered")


if __name__ == "__main__":
    test_1h_pulse_triggers()
    test_4h_pulse_triggers()
    test_needle_candle_filtered_by_body_ratio()
    test_low_volume_not_triggered()
    test_low_body_chg_not_triggered()
    test_no_pulse_returns_none()
    test_insufficient_klines_safe()
    test_bear_candle_not_triggered()
    print("\n✅ 8/8 通过")
