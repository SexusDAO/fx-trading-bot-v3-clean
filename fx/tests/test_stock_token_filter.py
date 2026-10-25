"""股票类代币交易时段过滤：只影响开仓扫描候选池。"""
import inspect
from datetime import datetime, timezone

import config
import scanner


class _FakeDT(datetime):
    _hour = 0
    _day = 26   # 2026-10-26 是周一；10-24 周六、10-25 周日

    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 10, cls._day, cls._hour, 30, tzinfo=timezone.utc)


def _at(monkeypatch, hour, day=26):
    import datetime as _dtmod
    _FakeDT._hour = hour
    _FakeDT._day = day
    monkeypatch.setattr(_dtmod, "datetime", _FakeDT)


def test_config_defaults():
    assert config.STOCK_TOKEN_FILTER_ENABLED is True
    assert config.STOCK_TOKENS_US_HOURS_UTC == (12, 21)
    assert config.STOCK_TOKENS_HK_HOURS_UTC == (1, 8)
    assert config.STOCK_TOKENS_KR_HOURS_UTC == (0, 7)
    assert config.STOCK_TOKENS_CN_HOURS_UTC == (1, 7)
    assert "TSLAUSDT" in config.STOCK_TOKENS_US and "BYDUSDT" in config.STOCK_TOKENS_HK
    assert "SAMSUNGUSDT" in config.STOCK_TOKENS_KR and "CXMTUSDT" in config.STOCK_TOKENS_CN
    assert (len(config.STOCK_TOKENS_US), len(config.STOCK_TOKENS_HK),
            len(config.STOCK_TOKENS_KR), len(config.STOCK_TOKENS_CN)) == (187, 15, 8, 2)


def test_non_stock_always_true(monkeypatch):
    for h in (0, 5, 12, 20, 23):
        _at(monkeypatch, h)
        assert scanner._is_stock_token_tradeable("BTCUSDT") is True


def test_us_window_half_open(monkeypatch):
    for h, ok in ((11, False), (12, True), (20, True), (21, False), (23, False), (0, False)):
        _at(monkeypatch, h)
        assert scanner._is_stock_token_tradeable("TSLAUSDT") is ok, h


def test_hk_kr_cn_windows(monkeypatch):
    for h, ok in ((0, False), (1, True), (7, True), (8, False)):
        _at(monkeypatch, h)
        assert scanner._is_stock_token_tradeable("BYDUSDT") is ok, ("HK", h)
    for h, ok in ((0, True), (6, True), (7, False), (12, False)):
        _at(monkeypatch, h)
        assert scanner._is_stock_token_tradeable("SAMSUNGUSDT") is ok, ("KR", h)
    for h, ok in ((0, False), (1, True), (6, True), (7, False)):
        _at(monkeypatch, h)
        assert scanner._is_stock_token_tradeable("CXMTUSDT") is ok, ("CN", h)


def test_switch_off_means_no_effect(monkeypatch):
    monkeypatch.setattr(config, "STOCK_TOKEN_FILTER_ENABLED", False)
    for h in (3, 15):
        _at(monkeypatch, h)
        assert scanner._is_stock_token_tradeable("TSLAUSDT") is True
        assert scanner._is_stock_token_tradeable("BYDUSDT") is True


def test_config_read_failure_fails_open(monkeypatch):
    monkeypatch.delattr(config, "STOCK_TOKENS_CN")
    _at(monkeypatch, 3)
    assert scanner._is_stock_token_tradeable("TSLAUSDT") is True


def test_hooked_into_scan_pool_only():
    src = inspect.getsource(scanner)
    i = src.index("scan_pool = [s for s in pool if s not in active_sym_set]")
    seg = src[i:i + 900]
    assert "_is_stock_token_tradeable(_s)" in seg and "非交易时段，跳过" in seg
    # 持仓维护路径（patrol / maintain_pending_order）不引用该过滤
    assert "_is_stock_token_tradeable" not in inspect.getsource(scanner.maintain_pending_order)


def test_fake_dates_weekdays():
    assert datetime(2026, 10, 26).weekday() == 0 and datetime(2026, 10, 24).weekday() == 5 \
        and datetime(2026, 10, 25).weekday() == 6 and datetime(2026, 10, 23).weekday() == 4


def test_weekend_blocks_all_stock_groups(monkeypatch):
    # 周六(24)、周日(25)：各市场在其“开市小时”内也不允许
    cases = (("TSLAUSDT", 15), ("BYDUSDT", 3), ("SAMSUNGUSDT", 3), ("CXMTUSDT", 3))
    for day in (24, 25):
        for sym, h in cases:
            _at(monkeypatch, h, day)
            assert scanner._is_stock_token_tradeable(sym) is False, (sym, day)
    # 同一小时工作日放行（周一、周五）
    for day in (26, 23):
        for sym, h in cases:
            _at(monkeypatch, h, day)
            assert scanner._is_stock_token_tradeable(sym) is True, (sym, day)


def test_weekend_does_not_affect_non_stock(monkeypatch):
    for day in (24, 25):
        _at(monkeypatch, 15, day)
        assert scanner._is_stock_token_tradeable("BTCUSDT") is True


def test_weekend_switch_off_no_effect(monkeypatch):
    monkeypatch.setattr(config, "STOCK_TOKEN_FILTER_ENABLED", False)
    _at(monkeypatch, 15, 24)
    assert scanner._is_stock_token_tradeable("TSLAUSDT") is True


def test_mara_fixed_and_spcx_usd1_kept():
    assert "MARAUSDT" in config.STOCK_TOKENS_US and "MARAUSTD" not in config.STOCK_TOKENS_US
    assert "SPCXUSD1" in config.STOCK_TOKENS_US
    assert len(config.STOCK_TOKENS_US) == 187
