"""启动分批初始化：P1 校验、批次等待、hub 订阅暂缓、P3 预热。"""
import asyncio

import pytest

try:
    import main
except SystemExit:
    pytest.skip("main.py 依赖缺失", allow_module_level=True)

import config
import scanner


def test_config_defaults():
    assert config.STARTUP_BATCH_DELAY_P2 == 5
    assert config.STARTUP_BATCH_DELAY_P3 == 8
    assert config.STARTUP_BATCH_DELAY_P4 == 12


def test_startup_wait_uses_config_and_sleeps(monkeypatch):
    slept = []

    async def fake_sleep(s):
        slept.append(s)

    monkeypatch.setattr(main.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(config, "STARTUP_BATCH_DELAY_P2", 7, raising=False)
    assert asyncio.run(main._startup_wait("P2", "STARTUP_BATCH_DELAY_P2", 5)) == 7
    assert slept == [7.0]


def test_startup_wait_zero_or_invalid(monkeypatch):
    slept = []

    async def fake_sleep(s):
        slept.append(s)

    monkeypatch.setattr(main.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(config, "STARTUP_BATCH_DELAY_P3", 0, raising=False)
    assert asyncio.run(main._startup_wait("P3", "STARTUP_BATCH_DELAY_P3", 8)) == 0
    monkeypatch.setattr(config, "STARTUP_BATCH_DELAY_P3", "abc", raising=False)
    # 非法值回落默认
    assert asyncio.run(main._startup_wait("P3", "STARTUP_BATCH_DELAY_P3", 8)) == 8
    assert slept == [8.0]


def test_p1_account_ok(monkeypatch):
    from shared import binance_api as b
    calls = []
    monkeypatch.setattr(b, "get_account", lambda: calls.append(1) or {"totalWalletBalance": "1"})
    asyncio.run(main._startup_p1_validate_account())
    assert calls == [1]          # 只调用一次


def test_p1_account_failure_does_not_raise(monkeypatch):
    from shared import binance_api as b

    def boom():
        raise RuntimeError("-1003 too many requests")

    monkeypatch.setattr(b, "get_account", boom)
    asyncio.run(main._startup_p1_validate_account())      # 不抛
    monkeypatch.setattr(b, "get_account", lambda: {})
    asyncio.run(main._startup_p1_validate_account())      # 空结果也不抛


def test_hub_subscribe_hold(monkeypatch):
    got = []
    from shared import market_data_hub as hub
    monkeypatch.setattr(scanner, "_hub_enabled", lambda: True)
    monkeypatch.setattr(hub, "subscribe_klines", lambda s: got.append(list(s)))
    try:
        scanner.set_hub_subscribe_hold(True)
        scanner._hub_subscribe_klines(["BTCUSDT"])
        assert got == []
        scanner.set_hub_subscribe_hold(False)
        scanner._hub_subscribe_klines(["BTCUSDT"])
        assert got == [["BTCUSDT"]]
    finally:
        scanner.set_hub_subscribe_hold(False)


def test_p3_warm_swallows_errors(monkeypatch):
    import shared.risk_manager as rm

    def boom():
        raise RuntimeError("x")

    monkeypatch.setattr(rm, "check_systemic_risk", boom)
    main._startup_p3_warm()      # 不抛


def test_sync_positions_rest_not_blocking_loop():
    import inspect
    src = inspect.getsource(main._sync_positions_on_startup)
    assert "asyncio.to_thread(_bapi.get_position)" in src
    assert "asyncio.to_thread(_bapi.get_open_orders)" in src
    assert "asyncio.to_thread(_bapi.get_algo_open_orders)" in src
