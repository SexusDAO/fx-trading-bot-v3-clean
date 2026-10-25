"""测试公共夹具。"""
import pytest

# 跳过非 pytest 的脚本型文件（fx/test_trade.py 会直接 sys.exit）
collect_ignore_glob = ["**/test_trade.py"]
collect_ignore = ["../test_trade.py"]

# [v2-7] 撤单结果未知时 signal_watcher 会后台查持仓补止损；
# 以下旧撤单测试只 mock 了撤单接口，这里把查持仓/补止损也挡掉，避免真实联网。
_GUARD_FILES = {"test_cancel_paths.py", "test_cancel_state_batch1.py"}


# add_signal 注册前会查交易所真实持仓/挂单（signal_watcher._exchange_conflict），查询失败即「保守放弃」。
# 以下测试只关心注册之后的行为，这里统一把交易所预检挡成「无持仓、无挂单」，避免真实联网（无密钥时 401 → 注册被放弃）。
_EXCHANGE_PRECHECK_FILES = {
    "test_cancel_state_batch1.py", "test_opt_v387_step5.py", "test_trigger_identity.py",
    "test_notifier_flush_timing.py", "test_config_fallback_signal_watcher.py", "test_price_event_freshness.py",
}


@pytest.fixture(autouse=True)
def _mock_unknown_fill_guard(request, monkeypatch):
    name = request.node.fspath.basename
    if name in _GUARD_FILES:
        from shared import binance_api as _api
        monkeypatch.setattr(_api, "get_position", lambda *a, **k: [], raising=False)
        monkeypatch.setattr(_api, "place_sl", lambda *a, **k: {}, raising=False)
    if name in _EXCHANGE_PRECHECK_FILES:
        from shared import binance_api as _api
        monkeypatch.setattr(_api, "get_position", lambda *a, **k: [], raising=False)
        monkeypatch.setattr(_api, "get_open_orders", lambda *a, **k: [], raising=False)
    yield
