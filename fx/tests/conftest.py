"""测试公共夹具。"""
import pytest

# [v2-7] 撤单结果未知时 signal_watcher 会后台查持仓补止损；
# 以下旧撤单测试只 mock 了撤单接口，这里把查持仓/补止损也挡掉，避免真实联网。
_GUARD_FILES = {"test_cancel_paths.py", "test_cancel_state_batch1.py"}


@pytest.fixture(autouse=True)
def _mock_unknown_fill_guard(request, monkeypatch):
    if request.node.fspath.basename in _GUARD_FILES:
        from shared import binance_api as _api
        monkeypatch.setattr(_api, "get_position", lambda *a, **k: [], raising=False)
        monkeypatch.setattr(_api, "place_sl", lambda *a, **k: {}, raising=False)
    yield
