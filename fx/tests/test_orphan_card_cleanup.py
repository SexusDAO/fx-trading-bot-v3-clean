import asyncio, inspect, time
import shared.notifier as N
import shared.signal_watcher as SW
import shared.state as S


class _E:
    def __init__(self, sig_id): self.signal = {"_sig_id": sig_id}


def test_close_entry_card_removes(monkeypatch):
    calls = []
    monkeypatch.setattr(N, "discard_signal", lambda s: calls.append(("discard", s)))
    monkeypatch.setattr(N, "card_remove", lambda s: calls.append(("remove", s)))
    SW._close_entry_card(_E("ABC_1"), "信号过期")
    assert calls == [("discard", "ABC_1"), ("remove", "ABC_1")]


def test_close_entry_card_skips_reorder_and_empty(monkeypatch):
    calls = []
    monkeypatch.setattr(N, "discard_signal", lambda s: calls.append(s))
    monkeypatch.setattr(N, "card_remove", lambda s: calls.append(s))
    SW._close_entry_card(_E("ABC_1"), "清算区位移重新挂单")   # 重挂要复用卡片
    SW._close_entry_card(_E(""), "信号过期")
    SW._close_entry_card(object(), "信号过期")                  # 无 signal 属性不抛
    assert calls == []


def test_cancel_and_remove_hooks_present():
    src = inspect.getsource(SW.SignalWatcher._cancel_and_remove)
    assert src.count("_close_entry_card(entry, reason)") == 2
    # filled / cancel_unknown 条目保留，不关卡片
    i = src.index('outcome == "confirmed":\n            _close_entry_card')
    assert i > 0


def test_cleanup_stale_cards(monkeypatch):
    now = time.time()
    monkeypatch.setattr(N, "_card_cache", {
        "OLD_1": {"message_id": "m1", "text": "t", "ts": now - 49 * 3600},
        "NEW_1": {"message_id": "m2", "text": "t", "ts": now - 3600},
    })
    appended, removed = [], []
    monkeypatch.setattr(N, "card_append", lambda s, l: appended.append((s, l)) or True)
    monkeypatch.setattr(N, "card_remove", lambda s: removed.append(s))
    done = N.cleanup_stale_cards(48 * 3600)
    assert done == ["OLD_1"] and removed == ["OLD_1"]
    assert appended and "定时清理：卡片超过48小时，自动关闭" in appended[0][1]


def test_cleanup_stale_cards_isolated_failures(monkeypatch):
    now = time.time()
    monkeypatch.setattr(N, "_card_cache", {
        "A": {"ts": now - 50 * 3600}, "B": {"ts": now - 50 * 3600}})
    def _boom(s, l): raise RuntimeError("x")
    removed = []
    monkeypatch.setattr(N, "card_append", _boom)
    monkeypatch.setattr(N, "card_remove", lambda s: removed.append(s))
    assert sorted(N.cleanup_stale_cards()) == ["A", "B"] and sorted(removed) == ["A", "B"]


def test_list_orphan_patching_matches_cleanup_definition(monkeypatch):
    data = [
        {"pos_id": "1", "symbol": "AAA", "status": "patching", "_sig_id": "AAA_1"},
        {"pos_id": "2", "symbol": "BBB", "status": "patching"},
        {"pos_id": "3", "symbol": "BBB", "status": "open"},
        {"pos_id": "4", "symbol": "CCC", "status": "pending"},
    ]
    monkeypatch.setattr(S, "_load_pos", lambda: [dict(p) for p in data])
    ids = [p["pos_id"] for p in S.list_orphan_patching()]
    assert ids == ["1"]


def test_main_hooks_present():
    import main
    s1 = inspect.getsource(main._sync_positions_on_startup)
    assert s1.index("list_orphan_patching") < s1.index("orphans = cleanup_orphan_patching()")
    assert "开机自检：清除孤儿记录，监控结束" in s1
    assert "cleanup_stale_cards" in inspect.getsource(main._loop_daily_maintenance)
