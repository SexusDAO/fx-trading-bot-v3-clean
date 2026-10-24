"""flush 发送途中到达的 card_append / card_remove 必须等卡片登记后再编辑追加，不能降级成独立消息。"""
import threading
import time

import shared.notifier as N


def _patch(monkeypatch, send_delay=0.5):
    sent, edits = [], []

    def slow_send(text):
        time.sleep(send_delay)
        sent.append(text)
        return "mid-1"

    monkeypatch.setattr(N, "_send", slow_send)
    monkeypatch.setattr(N, "_edit", lambda mid, t: edits.append((mid, t)) or True)
    monkeypatch.setattr(N, "notify", lambda t: sent.append("STANDALONE:" + t))
    monkeypatch.setattr(N, "_save_card_state", lambda *a, **k: None)
    return sent, edits


def test_reject_during_inflight_flush_appends_to_card(monkeypatch):
    sent, edits = _patch(monkeypatch)
    sid = "RACE1USDT_1"
    N.buffer_signal(sid, "title")
    N.card_append(sid, "trigger")
    t = threading.Thread(target=N.flush_signal, args=(sid,))
    t.start()
    time.sleep(0.1)                      # flush 已取走缓冲区，消息仍在发送
    N.card_append(sid, "REJECT")
    N.card_remove(sid)
    t.join()
    assert sent == ["title\ntrigger"]    # 只有一条卡片，没有独立消息
    assert edits and edits[-1][1].endswith("REJECT")
    assert sid not in N._card_cache      # 没有幽灵缓存
    assert sid not in N._flushing


def test_no_inflight_flush_does_not_wait(monkeypatch):
    sent, edits = _patch(monkeypatch, send_delay=0)
    sid = "RACE2USDT_1"
    N.buffer_signal(sid, "title")
    N.flush_signal(sid)
    t0 = time.time()
    N.card_append(sid, "later")
    assert time.time() - t0 < 1.0
    assert edits and edits[-1][1].endswith("later")
    N.card_remove(sid)


def test_second_flush_does_not_clobber_inflight_marker(monkeypatch):
    sent, edits = _patch(monkeypatch)
    sid = "RACE3USDT_1"
    N.buffer_signal(sid, "title")
    t = threading.Thread(target=N.flush_signal, args=(sid,))
    t.start()
    time.sleep(0.1)
    assert N.flush_signal(sid) is False  # 缓冲区已空；不得清掉第一次 flush 的在途标记
    assert sid in N._flushing
    t.join()
    assert sid not in N._flushing
    N.card_remove(sid)


def test_flush_exception_still_releases_waiters(monkeypatch):
    monkeypatch.setattr(N, "_send", lambda text: (_ for _ in ()).throw(RuntimeError("net")))
    monkeypatch.setattr(N, "_save_card_state", lambda *a, **k: None)
    sid = "RACE4USDT_1"
    N.buffer_signal(sid, "title")
    try:
        N.flush_signal(sid)
    except RuntimeError:
        pass
    assert sid not in N._flushing        # finally 已释放，后续 card_append 不会卡住
