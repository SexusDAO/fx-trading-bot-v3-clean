"""cg_get 429 退避跨线程生效：排队等限速间隔期间别的线程触发退避，排队线程醒来后必须跳过；
退避期内的重复 429 不叠加计数；在途成功请求不清零退避计数。全部 mock，不联网。"""
import threading
import time

import pytest
import shared.clawby_adapter as C


class Resp:
    def __init__(self, code, headers=None, body=None):
        self.status_code, self.headers, self._b = code, headers or {}, body
    def json(self):
        return self._b


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.setattr(C, "_CG_429_UNTIL", 0.0)
    monkeypatch.setattr(C, "_CG_429_COUNT", 0)
    monkeypatch.setattr(C, "_CG_429_LAST", 0.0)


def test_state_is_process_global_not_thread_local():
    C._cg_note_429()
    seen = []
    t = threading.Thread(target=lambda: seen.append(C.cg_backoff_remaining()))
    t.start(); t.join()
    assert seen[0] > 0                                         # 另一个线程能看到本线程触发的退避


def test_queued_thread_rechecks_backoff_after_rate_limit_wait(monkeypatch):
    """线程已过入口检查、正在 _wait_cg_rate_limit 排队；等待期间另一线程触发退避 → 醒来后不得发请求。"""
    sent = []
    monkeypatch.setattr(C.requests, "get", lambda *a, **k: sent.append(1) or Resp(200, body={"code": "0", "data": [1]}))

    def slow_wait():
        C._cg_note_429()                                       # 排队期间别的线程撞上 429
    monkeypatch.setattr(C, "_wait_cg_rate_limit", slow_wait)
    assert C.cg_get("api/futures/liquidation/heatmap/model1") is None
    assert sent == []                                          # 退避生效后没有再发出请求


def test_real_threads_only_one_request_hits_429(monkeypatch):
    """复现日志：多线程同时请求同一接口，第一个 429 之后其余线程都不应再发请求。"""
    sent = []
    gate = threading.Lock()
    last = [0.0]

    def paced_wait():                                          # 模拟 2.2s 间隔的串行排队（缩短为 0.05s）
        with gate:
            d = 0.05 - (time.time() - last[0])
            if d > 0:
                time.sleep(d)
            last[0] = time.time()
    monkeypatch.setattr(C, "_wait_cg_rate_limit", paced_wait)
    monkeypatch.setattr(C.requests, "get", lambda *a, **k: sent.append(1) or Resp(429))
    ts = [threading.Thread(target=C.cg_get, args=("api/futures/liquidation/heatmap/model1",)) for _ in range(6)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert len(sent) == 1


def test_repeat_429_inside_backoff_does_not_escalate():
    assert C._cg_note_429() == 10
    w = C._cg_note_429()                                       # 退避期内再来一次 429（在途请求）
    assert w <= 10 and C._CG_429_COUNT == 1                    # 不叠加成 20s，也不是又一个“退避10s”


def test_in_flight_success_does_not_reset_count(monkeypatch):
    sent_at = time.time() - 1.0
    C._cg_note_429()                                           # 之后才收到：发出时间早于最近一次 429
    C._cg_note_ok(sent_at)
    assert C._CG_429_COUNT == 1
    C._cg_note_ok(time.time() + 1)                             # 退避之后发出的成功请求才清零
    assert C._CG_429_COUNT == 0
    C._cg_note_429(); C._cg_note_ok()                          # 不带时间（旧调用方式）仍然清零
    assert C._CG_429_COUNT == 0


def test_backoff_applies_to_all_paths():
    """CG 限额是账号级（30次/分钟），退避必须覆盖所有接口，不能只按路径。"""
    C._cg_note_429()
    assert C.cg_get("api/futures/liquidation/heatmap/model1") is None
    assert C.cg_get("api/futures/cvd/history") is None
    assert C.cg_get("api/futures/liquidation/coin-list") is None
