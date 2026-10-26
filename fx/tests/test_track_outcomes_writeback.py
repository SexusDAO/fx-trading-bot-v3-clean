"""track_outcomes：outcome 回填写回 signal_analytics.jsonl；patch 行里的价格并入通过记录；回写不丢并发追加的行。全部 mock K 线。"""
import json
import time

import pytest

from tools import track_outcomes as TO

H = 3600_000
T0 = 1_700_000_000_000


def passed(ts=T0, sym="AUSDT", mode="pullback", d="LONG", **kw):
    r = {"ts": ts, "symbol": sym, "mode": mode, "direction": d, "judge_passed": True,
         "entry_params": {"entry_price": None, "sl_price": None, "tp1_price": None, "tp2_price": None},
         "outcome": {"filled": None, "checked_at": None}}
    r.update(kw)
    return r


def patch(ts=T0 + 2000, sym="AUSDT", mode="pullback", **kw):
    p = {"type": "entry_params_patch", "ts": ts, "symbol": sym, "mode": mode, "entry_params_ok": True,
         "entry_params": {"entry_price": 100.0, "sl_price": 95.0, "tp1_price": 110.0, "tp2_price": None}}
    p.update(kw)
    return p


def kl(ts, lo, hi, close=None):
    return [ts, "0", str(hi), str(lo), str(close if close is not None else (lo + hi) / 2), "0"]


def write(fp, rows):
    fp.write_text("\n".join(json.dumps(r) for r in rows) + "\n")


def read(fp):
    return [json.loads(x) for x in fp.read_text().splitlines()]


@pytest.fixture
def fp(tmp_path):
    return tmp_path / "signal_analytics.jsonl"


def test_merge_patches_fills_prices_only_when_missing():
    recs = [passed(), patch()]
    assert TO._merge_patches(recs) == 1
    assert recs[0]["entry_params"]["entry_price"] == 100.0 and recs[0]["entry_params_ok"] is True
    own = passed(entry_params={"entry_price": 1.0, "sl_price": 0.5})
    assert TO._merge_patches([own, patch()]) == 0 and own["entry_params"]["entry_price"] == 1.0


def test_merge_patches_matches_symbol_mode_and_window():
    far = patch(ts=T0 + TO.PATCH_WINDOW_MS + 1)
    other = patch(sym="BUSDT")
    recs = [passed(), far, other]
    assert TO._merge_patches(recs) == 0
    near, nearer = patch(ts=T0 + 5000), patch(ts=T0 + 1000, entry_params={"entry_price": 7.0, "sl_price": 6.0})
    recs = [passed(), near, nearer]
    TO._merge_patches(recs)
    assert recs[0]["entry_params"]["entry_price"] == 7.0               # 取时间上最近的一条


def test_run_fills_outcome_and_writes_back_same_file(fp, monkeypatch):
    old = int((time.time() - 10 * 3600) * 1000)
    write(fp, [passed(ts=old), patch(ts=old + 2000), {"ts": old, "symbol": "R", "judge_passed": False}])
    bars = [kl(old, 99, 101), kl(old + 900_000, 100, 111)]               # 先触及 entry=100，再到 TP1=110
    monkeypatch.setattr(TO, "_get_klines", lambda *a, **k: bars)
    monkeypatch.setattr(TO.time, "sleep", lambda s: None)
    stats = TO.run(fp)
    assert stats["processed"] == 1
    rows = read(fp)
    r = [x for x in rows if x.get("judge_passed") and x["symbol"] == "AUSDT"][0]
    assert r["outcome"]["filled"] is True and r["outcome"]["exit_reason"] == "tp1" and r["outcome"]["checked_at"]
    assert r["entry_params"]["entry_price"] == 100.0                        # 价格已并入
    assert any(x.get("type") == "entry_params_patch" for x in rows)       # patch 行保留
    assert any(x["symbol"] == "R" for x in rows)                          # 其他行保留


def test_run_skips_already_tracked_and_too_recent(fp, monkeypatch):
    now = int(time.time() * 1000)
    done = passed(ts=now - 20 * H, outcome={"filled": True, "checked_at": 5})
    recent = passed(ts=now - 1000, sym="CUSDT")
    write(fp, [done, recent])
    called = []
    monkeypatch.setattr(TO, "_get_klines", lambda *a, **k: called.append(1) or [])
    before = fp.read_text()
    stats = TO.run(fp)
    assert stats["processed"] == 0 and not called and fp.read_text() == before


def test_write_back_keeps_rows_appended_after_read(fp):
    a = passed()
    write(fp, [a])
    key = TO._rec_key(a)
    oc = {"filled": True, "exit_reason": "sl", "checked_at": 123}
    fp.write_text(fp.read_text() + json.dumps(passed(ts=T0 + 99, sym="NEWUSDT")) + "\n")   # 读之后 scanner 又追加了一行
    assert TO._write_back(fp, {key: oc}, {}) == 1
    rows = read(fp)
    assert len(rows) == 2 and rows[0]["outcome"]["checked_at"] == 123 and rows[1]["symbol"] == "NEWUSDT"
    assert not list(fp.parent.glob("*.tmp"))


def test_write_back_does_not_overwrite_existing_outcome(fp):
    a = passed(outcome={"filled": True, "checked_at": 1, "exit_reason": "tp1"})
    write(fp, [a])
    assert TO._write_back(fp, {TO._rec_key(a): {"filled": False, "checked_at": 2}}, {}) == 0
    assert read(fp)[0]["outcome"]["exit_reason"] == "tp1"


def test_write_back_keeps_garbage_lines(fp):
    a = passed()
    fp.write_text("garbage\n" + json.dumps(a) + "\n")
    TO._write_back(fp, {TO._rec_key(a): {"filled": False, "checked_at": 9}}, {})
    assert fp.read_text().splitlines()[0] == "garbage" and read_ok(fp)


def read_ok(fp):
    return json.loads(fp.read_text().splitlines()[1])["outcome"]["checked_at"] == 9


def test_scanner_patch_carries_entry_params():
    import inspect
    import scanner
    src = inspect.getsource(scanner)
    assert '_patch["entry_params"]' in src and '"entry_price", "sl_price", "tp1_price", "tp2_price"' in src
