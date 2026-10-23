"""tools/reconcile_triggers.py 与 tools/cleanup_rejected_log.py 的离线测试（全 mock，不调 API）。"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools import reconcile_triggers as RT
from tools import cleanup_rejected_log as CL


def _w(p, recs):
    p.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in recs), encoding="utf-8")


def _r(p):
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]


def test_reconcile_classify_and_files(tmp_path):
    src, done = tmp_path / "tr.jsonl", tmp_path / "done.jsonl"
    _w(src, [{"symbol": "AUSDT", "order_id_if_known": 1},
             {"symbol": "BUSDT", "order_id_if_known": 2},
             {"symbol": "CUSDT", "order_id_if_known": 3},
             {"symbol": "DUSDT", "order_id_if_known": None},
             {"symbol": "EUSDT", "order_id_if_known": 5}])
    status = {1: "FILLED", 2: "CANCELED", 3: "NEW"}

    def fake(sym, oid):
        if oid == 5:
            raise RuntimeError("boom")
        return {"status": status[oid], "executedQty": "1", "avgPrice": "10"}

    c = RT.run(get_order=fake, in_path=src, done_path=done)
    assert c == {"filled": 1, "canceled": 1, "unknown": 3}
    assert [r["symbol"] for r in _r(src)] == ["CUSDT", "DUSDT", "EUSDT"]
    d = {r["symbol"]: r["reconcile_status"] for r in _r(done)}
    assert d == {"AUSDT": "filled", "BUSDT": "canceled"}


def test_reconcile_dry_run_no_change(tmp_path):
    src, done = tmp_path / "tr.jsonl", tmp_path / "done.jsonl"
    _w(src, [{"symbol": "AUSDT", "order_id_if_known": 1}])
    before = src.read_text(encoding="utf-8")
    RT.run(dry_run=True, get_order=lambda s, o: {"status": "FILLED"}, in_path=src, done_path=done)
    assert src.read_text(encoding="utf-8") == before and not done.exists()


def test_cleanup_keeps_7_days(tmp_path):
    p = tmp_path / "rej.jsonl"
    now = 1_800_000_000.0
    _w(p, [{"ts": int((now - 8 * 86400) * 1000)}, {"ts": int((now - 1 * 86400) * 1000)},
           {"ts": now - 10 * 86400}, {"no_ts": 1}])
    with open(p, "a", encoding="utf-8") as f:
        f.write("not json\n")
    c = CL.run(days=7, dry_run=True, path=p, now=now)
    assert c == {"delete": 2, "keep": 3}
    assert len(p.read_text(encoding="utf-8").splitlines()) == 5      # dry-run 不改文件
    CL.run(days=7, path=p, now=now)
    lines = p.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3 and "not json" in lines
