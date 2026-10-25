import asyncio, inspect, json
from datetime import datetime, timezone

from tools import liq_snapshot as L


def test_run_appends_one_line_per_symbol(tmp_path):
    zones = {"BTCUSDT": [{"price_low": 1, "price_high": 2, "side": "LONG"}], "ETHUSDT": []}
    r = L.run(["BTCUSDT", "ETHUSDT"], fetch=lambda s: zones[s], base=tmp_path)
    assert r["ok"] == 1 and r["empty"] == 1 and r["failed"] == 0 and r["total"] == 2
    lines = [json.loads(x) for x in open(r["path"], encoding="utf-8")]
    assert [x["symbol"] for x in lines] == ["BTCUSDT", "ETHUSDT"]
    assert lines[0]["zones"][0]["side"] == "LONG" and lines[1]["zones"] == [] and isinstance(lines[0]["ts"], int)


def test_second_run_appends_not_overwrites(tmp_path):
    t = [1_700_000_000.0]
    L.run(["A"], fetch=lambda s: [], base=tmp_path, now_fn=lambda: t[0])
    t[0] += 3600
    r = L.run(["A"], fetch=lambda s: [], base=tmp_path, now_fn=lambda: t[0])
    assert len(open(r["path"]).read().strip().splitlines()) == 2


def test_file_named_by_utc_date(tmp_path):
    ts = datetime(2026, 10, 25, 23, 59, tzinfo=timezone.utc).timestamp()
    assert L.snapshot_path(ts, tmp_path).name == "20261025.jsonl"


def test_single_failure_isolated(tmp_path):
    def f(s):
        if s == "BAD":
            raise RuntimeError("x")
        return [{"a": 1}]
    r = L.run(["A", "BAD", "B"], fetch=f, base=tmp_path)
    assert (r["ok"], r["failed"]) == (2, 1)


def test_empty_pool_writes_nothing(tmp_path):
    r = L.run([], fetch=lambda s: [], base=tmp_path)
    assert r["total"] == 0 and not list(tmp_path.glob("*"))


def test_budget_stops_early(tmp_path):
    t = [0.0]
    def now(): t[0] += 100; return t[0]
    r = L.run(["A", "B", "C"], fetch=lambda s: [{"z": 1}], base=tmp_path, budget_sec=150, now_fn=now)
    assert r["ok"] < 3


def test_load_pool(tmp_path):
    p = tmp_path / "pool.json"
    p.write_text(json.dumps({"pool": ["A", "B", 3]}))
    assert L.load_pool(p) == ["A", "B"]
    assert L.load_pool(tmp_path / "none.json") == []


def test_never_raises_on_bad_input(tmp_path):
    assert L.run(None, base=tmp_path, fetch=None)["total"] >= 0


def test_main_hourly_task_registered():
    import main
    from datetime import datetime, timezone
    d = datetime(2026, 10, 25, 10, 59, 30, tzinfo=timezone.utc)
    assert abs(main._seconds_until_next_hour(d) - 30) < 1e-6
    assert main._seconds_until_next_hour(datetime(2026, 10, 25, 10, 0, 0, tzinfo=timezone.utc)) == 3600
    src = inspect.getsource(main)
    assert '_loop_liq_snapshot(),' in src and 'name="liq_snapshot"' in src
    assert "非致命" in inspect.getsource(main._loop_liq_snapshot)
