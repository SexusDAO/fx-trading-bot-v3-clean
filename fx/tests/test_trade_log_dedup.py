"""trade_log 平仓记录写入前去重：相同 open_time + symbol + direction 只留先到的一条。"""
import json
import threading

from shared import trade_log_io as T


def rec(tid, sym="CHIPUSDT", d="short", open_time=1_700_000_000_000, close=1_700_000_100_000, **kw):
    r = {"trade_id": tid, "symbol": sym, "direction": d, "open_time": open_time, "close_time": close, "pnl_usdt": 1.0}
    r.update(kw)
    return r


def lines(p):
    return [json.loads(x) for x in p.read_text().splitlines()]


def test_second_record_same_open_time_skipped(tmp_path):
    p = tmp_path / "t.jsonl"
    assert T.append_trade_record_dedup(p, rec("a")) is True
    assert T.append_trade_record_dedup(p, rec("b", close=1_700_000_100_999)) is False     # 不同 trade_id，同一笔
    assert [r["trade_id"] for r in lines(p)] == ["a"]


def test_direction_case_insensitive(tmp_path):
    p = tmp_path / "t.jsonl"
    T.append_trade_record_dedup(p, rec("a", d="SHORT"))
    assert T.append_trade_record_dedup(p, rec("b", d="short")) is False


def test_different_symbol_direction_or_open_time_kept(tmp_path):
    p = tmp_path / "t.jsonl"
    T.append_trade_record_dedup(p, rec("a"))
    assert T.append_trade_record_dedup(p, rec("b", sym="ETHUSDT"))
    assert T.append_trade_record_dedup(p, rec("c", d="long"))
    assert T.append_trade_record_dedup(p, rec("d", open_time=1_700_000_000_001))
    assert len(lines(p)) == 4


def test_records_without_open_time_always_appended(tmp_path):
    p = tmp_path / "t.jsonl"
    for i in range(3):
        assert T.append_trade_record_dedup(p, rec(f"m{i}", open_time=0))
        assert T.append_trade_record_dedup(p, {"trade_id": f"n{i}", "symbol": "X", "close_reason": "missed_unfilled"})
    assert len(lines(p)) == 6


def test_open_time_as_float_and_string_compares_numerically(tmp_path):
    p = tmp_path / "t.jsonl"
    T.append_trade_record_dedup(p, rec("a", open_time=1_700_000_000_000.0))
    assert T.append_trade_record_dedup(p, rec("b", open_time="1700000000000")) is False


def test_open_records_with_pos_id_do_not_block_close_records(tmp_path):
    p = tmp_path / "t.jsonl"
    p.write_text(json.dumps({"pos_id": "p", "symbol": "CHIPUSDT", "direction": "short", "fill_price": 1}) + "\n")
    assert T.append_trade_record_dedup(p, rec("a"))


def test_garbage_lines_and_missing_file_ok(tmp_path):
    p = tmp_path / "sub" / "t.jsonl"
    assert T.append_trade_record_dedup(p, rec("a"))
    p.write_text("not json\n" + p.read_text())
    assert T.append_trade_record_dedup(p, rec("b")) is False


def test_only_tail_is_scanned_for_big_files(tmp_path, monkeypatch):
    p = tmp_path / "t.jsonl"
    T.append_trade_record_dedup(p, rec("old"))
    p.write_text(p.read_text() + "\n".join(json.dumps(rec(f"f{i}", sym=f"S{i}", open_time=i + 1)) for i in range(400)) + "\n")
    monkeypatch.setattr(T, "_DEDUP_TAIL_BYTES", 2000)
    assert T.append_trade_record_dedup(p, rec("late")) is True        # 很久以前的同名记录在回看范围外，不拦截
    assert T.append_trade_record_dedup(p, rec("f399", sym="S399", open_time=400)) is False   # 近期的会被拦


def test_concurrent_same_trade_leaves_exactly_one(tmp_path):
    p = tmp_path / "t.jsonl"
    res = []
    ts = [threading.Thread(target=lambda i=i: res.append(T.append_trade_record_dedup(p, rec(f"t{i}")))) for i in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert res.count(True) == 1 and len(lines(p)) == 1


def test_write_sites_use_dedup_and_missed_entry_does_not():
    import inspect
    import executor as E
    import position_manager as PM
    assert "append_trade_record_dedup" in inspect.getsource(E)
    src = inspect.getsource(PM)
    assert src.count("append_trade_record_dedup") >= 2
    assert "append_jsonl as _append_jsonl   # v3.93\n            await asyncio.to_thread(_append_jsonl, _log_path, _missed_entry" in src
