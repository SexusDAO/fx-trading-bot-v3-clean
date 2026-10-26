"""Bug: _close_position_post_static 步骤4先 pop _mfe/_mae，步骤5再 get → mfe_pct/mae_pct 恒为 0。"""
import asyncio
import time

import position_manager as pm


def _setup(monkeypatch, pos):
    records = []
    monkeypatch.setattr(pm, "get_all_positions", lambda: [dict(pos)])
    monkeypatch.setattr(pm, "_remove_position_checked", lambda pid: True)
    monkeypatch.setattr(pm, "_fetch_actual_fees", lambda *a, **k: 0.0)
    monkeypatch.setattr(pm, "_record_pnl", lambda *a, **k: None)
    monkeypatch.setattr(pm, "set_cooldown", lambda *a, **k: None)
    monkeypatch.setattr(pm, "_log_vol_exhaust_result", lambda *a, **k: None)

    async def _notify(t):
        return None

    monkeypatch.setattr(pm, "_notify_async", _notify)
    import shared.trade_log_io as T
    # 平仓记录现经 append_trade_record_dedup 写入（按 open_time+symbol+direction 去重）；两个入口都截获
    monkeypatch.setattr(T, "append_jsonl", lambda path, rec, *a, **k: records.append(dict(rec)))
    monkeypatch.setattr(T, "append_trade_record_dedup", lambda path, rec, *a, **k: records.append(dict(rec)) or True)
    return records


def _pos(pid="p-mfe"):
    return {"pos_id": pid, "symbol": "ABCUSDT", "direction": "long", "entry_price": 100.0,
            "quantity": 1.0, "leverage": 5, "open_time": int((time.time() - 3600) * 1000),
            "mode": "pullback", "sl_price": 95.0, "tp1_price": 110.0}


def _run(pid):
    asyncio.run(pm._close_position_post_static(pid, "ABCUSDT", "tp1", {}, fill_price=105.0, fill_qty=1.0))


def test_mfe_mae_written_before_clear(monkeypatch):
    pid = "p-mfe"
    pm._mfe[pid] = 0.0432
    pm._mae[pid] = 0.0117
    recs = _setup(monkeypatch, _pos(pid))
    _run(pid)
    assert len(recs) == 1
    assert recs[0]["mfe_pct"] == 4.32
    assert recs[0]["mae_pct"] == 1.17
    # 步骤4仍然清理了内存状态
    assert pid not in pm._mfe and pid not in pm._mae


def test_missing_mfe_mae_defaults_to_zero(monkeypatch):
    pid = "p-none"
    pm._mfe.pop(pid, None)
    pm._mae.pop(pid, None)
    recs = _setup(monkeypatch, _pos(pid))
    _run(pid)
    assert recs and recs[0]["mfe_pct"] == 0.0 and recs[0]["mae_pct"] == 0.0
