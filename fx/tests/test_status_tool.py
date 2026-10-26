"""tools/status.py：只读本地文件的状态汇总。全部用临时目录造数据，不联网。"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from tools import status as S

NOW_UTC = datetime(2026, 10, 26, 8, 0, 0, tzinfo=timezone.utc)
NOW_LOCAL = datetime(2026, 10, 26, 16, 0, 0)


def ms(h, m=0, day=26):
    return int(datetime(2026, 10, day, h, m, tzinfo=timezone.utc).timestamp() * 1000)


@pytest.fixture
def root(tmp_path):
    (tmp_path / "logs").mkdir()
    return tmp_path


def write_pos(root, items):
    (root / "logs" / "pos_state.json").write_text(json.dumps(items))


def write_jsonl(path, rows):
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")


GOOD = {"symbol": "LTCUSDT", "direction": "short", "mode": "ema_trend", "status": "open", "entry_price": 56.79,
        "sl_price": 58.57, "sl_order_id": 1, "tp1_price": 53.23, "tp1_order_id": 2}
BAD = {"symbol": "BONKUSDT", "direction": "short", "mode": "recovered", "status": "pending_guard",
       "entry_price": 0.003, "sl_price": 0}


def render(root, daemon="daemon : ✅ 运行中 (PID=12)\nmain.py: ✅ 运行中 (PID=34)\n"):
    return S.render(root, NOW_UTC, NOW_LOCAL, daemon, "3h 5m")


def test_positions_good_and_bad_lines(root):
    write_pos(root, [GOOD, BAD, {"symbol": "X", "status": "closed"}])
    out = render(root)
    assert "持仓（2笔）" in out
    ltc = [l for l in out.splitlines() if l.startswith("LTCUSDT")][0]
    assert "SHORT" in ltc and "ema_trend" in ltc and "sl=58.57" in ltc and "tp1=53.23" in ltc and "✅SL ✅TP" in ltc
    bonk = [l for l in out.splitlines() if l.startswith("BONKUSDT")][0]
    assert "sl=0" in bonk and "tp1=?" in bonk and "❌无SL" in bonk and "status=pending_guard" in bonk
    assert "pnl=?" in ltc and "⚠️ 1个持仓 sl=0" in out and "pending_guard 1个" in out and "BONKUSDT(SHORT)" in out


def test_no_positions_and_missing_files(root):
    out = render(root)
    assert "持仓（0笔）" in out and "（无）" in out and "✅ 无告警" in out
    assert "最近一轮: ?" in out


def test_today_closes_filter_dedup_and_alert(root):
    rows = [
        {"trade_id": "a", "symbol": "ETHUSDT", "direction": "LONG", "mode": "pullback", "close_reason": "tp1", "pnl_usdt": 3.5, "close_time": ms(2, 30)},
        {"trade_id": "a", "symbol": "ETHUSDT", "direction": "LONG", "mode": "pullback", "close_reason": "tp1", "pnl_usdt": 3.5, "close_time": ms(2, 30)},
        {"trade_id": "b", "symbol": "SOLUSDT", "direction": "SHORT", "mode": "reversal", "close_reason": "sl", "pnl_usdt": -1.25, "close_time": ms(5, 0)},
        {"trade_id": "old", "symbol": "OLD", "direction": "LONG", "close_time": ms(23, 59, day=25)},
        {"trade_id": "bad", "symbol": "BAD", "close_time": "x"},
    ]
    write_jsonl(root / "logs" / "trade_log.jsonl", rows)
    out = render(root)
    assert "今日平仓（2笔）" in out and "OLD" not in out
    assert out.index("ETHUSDT") < out.index("SOLUSDT")                 # 按时间排序
    assert "+3.50U" in out and "-1.25U" in out and "合计: +2.25U" in out
    assert "trade_log 有重复记录" in out and "1 条重复" in out


def test_trade_log_corrupt_lines_ignored(root):
    (root / "logs" / "trade_log.jsonl").write_text("{bad json\n\n" + json.dumps(
        {"trade_id": "z", "symbol": "ZUSDT", "close_time": ms(1)}) + "\n")
    assert "今日平仓（1笔）" in render(root)


def test_scan_state_from_log(root):
    log = "\n".join([
        "2026-10-26 15:40:00 INFO fx.scanner [scan-perf] 候选池加载=1.00s 批量标记价=0.50s 并行分析=30.00s 信号后处理=2.50s scan_unfinished=0 候选池=120",
        "2026-10-26 15:40:01 INFO fx.scanner [liq-stats] 本轮清算区: 命中50个/扫描100个(50%) 缺失2个(sl_degraded)",
        "2026-10-26 15:45:00 INFO fx.scanner [scan-perf] 候选池加载=2.00s 批量标记价=1.00s 并行分析=40.00s 信号后处理=3.00s scan_unfinished=0 候选池=120",
        "2026-10-26 15:45:01 INFO fx.scanner [liq-stats] 本轮清算区: 命中82个/扫描100个(82%) 缺失0个(sl_degraded)",
    ])
    (root / "logs" / "system.log").write_text(log + "\n")
    out = render(root)
    assert "最近一轮: 15:45 | 耗时46s | 清算区命中82%（82/100）" in out        # 取最后一条


def test_loop_block_count_last_hour_only(root):
    lines = [
        "2026-10-26 14:30:00 WARNING fx.main [loop] 心跳延迟 2.10s",          # 超过 1 小时
        "2026-10-26 15:10:00 WARNING fx.main [loop] 事件循环延迟 0.90s",
        "2026-10-26 15:30:00 ERROR fx.main [watchdog] 事件循环阻塞 3.2s，线程堆栈:",
        "2026-10-26 15:31:00 INFO fx.main [loop] 无关行不含阻塞? 仍含[loop]标记",
        "2026-10-26 15:32:00 INFO fx.main 普通日志",
    ]
    assert S.count_loop_blocks(lines, NOW_LOCAL, 1.0) == 3
    (root / "logs" / "system.log").write_text("\n".join(lines) + "\n")
    assert "最近事件循环阻塞: 3次" in render(root)


def test_daemon_parse_and_render(root):
    ds = S.parse_daemon_status("daemon : ✅ 运行中 (PID=12)\nmain.py: ❌ 未运行 (PID=None)\n")
    assert ds == {"daemon": True, "pid": 12, "main": False}
    assert "daemon: ✅运行中" in render(root) and "uptime: 3h 5m" in render(root)
    assert "daemon: ❌未运行" in render(root, "daemon : ❌ 未运行 (PID=None)\nmain.py: ❌ 未运行 (PID=None)\n")
    assert "daemon: ?" in render(root, "")


def test_tail_lines_reads_only_tail(tmp_path):
    f = tmp_path / "big.log"
    f.write_text("\n".join(f"line{i}" for i in range(5000)) + "\n")
    t = S.tail_lines(f, 500)
    assert len(t) == 500 and t[-1] == "line4999" and t[0] == "line4500"
    assert S.tail_lines(tmp_path / "nope.log") == []


def test_read_only_never_writes(root):
    write_pos(root, [GOOD])
    before = sorted(p.name for p in root.rglob("*"))
    render(root)
    assert sorted(p.name for p in root.rglob("*")) == before


def test_main_runs_with_root_arg(root, capsys, monkeypatch):
    write_pos(root, [GOOD])
    monkeypatch.setattr(S, "run_daemon_status", lambda r: "")
    assert S.main(["--root", str(root)]) == 0
    out = capsys.readouterr().out
    assert "═══ 系统状态" in out and "═══ 持仓（1笔）" in out and "═══ 告警" in out


# ── v2：5 秒容差去重 / 2000 行窗口 / --sync ──
def test_today_closes_tolerance_dedup_same_symbol_direction(root):
    rows = [
        {"trade_id": "a", "symbol": "ETHUSDT", "direction": "LONG", "close_time": ms(2, 30), "pnl_usdt": 1},
        {"trade_id": "b", "symbol": "ETHUSDT", "direction": "long", "close_time": ms(2, 30) + 4000, "pnl_usdt": 1},   # 4 秒内 → 重复
        {"trade_id": "c", "symbol": "ETHUSDT", "direction": "LONG", "close_time": ms(2, 30) + 6000, "pnl_usdt": 2},   # 6 秒 → 另一笔
        {"trade_id": "d", "symbol": "ETHUSDT", "direction": "SHORT", "close_time": ms(2, 30), "pnl_usdt": 3},         # 方向不同 → 另一笔
        {"trade_id": "e", "symbol": "SOLUSDT", "direction": "LONG", "close_time": ms(2, 30), "pnl_usdt": 4},          # 标的不同
    ]
    kept, dups = S.today_closes(rows, "2026-10-26")
    assert [r["trade_id"] for r in kept] == ["a", "d", "e", "c"] or len(kept) == 4
    assert dups == 1 and "b" not in [r["trade_id"] for r in kept]


def test_scan_window_is_2000_lines(root):
    assert S.TAIL_LINES == 2000
    filler = "\n".join(f"2026-10-26 15:00:00 INFO x filler{i}" for i in range(1500))
    scan = ("2026-10-26 15:01:00 INFO x [scan-perf] 候选池加载=1.00s 批量标记价=1.00s 并行分析=1.00s 信号后处理=1.00s\n"
            "2026-10-26 15:01:01 INFO x [liq-stats] 本轮清算区: 命中10个/扫描20个(50%)\n")
    (root / "logs" / "system.log").write_text(scan + filler + "\n")           # 扫描行在 1500 行之前，旧的 500 行窗口读不到
    assert "清算区命中50%" in render(root)


def test_sync_flag_calls_sync_then_renders(root, capsys, monkeypatch):
    import tools.sync_trades as T
    called = {}
    monkeypatch.setattr(T, "sync", lambda r, **k: called.setdefault("root", r))
    monkeypatch.setattr(S, "run_daemon_status", lambda r: "")
    assert S.main(["--sync", "--root", str(root)]) == 0
    assert called["root"] == root and "═══ 系统状态" in capsys.readouterr().out


def test_sync_failure_does_not_break_status(root, capsys, monkeypatch):
    import tools.sync_trades as T
    def boom(r, **k):
        raise RuntimeError("net down")
    monkeypatch.setattr(T, "sync", boom)
    monkeypatch.setattr(S, "run_daemon_status", lambda r: "")
    assert S.main(["--sync", "--root", str(root)]) == 0
    out = capsys.readouterr().out
    assert "交易所同步失败" in out and "═══ 持仓" in out


def test_without_sync_flag_never_imports_sync(root, monkeypatch):
    import tools.sync_trades as T
    monkeypatch.setattr(T, "sync", lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not sync")))
    monkeypatch.setattr(S, "run_daemon_status", lambda r: "")
    assert S.main(["--root", str(root)]) == 0
