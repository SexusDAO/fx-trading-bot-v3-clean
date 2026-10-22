"""_mark_price_map 的作用域/缩进与两处复用。运行：cd fx && BINANCE_API_KEY=x BINANCE_API_SECRET=y python3 -m pytest -q tests/test_scanner_batch_markprice_scope.py"""
import ast, os, sys
from pathlib import Path

os.environ.setdefault("BINANCE_API_KEY", "x")
os.environ.setdefault("BINANCE_API_SECRET", "y")
SRC = (Path(__file__).parent.parent / "scanner.py").read_text()
TREE = ast.parse(SRC)


def _run_scan():
    return next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == "run_scan")


def _top_index(pred):
    for i, n in enumerate(_run_scan().body):
        if pred(n):
            return i
    raise AssertionError("not found in run_scan top-level body")


def _assigns(name):
    def p(n):
        return (isinstance(n, ast.AnnAssign) and getattr(n.target, "id", "") == name) or \
               (isinstance(n, ast.Assign) and any(getattr(t, "id", "") == name for t in n.targets))
    return p


def test_map_init_is_top_level_in_run_scan_and_before_submit():
    i_map = _top_index(_assigns("_mark_price_map"))
    i_ts = _top_index(_assigns("_mark_price_map_ts"))
    # exe.submit 所在的 `with ThreadPoolExecutor` 或 _run_parallel 调用同样位于函数体顶层，且在 map 初始化之后
    def has_submit(n):
        return "analyze_symbol" in ast.unparse(n) and "_mark_price_map" in ast.unparse(n) and not _assigns("_mark_price_map")(n)
    i_sub = _top_index(has_submit)
    assert i_map < i_ts < i_sub        # 同一作用域、顺序正确，不会 NameError，也不在 if/try 分支里


def test_avg_funding_snapshot_reuses_map():
    assert "list(_mark_price_map.values()) or (api.get_all_mark_prices() or [])" in SRC
    assert SRC.count("_all_mp = api.get_all_mark_prices() or []") == 0


def test_registration_uses_map_only_when_fresh():
    assert "time.monotonic() - _mark_price_map_ts <= _MARK_MAP_MAX_AGE_SEC" in SRC
    assert "mp     = mp or api.get_mark_price(sym)" in SRC
    import scanner
    assert scanner._MARK_MAP_MAX_AGE_SEC == 30.0
