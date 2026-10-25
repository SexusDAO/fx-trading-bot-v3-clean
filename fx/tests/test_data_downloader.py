import json

import pytest

from tools import data_downloader as D

H = D.HOUR_MS


@pytest.fixture(autouse=True)
def _tmp_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr(D, "DATA_DIR", tmp_path)
    monkeypatch.setattr(D, "KLINE_DIR", tmp_path / "klines")
    monkeypatch.setattr(D, "FUNDING_DIR", tmp_path / "funding")
    monkeypatch.setattr(D, "SQLITE_PATH", tmp_path / "klines.db")


def _k(t):
    return [t, "1", "2", "0.5", "1.5", "10", t + H - 1, "15", 5, "4", "6"]


class FakeApi:
    """模拟币安：按 startTime/limit 返回连续 1H K线，最后一根可能未收盘。"""
    def __init__(self, first, last_open, fund_times=()):
        self.first, self.last_open, self.fund = first, last_open, list(fund_times)
        self.calls = []

    def __call__(self, path, params):
        self.calls.append((path, dict(params)))
        if path == "/fapi/v1/klines":
            t = max(params["startTime"], self.first)
            t = (t // H) * H if t % H else t
            out = []
            while t <= self.last_open and len(out) < params["limit"]:
                out.append(_k(t)); t += H
            return out
        if path == "/fapi/v1/fundingRate":
            rows = [{"fundingTime": f, "fundingRate": "0.0001", "markPrice": "100"}
                    for f in self.fund if f >= params["startTime"]]
            return rows[:params["limit"]]
        raise AssertionError(path)


NOW = 1_800_000_000_000 // H * H + 30 * 60_000      # 整点后30分钟


@pytest.mark.parametrize("pq", [True, False])
def test_kline_download_and_resume(pq):
    if pq and not D._pyarrow_ok():
        pytest.skip("pyarrow 不可用")
    api = FakeApi(first=NOW - 10 * H, last_open=NOW // H * H)       # 最后一根（当前小时）未收盘
    r1 = D.download_klines("AAAUSDT", 1, get=api, now_ms=NOW, use_parquet=pq, sleep=lambda s: None)
    rows = D.load_klines("AAAUSDT", pq)
    assert r1["new"] == len(rows) and rows[-1]["open_time"] < NOW // H * H       # 未收盘 K 线被丢弃
    assert rows == sorted(rows, key=lambda x: x["open_time"])
    assert len({x["open_time"] for x in rows}) == len(rows)
    # 续传：时间推进 3 小时，只补增量，不重复下载旧数据
    api2 = FakeApi(first=NOW - 10 * H, last_open=(NOW + 3 * H) // H * H)
    r2 = D.download_klines("AAAUSDT", 1, get=api2, now_ms=NOW + 3 * H, use_parquet=pq, sleep=lambda s: None)
    assert r2["resumed"] is True and r2["new"] == 3
    assert api2.calls[0][1]["startTime"] == rows[-1]["open_time"] + H
    again = D.download_klines("AAAUSDT", 1, get=api2, now_ms=NOW + 3 * H, use_parquet=pq, sleep=lambda s: None)
    assert again["new"] == 0 and again["total"] == r2["total"]


def test_kline_pagination(monkeypatch):
    monkeypatch.setattr(D, "KLINE_LIMIT", 4)
    api = FakeApi(first=NOW - 11 * H, last_open=NOW // H * H - H)
    r = D.download_klines("BBBUSDT", 1, get=api, now_ms=NOW, use_parquet=False, sleep=lambda s: None)
    # 假接口把 first 向下取整到整点：floor(NOW)-11H .. floor(NOW)-1H 共 11 根已收盘；limit=4 → 4+4+3，共 3 次请求
    assert r["new"] == 11 and r["requests"] == 3 and r["total"] == 11


def test_rate_limit_sleep_called_each_request():
    sleeps = []
    api = FakeApi(first=NOW - 3 * H, last_open=NOW // H * H - H)
    r = D.download_klines("CCCUSDT", 1, get=api, now_ms=NOW, use_parquet=False, sleep=sleeps.append)
    assert len(sleeps) == r["requests"] and set(sleeps) == {0.2}


def test_funding_download_dedup_and_resume():
    ft = [NOW - 24 * H + i * 8 * H for i in range(3)]
    api = FakeApi(0, 0, fund_times=ft)
    r1 = D.download_funding("DDDUSDT", 2, get=api, now_ms=NOW, sleep=lambda s: None)
    assert r1["new"] == 3
    lines = [json.loads(x) for x in D.funding_path("DDDUSDT").read_text().splitlines()]
    assert [x["fundingTime"] for x in lines] == ft and lines[0]["fundingRate"] == 0.0001
    api2 = FakeApi(0, 0, fund_times=ft + [NOW - 1000])
    r2 = D.download_funding("DDDUSDT", 2, get=api2, now_ms=NOW, sleep=lambda s: None)
    assert r2["resumed"] and r2["new"] == 1 and r2["total"] == 4
    assert api2.calls[0][1]["startTime"] == max(ft) + 1


def test_funding_limit_1000_and_truncated_line_tolerated():
    D.FUNDING_DIR.mkdir(parents=True, exist_ok=True)
    D.funding_path("EEEUSDT").write_text('{"fundingTime": 5, "fundingRate": 0.1, "markPrice": 1}\n{"fundingTi')
    assert len(D.load_funding("EEEUSDT")) == 1
    api = FakeApi(0, 0, fund_times=[NOW - H])
    D.download_funding("EEEUSDT", 1, get=api, now_ms=NOW, sleep=lambda s: None)
    assert api.calls[0][1]["limit"] == 1000


def test_top_symbols_by_volume(tmp_path):
    pf = tmp_path / "pool.json"
    pf.write_text(json.dumps({"pool": ["A", "B", "C", "D"]}))
    api = lambda path, p: [{"symbol": "A", "quoteVolume": "1"}, {"symbol": "B", "quoteVolume": "9"},
                           {"symbol": "C", "quoteVolume": "5"}]
    assert D.top_symbols(2, get=api, pool_file=pf) == ["B", "C"]
    def boom(path, p): raise RuntimeError("x")
    assert D.top_symbols(3, get=boom, pool_file=pf) == ["A", "B", "C"]      # 取不到成交额退回原顺序
    assert D.top_symbols(3, get=api, pool_file=tmp_path / "none.json") == []


def test_run_isolates_failures_and_cli_requires_symbols(monkeypatch):
    def get(path, p):
        if p["symbol"] == "BAD":
            raise RuntimeError("boom")
        return []
    res = D.run(["BAD", "OKUSDT"], 1, get=get)
    assert res[0]["error"] and res[1]["error"] is None
    assert D.main([]) == 2


def test_module_has_no_business_imports_and_paths_under_data():
    import inspect
    src = inspect.getsource(D)
    for banned in ("import position_manager", "import executor", "import scanner", "from scanner", "from executor"):
        assert banned not in src
    assert D.REQUEST_INTERVAL_SEC == 0.2


def test_default_paths_under_fx_data():
    import importlib
    m = importlib.reload(D)
    assert m.DATA_DIR.name == "data" and m.DATA_DIR.parent.name == "fx"
    assert m.KLINE_DIR.parent == m.DATA_DIR and m.FUNDING_DIR.parent == m.DATA_DIR
