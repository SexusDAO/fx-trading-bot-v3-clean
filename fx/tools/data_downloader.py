"""回测历史数据下载器（步骤1）：1H K线 + 资金费率，支持断点续传。

用法：
  python3 tools/data_downloader.py --symbols BTCUSDT ETHUSDT --days 180
  python3 tools/data_downloader.py --top30 --days 180

存储（统一放 fx/data/，已 gitignore）：
  K线   fx/data/klines/{symbol}_1h.parquet；pyarrow 不可用时降级 SQLite fx/data/klines.db
  费率  fx/data/funding/{symbol}_funding.jsonl（每行一条，按 fundingTime 去重追加）
只调用币安公开接口（经 shared.binance_api._get，沿用其直连/代理逻辑），不依赖任何业务模块。
断点续传：已有数据不重复下载，只从最后一条之后补增量；只存已收盘的 K 线。
每次请求间隔 REQUEST_INTERVAL_SEC（0.2s）限速。
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logger = logging.getLogger("fx.data_downloader")

DATA_DIR = ROOT / "data"
KLINE_DIR = DATA_DIR / "klines"
FUNDING_DIR = DATA_DIR / "funding"
SQLITE_PATH = DATA_DIR / "klines.db"
REQUEST_INTERVAL_SEC = 0.2
HOUR_MS = 3_600_000
KLINE_LIMIT = 1500          # /fapi/v1/klines 单次上限
FUNDING_LIMIT = 1000        # /fapi/v1/fundingRate 单次上限
KLINE_COLS = ["open_time", "open", "high", "low", "close", "volume", "close_time",
              "quote_volume", "trades", "taker_buy_base", "taker_buy_quote"]


def _default_get(path: str, params: dict):
    from shared.binance_api import _get       # 公开 GET：直连失败自动切代理
    return _get(path, params)


def _pyarrow_ok() -> bool:
    try:
        import pyarrow  # noqa: F401
        import pandas   # noqa: F401
        return True
    except Exception:
        return False


# ───────────────────────── K 线存储 ─────────────────────────
def _norm_kline_row(k: list) -> dict:
    return {"open_time": int(k[0]), "open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
            "close": float(k[4]), "volume": float(k[5]), "close_time": int(k[6]),
            "quote_volume": float(k[7]), "trades": int(k[8]),
            "taker_buy_base": float(k[9]), "taker_buy_quote": float(k[10])}


def kline_path(symbol: str) -> Path:
    return KLINE_DIR / f"{symbol}_1h.parquet"


def load_klines(symbol: str, use_parquet: bool | None = None) -> list:
    """读已存 K 线（list[dict]，按 open_time 升序）。"""
    use_parquet = _pyarrow_ok() if use_parquet is None else use_parquet
    if use_parquet:
        p = kline_path(symbol)
        if not p.exists():
            return []
        import pandas as pd
        return pd.read_parquet(p).sort_values("open_time").to_dict("records")
    if not SQLITE_PATH.exists():
        return []
    with sqlite3.connect(SQLITE_PATH) as con:
        try:
            cur = con.execute(f"SELECT {','.join(KLINE_COLS)} FROM klines_1h WHERE symbol=? ORDER BY open_time", (symbol,))
        except sqlite3.OperationalError:
            return []
        return [dict(zip(KLINE_COLS, r)) for r in cur.fetchall()]


def save_klines(symbol: str, rows: list, use_parquet: bool | None = None) -> int:
    """合并去重后写入（按 open_time 去重，新数据覆盖旧数据）。返回合并后总行数。"""
    use_parquet = _pyarrow_ok() if use_parquet is None else use_parquet
    merged = {r["open_time"]: r for r in load_klines(symbol, use_parquet)}
    for r in rows:
        merged[r["open_time"]] = r
    ordered = [merged[k] for k in sorted(merged)]
    if use_parquet:
        import pandas as pd
        KLINE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = kline_path(symbol).with_suffix(".parquet.tmp")
        pd.DataFrame(ordered, columns=KLINE_COLS).to_parquet(tmp, index=False)
        tmp.replace(kline_path(symbol))           # 原子替换，中断不留半截文件
    else:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(SQLITE_PATH) as con:
            con.execute("CREATE TABLE IF NOT EXISTS klines_1h (symbol TEXT, " +
                        ", ".join(f"{c} REAL" if c not in ("open_time", "close_time", "trades") else f"{c} INTEGER"
                                  for c in KLINE_COLS) + ", PRIMARY KEY(symbol, open_time))")
            con.executemany(
                f"INSERT OR REPLACE INTO klines_1h (symbol,{','.join(KLINE_COLS)}) VALUES (?,{','.join('?'*len(KLINE_COLS))})",
                [(symbol, *[r[c] for c in KLINE_COLS]) for r in rows])
    return len(ordered)


def download_klines(symbol: str, days: int, get=None, now_ms: int | None = None,
                    use_parquet: bool | None = None, sleep=time.sleep) -> dict:
    """下载/补齐某标的 1H K线。已有数据 → 从最后一根之后续传；只保留已收盘 K 线。"""
    get = get or _default_get
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    existing = load_klines(symbol, use_parquet)
    want_start = (now_ms // HOUR_MS) * HOUR_MS - days * 24 * HOUR_MS
    start = max(existing[-1]["open_time"] + HOUR_MS, want_start) if existing else want_start
    fetched, requests = [], 0
    while start < now_ms:
        batch = get("/fapi/v1/klines", {"symbol": symbol, "interval": "1h", "startTime": start,
                                         "limit": KLINE_LIMIT})
        requests += 1
        sleep(REQUEST_INTERVAL_SEC)
        if not batch:
            break
        fetched.extend(batch)
        nxt = int(batch[-1][0]) + HOUR_MS
        if nxt <= start:
            break
        start = nxt
        if len(batch) < KLINE_LIMIT:
            break
    rows = [_norm_kline_row(k) for k in fetched if int(k[6]) < now_ms]    # 丢弃未收盘 K 线
    total = save_klines(symbol, rows, use_parquet) if rows else len(existing)
    return {"symbol": symbol, "new": len(rows), "total": total, "requests": requests,
            "resumed": bool(existing)}


# ───────────────────────── 资金费率 ─────────────────────────
def funding_path(symbol: str) -> Path:
    return FUNDING_DIR / f"{symbol}_funding.jsonl"


def load_funding(symbol: str) -> list:
    p = funding_path(symbol)
    if not p.exists():
        return []
    out = []
    for line in p.read_text(encoding="utf-8").splitlines():
        try:
            out.append(json.loads(line))
        except Exception:
            continue                                   # 跳过半截行
    return out


def download_funding(symbol: str, days: int, get=None, now_ms: int | None = None, sleep=time.sleep) -> dict:
    """下载/补齐资金费率历史（/fapi/v1/fundingRate，每次最多 1000 条），按 fundingTime 去重追加。"""
    get = get or _default_get
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    existing = load_funding(symbol)
    seen = {int(r["fundingTime"]) for r in existing}
    start = (max(seen) + 1) if seen else now_ms - days * 24 * HOUR_MS
    new_rows, requests = [], 0
    while start < now_ms:
        batch = get("/fapi/v1/fundingRate", {"symbol": symbol, "startTime": start, "limit": FUNDING_LIMIT})
        requests += 1
        sleep(REQUEST_INTERVAL_SEC)
        if not batch:
            break
        for r in batch:
            ft = int(r["fundingTime"])
            if ft not in seen:
                seen.add(ft)
                new_rows.append({"fundingTime": ft, "fundingRate": float(r["fundingRate"]),
                                 "markPrice": float(r["markPrice"]) if r.get("markPrice") not in (None, "") else None})
        nxt = int(batch[-1]["fundingTime"]) + 1
        if nxt <= start:
            break
        start = nxt
        if len(batch) < FUNDING_LIMIT:
            break
    if new_rows:
        FUNDING_DIR.mkdir(parents=True, exist_ok=True)
        with open(funding_path(symbol), "a", encoding="utf-8") as f:
            for r in sorted(new_rows, key=lambda x: x["fundingTime"]):
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return {"symbol": symbol, "new": len(new_rows), "total": len(existing) + len(new_rows),
            "requests": requests, "resumed": bool(existing)}


# ───────────────────────── 标的选择 ─────────────────────────
def top_symbols(n: int = 30, get=None, pool_file: Path | None = None) -> list:
    """扫描池（logs/candidate_pool.json）里按 24h 成交额排名前 n 的标的；
    取不到成交额时退回候选池原顺序的前 n 个。"""
    get = get or _default_get
    pool_file = pool_file or (ROOT / "logs" / "candidate_pool.json")
    try:
        pool = [s for s in (json.loads(Path(pool_file).read_text(encoding="utf-8")).get("pool") or [])
                if isinstance(s, str)]
    except Exception as e:
        logger.warning(f"[downloader] 读取候选池失败: {e}")
        return []
    try:
        vol = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in (get("/fapi/v1/ticker/24hr", {}) or [])}
        if vol:
            pool = sorted(pool, key=lambda s: vol.get(s, 0.0), reverse=True)
    except Exception as e:
        logger.warning(f"[downloader] 取 24h 成交额失败，按候选池原顺序: {e}")
    return pool[:n]


def run(symbols: list, days: int, get=None) -> list:
    results = []
    for sym in symbols:
        try:
            k = download_klines(sym, days, get=get)
            f = download_funding(sym, days, get=get)
            results.append({"symbol": sym, "klines": k, "funding": f, "error": None})
            logger.info(f"[downloader] {sym} K线 +{k['new']}（共{k['total']}） 费率 +{f['new']}（共{f['total']}）")
        except Exception as e:
            results.append({"symbol": sym, "error": str(e)})
            logger.warning(f"[downloader] {sym} 失败: {e}")
    return results


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="下载回测用 1H K线 + 资金费率（断点续传）")
    ap.add_argument("--symbols", nargs="+", help="标的列表，如 BTCUSDT ETHUSDT")
    ap.add_argument("--top30", action="store_true", help="自动取扫描池 24h 成交额前30")
    ap.add_argument("--days", type=int, default=180)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    syms = a.symbols or (top_symbols(30) if a.top30 else [])
    if not syms:
        print("未指定标的：用 --symbols 或 --top30（候选池为空时 --top30 取不到标的）")
        return 2
    res = run(syms, a.days)
    bad = [r for r in res if r.get("error")]
    print(f"完成 {len(res) - len(bad)}/{len(res)}；失败: {[r['symbol'] for r in bad]}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
