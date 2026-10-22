"""阶段2 等价性测试的公共夹具：确定性合成行情 + 对 scanner 模块的全量 mock（不联网、不写文件）。"""
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

IV_MS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}
T0 = 1_700_000_000_000


def _bars(rng, iv, kind, n=100):
    ms = IV_MS[iv]
    step = {"15m": 0.0015, "1h": 0.004, "4h": 0.008}[iv]
    drift = {"up": 0.35, "down": -0.35, "range": 0.0, "spike": 0.0, "pump": 0.0, "dip": -0.2}[kind] * step
    p, rows = 100.0, []
    start = T0 - (n - 1) * ms
    for i in range(n):
        d = drift
        if kind == "range":
            d = (100.0 - p) / 100.0 * 0.15
        if kind == "spike" and i >= n - max(n // 6, 3):
            d = 0.9 * step
        if kind == "pump":
            d = 0.06 * (1 if i < n - 26 else 0.0) * (4 if iv == "1h" else 1) * (1 if iv != "15m" else 0.2)
        o = p
        c = max(o * (1 + d + rng.uniform(-1, 1) * step), 0.01)
        h = max(o, c) * (1 + rng.uniform(0, 0.4) * step)
        l = min(o, c) * (1 - rng.uniform(0, 0.4) * step)
        v = rng.uniform(800, 1200) * (3.0 if (kind == "spike" and i >= n - 4) else 1.0)
        buy = v * rng.uniform(0.35, 0.65)
        t = start + i * ms
        rows.append([t, f"{o:.8f}", f"{h:.8f}", f"{l:.8f}", f"{c:.8f}", f"{v:.4f}", t + ms - 1,
                     f"{v * c:.4f}", 50, f"{buy:.4f}", f"{buy * c:.4f}", "0"])
        p = c
    return rows


def make_scenario(seed):
    rng = random.Random(seed)
    kind = ["up", "down", "range", "spike", "pump", "dip"][seed % 6]
    k = {iv: _bars(rng, iv, kind) for iv in ("15m", "1h", "4h")}
    k1h = k["1h"]
    last = float(k1h[-1][4])
    day = k1h[-24:]
    hi = max(float(r[2]) for r in day); lo = min(float(r[3]) for r in day)
    first = float(k1h[-25][4])
    sym = f"S{seed:03d}USDT"
    ticker = {"symbol": sym, "quoteVolume": "60000000", "highPrice": f"{hi:.8f}", "lowPrice": f"{lo:.8f}",
              "lastPrice": f"{last:.8f}", "priceChangePercent": f"{(last - first) / first * 100:.4f}"}
    fr = [-0.0009, -0.0001, 0.0001, 0.0006, 0.0012][(seed // 6) % 5]
    mp = {"symbol": sym, "markPrice": f"{last:.8f}", "lastFundingRate": str(fr), "nextFundingTime": 0}
    oi_hist = [{"symbol": sym, "sumOpenInterest": str(1000.0 * (1 + (j - 3) * (0.02 if seed % 4 == 0 else -0.015))),
                "timestamp": j} for j in range(6)]
    return {"symbol": sym, "kind": kind, "k": k, "ticker": ticker, "mp": mp, "oi_hist": oi_hist,
            "oi_now": 1100.0, "oi_24h_ago": [None, 1500.0, 900.0, 1100.0][seed % 4],
            "fv": {"velocity": -0.0007 if seed % 3 == 0 else 0.0001, "current": -0.0003 if seed % 3 == 0 else 0.0001,
                   "16h_ago": 0.0, "rapid_negative": seed % 3 == 0, "detail": "x"},
            "cooldown": [False, False, False, True][seed % 4] if seed % 7 == 0 else False}


class Rec:
    def __init__(self):
        self.analytics, self.shadow, self.api_calls = [], [], []


def _clean(o):
    return json.loads(json.dumps(o, default=str, sort_keys=True))


def install(mod, sc, monkeypatch, rec, llm=None, force=None):
    """给 scanner 模块 mod 打上全量 mock，使 analyze 完全离线、可重复。"""
    from shared import judges, state, shadow_trace
    import config

    def klines(symbol, interval, limit):
        rec.api_calls.append(("klines", interval))
        return [list(r) for r in sc["k"][interval][-limit:]]
    monkeypatch.setattr(mod, "_get_klines_cached", klines)
    monkeypatch.setattr(mod, "_get_mark_price", lambda s: dict(sc["mp"]))
    monkeypatch.setattr(mod, "_in_symbol_funding_window", lambda *a: (False, ""))
    monkeypatch.setattr(mod, "_get_oi_history_cached",
                        lambda s, period="1h", limit=6: [dict(x) for x in sc["oi_hist"][-limit:]])
    monkeypatch.setattr(mod.api, "get_open_interest", lambda s: {"openInterest": str(sc["oi_now"])})
    monkeypatch.setattr(mod, "get_oi_24h_ago", lambda s: sc["oi_24h_ago"])
    monkeypatch.setattr(mod, "_is_weekend_period", lambda now: False)
    monkeypatch.setattr(judges, "_funding_rate_velocity", lambda s: dict(sc["fv"]))
    monkeypatch.setattr(judges, "check_signal_consistency", lambda *a, **k: None)
    monkeypatch.setattr(state, "is_open_dir_cooldown", lambda s, d: bool(sc["cooldown"]) and d == "LONG")
    monkeypatch.setattr(mod, "_write_signal_analytics", lambda **kw: rec.analytics.append(_clean(kw)))
    monkeypatch.setattr(shadow_trace, "record_judge_rejection", lambda **kw: rec.shadow.append(_clean(kw)))
    monkeypatch.setattr(config, "OPT_FUNDING_WINDOW_PER_SYMBOL", False, raising=False)
    if llm is None:
        monkeypatch.setattr(config, "LLM_JUDGE_ENABLED", False, raising=False)
    else:
        from shared import llm_judge
        monkeypatch.setattr(config, "LLM_JUDGE_ENABLED", True, raising=False)
        monkeypatch.setattr(config, "LLM_JUDGE_EXPLOSIVE_COIN", True, raising=False)
        monkeypatch.setattr(config, "LLM_JUDGE_MIN_CONFIDENCE", 0.6, raising=False)
        monkeypatch.setattr(llm_judge, "judge_explosive_coin_llm", lambda *a, **k: dict(llm))
    for route, reason in (force or {}).items():      # 强制某条路线的 judge 通过，覆盖「出信号」分支
        monkeypatch.setattr(mod, f"judge_{route}", lambda *a, _r=reason, **k: (True, dict(_r)))
    monkeypatch.setattr(mod.time, "time", lambda: 1_700_000_100.0)


def run_impl(mod, sc, btc_trend="neutral"):
    return mod._analyze_symbol_impl(sc["symbol"], sc["ticker"], set(), set(), None, None, btc_trend, "range",
                                    None, {sc["symbol"]: dict(sc["mp"])})


def normalize(sig):
    return None if sig is None else _clean(sig)


FORCE_SETS = [None,
              {"pullback": {"signal_strength": "strong", "position_size_cap": 0.5}},
              {"pullback": {"signal_strength": "medium", "position_size_cap": 0.4},
               "range_bounce": {"signal_strength": "medium", "position_size_cap": 0.3}},
              {"reversal": {"signal_strength": "strong", "position_size_cap": 0.5, "direction": "short"},
               "breakout": {"signal_strength": "medium", "position_size_cap": 0.5, "breakout_direction": "long"},
               "stop_hunt": {"signal_strength": "strong", "position_size_cap": 0.5, "direction": "LONG", "sl_price_hint": 95.0}}]


import hashlib

LLM_CASES = {
    "llm_open": {"decision": "open", "confidence": 0.9, "reason": "蓄势"},
    "llm_veto": {"decision": "skip", "confidence": 0.9, "reason": "出货"},
    "llm_low_conf": {"decision": "open", "confidence": 0.3, "reason": "不确定"},
}


def case_key(seed, fi, llm_name=None):
    return f"{seed}:{fi}:{llm_name or '-'}"


def run_case(mod, seed, fi, llm_name=None, btc_trend="neutral"):
    """跑一个场景，返回 (指纹, 摘要)。指纹覆盖：信号 dict（去掉 ts）+ analytics 记录 + shadow 记录。"""
    import pytest
    mp = pytest.MonkeyPatch()
    try:
        sc = make_scenario(seed)
        rec = Rec()
        llm = LLM_CASES.get(llm_name)
        install(mod, sc, mp, rec, llm=llm, force=FORCE_SETS[fi])
        if llm is not None:
            from shared import llm_judge
            mp.setattr(llm_judge, "is_explosive_coin", lambda k1h: (True, 150.0, 2.0))
        sig = run_impl(mod, sc, btc_trend=btc_trend)
    finally:
        mp.undo()
    norm = normalize(sig)
    if norm:
        norm.pop("ts", None)
    blob = json.dumps({"sig": norm, "analytics": rec.analytics, "shadow": rec.shadow}, sort_keys=True)
    summary = None if norm is None else f"{norm['sig_type']}/{norm['direction']}"
    return hashlib.sha256(blob.encode()).hexdigest()[:20], summary


def all_cases():
    cs = [(s, fi, None) for s in range(36) for fi in range(len(FORCE_SETS))]
    cs += [(s, 1, n) for s in range(6) for n in LLM_CASES]
    cs += [(s, 3, None, "down") for s in range(6)]
    return cs
