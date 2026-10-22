"""阶段3.1：影子触发器（全 mock，不联网）。"""
import json
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import pytest

import shared.trigger_detector as td

M15, H, H4 = 900_000, 3_600_000, 14_400_000
T0 = 1_800_000_000.0


def _rows(closes, iv_ms, now_ms, highs=None, lows=None, vols=None):
    n = len(closes)
    t0 = int(now_ms - (n - 1) * iv_ms - iv_ms // 2)
    out = []
    for i, c in enumerate(closes):
        h = highs[i] if highs else c * 1.01
        l = lows[i] if lows else c * 0.99
        v = vols[i] if vols else 100.0
        out.append([t0 + i * iv_ms, c, h, l, c, v, t0 + (i + 1) * iv_ms - 1])
    return out


class FakeHub:
    def __init__(self, clk):
        self._kl_lock = threading.Lock()
        self.clk = clk
        self.k, self.mp, self.tk = {}, {}, {}

    @property
    def _series(self):
        return {key: None for key in self.k}

    def set(self, sym, c1h=None, price=100.0, chg24=0.0, funding=0.0, nft=0, hi24=200.0, lo24=1.0,
            k1h=None, k15=None, k4h=None):
        now_ms = self.clk.t * 1000
        if c1h is not None or k1h is not None:
            self.k[(sym, "1h")] = k1h or _rows(c1h, H, now_ms)
            self.k[(sym, "15m")] = k15 or _rows([100.0] * 100, M15, now_ms)
            self.k[(sym, "4h")] = k4h or _rows([100.0] * 100, H4, now_ms)
        self.mp[sym] = {"markPrice": price * 1.05, "lastFundingRate": funding, "nextFundingTime": nft,
                        "time": int(now_ms)}
        self.tk[sym] = {"lastPrice": str(price), "priceChangePercent": str(chg24),
                        "highPrice": str(hi24), "lowPrice": str(lo24), "age_sec": 0.2}

    def price(self, sym, p):
        self.tk[sym]["lastPrice"] = str(p)

    def chg(self, sym, c):
        self.tk[sym]["priceChangePercent"] = str(c)

    def get_klines(self, sym, iv, limit):
        r = self.k.get((sym, iv))
        if not r or len(r) < limit:
            return None
        return [list(x) for x in r[-limit:]]

    def get_mark_price(self, sym):
        return dict(self.mp[sym]) if sym in self.mp else None

    def get_ticker(self, sym):
        return dict(self.tk[sym]) if sym in self.tk else None


class Clock:
    def __init__(self, t=T0):
        self.t = t

    def __call__(self):
        return self.t


def advance(hub, sym, iv, close=None, high=None, low=None, vol=None):
    """追加一根新 K 线（上一根视为收盘），可改上一根（已收盘）的值。"""
    ms = {"15m": M15, "1h": H, "4h": H4}[iv]
    rows = hub.k[(sym, iv)]
    last = rows[-1]
    if close is not None:
        last[4] = close
    if high is not None:
        last[2] = high
    if low is not None:
        last[3] = low
    if vol is not None:
        last[5] = vol
    nxt = [last[0] + ms, last[4], last[4] * 1.01, last[4] * 0.99, last[4], 100.0, last[6] + ms]
    rows.append(nxt)
    rows.pop(0)


@pytest.fixture
def env(tmp_path, monkeypatch):
    ind = {"rsi": 50.0}
    monkeypatch.setattr(td, "_rsi", lambda c: ind["rsi"])
    clk = Clock()
    hub = FakeHub(clk)
    oi = {"v": (0.0, 0.0, 0.0)}
    pos = {"v": []}
    tradable = {"v": None}

    def exinfo():
        return tradable["v"] if tradable["v"] is not None else set(s for s, _ in hub.k) | set(hub.tk)

    def make(**kw):
        args = dict(hub=hub, clock=clk, oi_reader=lambda s: oi["v"], pool_reader=lambda: list(hub.tk),
                    exinfo_reader=exinfo, positions_reader=lambda: pos["v"],
                    trigger_log=tmp_path / "trigger_log.jsonl", scan_log=tmp_path / "scan_signals.jsonl",
                    state_file=tmp_path / "trigger_state.json")
        args.update(kw)
        return td.TriggerDetector(**args)

    return {"det": make(), "make": make, "hub": hub, "clk": clk, "ind": ind, "oi": oi, "pos": pos,
            "tradable": tradable, "tmp": tmp_path}


def poll(e, dt=1.0, det=None):
    e["clk"].t += dt
    return (det or e["det"]).poll_once()


def fired(recs, sym="AAAUSDT"):
    return {(r["strategy"], r["direction"]) for r in recs if r["symbol"] == sym}


def strats(recs, sym="AAAUSDT"):
    return {s for s, _ in fired(recs, sym)}


def flat(n=100, c=100.0):
    return [c] * n


# ═══════════════ 基础：首轮 / 就绪 / 分类 ═══════════════
def test_first_round_only_initializes(env):
    e = env
    e["ind"]["rsi"] = 20.0
    e["hub"].set("AAAUSDT", flat(), chg24=-30.0, funding=-0.01, nft=int((T0 + 600) * 1000))
    assert e["det"].poll_once() == []
    e["det"].flush()
    assert not (e["tmp"] / "trigger_log.jsonl").exists()
    assert "reversal" not in strats(poll(e))      # 条件持续成立，边沿不再触发


def test_symbol_not_covered_by_hub_skipped(env):
    e = env
    e["hub"].tk["BBBUSDT"] = {"lastPrice": None}            # 无价格
    e["hub"].k[("CCCUSDT", "1h")] = _rows(flat(5), H, T0 * 1000)   # K 线不足，且无 ticker
    e["det"].poll_once()
    assert poll(e) == []


def test_non_tradable_skipped(env):
    e = env
    e["tradable"]["v"] = set()
    e["hub"].set("AAAUSDT", None, chg24=0.0)
    e["det"].poll_once()
    e["hub"].chg("AAAUSDT", -20.0)
    assert poll(e) == []


def test_exinfo_unavailable_skips_everything(env):
    e = env
    det = e["make"](exinfo_reader=lambda: None)
    e["hub"].set("AAAUSDT", None)
    det.poll_once()
    e["hub"].chg("AAAUSDT", -20.0)
    assert poll(e, det=det) == []


def test_no_kline_symbol_only_three_strategies(env):
    e = env
    nft = int((T0 + 1200) * 1000)
    e["hub"].set("AAAUSDT", None, price=100.0, chg24=0.0, hi24=101.0, lo24=50.0, nft=nft)
    e["det"].poll_once()
    e["hub"].chg("AAAUSDT", 20.0)                       # reversal SHORT（无 RSI）
    e["hub"].mp["AAAUSDT"]["lastFundingRate"] = 0.001   # funding_arb SHORT
    e["oi"]["v"] = (90.0, 100.0, 0.0)                   # OI -10% + 近高点 → oi_divergence SHORT
    got = strats(poll(e, 301))
    assert got == {"reversal", "funding_arb", "oi_divergence"}


def test_hub_not_ready_bars_insufficient(env):
    e = env
    now_ms = T0 * 1000
    # 15m 只有 60 根（>=50 算有 K 线，但 <100 未就绪）→ 只跑 3 种；breakout 不触发
    e["hub"].set("AAAUSDT", flat(), price=100.0, chg24=6.0, k15=_rows(flat(60), M15, now_ms))
    e["det"].poll_once()
    e["hub"].price("AAAUSDT", 102.0)
    assert "breakout" not in strats(poll(e))


def test_global_gate_opens_after_120s(env):
    e = env
    now_ms = T0 * 1000
    e["hub"].set("AAAUSDT", flat(), chg24=6.0)
    for s in ("B1", "B2", "B3", "B4"):      # 4/5 未就绪 → 就绪比例 20%
        e["hub"].set(s, flat(), k15=_rows(flat(60), M15, now_ms))
    e["det"].poll_once()
    assert e["det"].gate_open is False
    poll(e, 121)
    assert e["det"].gate_open is True


# ═══════════════ 8 种策略 ═══════════════
def test_pullback_on_15m_close_asymmetric_zone(env):
    e = env
    e["hub"].set("AAAUSDT", flat(), price=100.0)        # ema20=ema50=100，atr≈2 → LONG [97, 100.6]
    e["det"].poll_once()
    assert "pullback" not in strats(poll(e))            # 无收盘
    advance(e["hub"], "AAAUSDT", "15m")
    assert ("pullback", "LONG") in fired(poll(e))
    e["hub"].price("AAAUSDT", 101.0)                    # 超过 +0.3ATR
    advance(e["hub"], "AAAUSDT", "15m")
    assert "pullback" not in strats(poll(e, 1000))
    e["hub"].price("AAAUSDT", 97.2)                     # 在 -1.5ATR 内
    advance(e["hub"], "AAAUSDT", "15m")
    assert "pullback" in strats(poll(e, 1000))


def test_reversal_chg24_long(env):
    e = env
    e["hub"].set("AAAUSDT", flat(), chg24=0.0)
    e["det"].poll_once()
    e["hub"].chg("AAAUSDT", -11.0)
    assert ("reversal", "LONG") in fired(poll(e))


def test_reversal_long_and_short_independent(env):
    e = env
    e["hub"].set("AAAUSDT", None, chg24=0.0, funding=0.0)
    e["det"].poll_once()
    e["hub"].chg("AAAUSDT", -12.0)                       # LONG 条件
    e["hub"].mp["AAAUSDT"]["lastFundingRate"] = 0.0008   # SHORT 条件
    got = fired(poll(e))
    assert ("reversal", "LONG") in got and ("reversal", "SHORT") in got


def test_reversal_uses_closed_1h_rsi(env):
    e = env
    e["hub"].set("AAAUSDT", flat())
    e["det"].poll_once()
    e["ind"]["rsi"] = 20.0                               # 未收盘不重算
    assert "reversal" not in strats(poll(e))
    advance(e["hub"], "AAAUSDT", "1h")
    assert ("reversal", "LONG") in fired(poll(e))


def test_rsi_hysteresis(env):
    e = env
    e["hub"].set("AAAUSDT", flat())
    e["det"].poll_once()

    def step(r):
        e["ind"]["rsi"] = r
        advance(e["hub"], "AAAUSDT", "1h")
        advance(e["hub"], "AAAUSDT", "15m")      # 保持 15m 新鲜（30 分钟内）
        return ("reversal", "LONG") in fired(poll(e, 1000))

    assert step(31.0) is True        # <32 触发
    assert step(34.0) is False       # 34 不复位，也不重复
    assert step(31.0) is False       # 未复位
    assert step(37.0) is False       # >36 复位
    assert step(31.0) is True        # 复位后再触发


def test_chg24_hysteresis(env):
    e = env
    e["hub"].set("AAAUSDT", None, chg24=0.0)
    e["det"].poll_once()

    def step(c):
        e["hub"].chg("AAAUSDT", c)
        return ("reversal", "LONG") in fired(poll(e, 1000))

    assert step(-11.0) is True
    assert step(-9.0) is False       # 8%~10% 之间不复位
    assert step(-11.0) is False
    assert step(-7.0) is False       # <8% 复位
    assert step(-11.0) is True


def test_breakout_direction_matches_chg24(env):
    e = env
    e["hub"].set("AAAUSDT", flat(), price=100.0, chg24=4.0)   # h20=101，atr≈2 → 距离阈值 0.6
    e["det"].poll_once()
    e["hub"].price("AAAUSDT", 100.5)
    assert ("breakout", "LONG") in fired(poll(e))
    det2 = e["make"]()
    e["hub"].set("BBBUSDT", flat(), price=100.0, chg24=-4.0)
    det2.poll_once()
    e["hub"].price("BBBUSDT", 100.5)                       # 向上但 chg24<0 → 不算
    assert "breakout" not in strats(poll(e, det=det2), "BBBUSDT")


def test_breakout_by_closed_1h_close(env):
    e = env
    e["hub"].set("AAAUSDT", flat(), price=101.0, chg24=-4.0)  # l20=99，初始未满足
    e["det"].poll_once()
    advance(e["hub"], "AAAUSDT", "1h", close=98.0)          # 已收盘 close <= l20
    e["hub"].price("AAAUSDT", 99.5)                         # 当前价距 l20 0.5 ≤ 0.6 也满足
    assert ("breakout", "SHORT") in fired(poll(e))


def test_funding_arb_window_scales_with_period(env):
    e = env
    now_ms = int(T0 * 1000)
    e["hub"].set("AAAUSDT", None, funding=0.0, nft=now_ms + 3_600_000)
    e["det"].poll_once()
    e["hub"].mp["AAAUSDT"]["nextFundingTime"] = now_ms + 2 * 3_600_000 - 2 * 60_000  # 周期 1h 推断
    e["hub"].mp["AAAUSDT"]["lastFundingRate"] = -0.001
    e["clk"].t += 1
    e["hub"].mp["AAAUSDT"]["nextFundingTime"] = int(e["clk"].t * 1000) + 40 * 60_000
    e["det"]._fund["AAAUSDT"] = {"nft": int(e["clk"].t * 1000) - 20 * 60_000, "period": 3_600_000}
    assert "funding_arb" not in strats(poll(e, 301))        # 1H 周期：40min > 30min
    e["hub"].mp["AAAUSDT"]["nextFundingTime"] = int(e["clk"].t * 1000) + 20 * 60_000
    e["det"]._fund["AAAUSDT"] = {"nft": int(e["clk"].t * 1000) - 40 * 60_000, "period": 3_600_000}
    assert ("funding_arb", "LONG") in fired(poll(e, 301))


def test_funding_arb_8h_window_and_interval(env):
    e = env
    e["hub"].set("AAAUSDT", None, funding=0.0, nft=int((T0 + 2 * 3600) * 1000))
    e["det"].poll_once()
    e["hub"].mp["AAAUSDT"]["lastFundingRate"] = 0.0008
    assert "funding_arb" not in strats(poll(e, 10))           # 未到 5 分钟
    assert ("funding_arb", "SHORT") in fired(poll(e, 300))   # 默认 8H：2h ≤ 3h


def test_range_bounce_amplitude_scales_with_gain(env):
    e = env
    base = [100.0] * 74
    # 前期涨 20%（100→120），横盘振幅 10% ≤ 20%×0.6=12% → 触发
    c = base + [104, 108, 112, 116, 120, 118, 116, 114, 112, 110, 109, 110, 111, 112, 113,
                112, 111, 110, 109, 110, 111, 112, 111, 110, 111, 0]
    k1h = _rows(c, H, T0 * 1000, highs=[x * 1.02 for x in c], lows=[x * 0.98 for x in c])
    e["hub"].set("AAAUSDT", k1h=k1h, price=112.0)
    e["det"].poll_once()
    advance(e["hub"], "AAAUSDT", "1h", close=111.0, high=113.0, low=109.0)
    assert ("range_bounce", "LONG") in fired(poll(e))
    # 同形态但横盘振幅 15% > 12% → 不触发
    c2 = list(c)
    c2[-12] = 104.0
    det2 = e["make"]()
    k1h2 = _rows(c2, H, e["clk"].t * 1000, highs=[x * 1.02 for x in c2], lows=[x * 0.98 for x in c2])
    e["hub"].set("BBBUSDT", k1h=k1h2, price=106.0)
    det2.poll_once()
    advance(e["hub"], "BBBUSDT", "1h", close=111.0, high=113.0, low=109.0)
    assert "range_bounce" not in strats(poll(e, det=det2), "BBBUSDT")


def _stop_hunt_hub(e, sym="AAAUSDT"):
    c = [100.0] * 80 + [105, 104, 103, 102, 95, 101, 102, 103, 104, 105, 104, 105, 104, 105, 104, 105, 104, 105, 104, 105]
    now_ms = e["clk"].t * 1000
    e["hub"].set(sym, k1h=_rows(c, H, now_ms), k15=_rows(flat(), M15, now_ms))


def test_stop_hunt_pierce_low(env):
    e = env
    _stop_hunt_hub(e)
    e["det"].poll_once()
    k15 = e["hub"].k[("AAAUSDT", "15m")]
    k15[-1][3], k15[-1][5] = 94.0, 300.0                    # 实时帧刺穿 pivot 95，量比 3x
    got = poll(e)
    assert ("stop_hunt", "LONG") in fired(got)


def test_stop_hunt_pierce_threshold_and_cooldown_key(env):
    e = env
    _stop_hunt_hub(e)
    e["det"].poll_once()
    k15 = e["hub"].k[("AAAUSDT", "15m")]
    k15[-1][3], k15[-1][5] = 94.95, 300.0                   # 刺穿 0.05 < max(0.1ATR, 0.15%×95)
    assert "stop_hunt" not in strats(poll(e))
    k15[-1][3] = 94.0
    poll(e)
    keys = [k for k in e["det"]._st if k[1] == "stop_hunt" and k[2].startswith("LONG|")]
    pot = e["det"]._ind["AAAUSDT"]["pivots"]["low_ot"]
    assert keys and keys[0][2] == f"LONG|{pot}" and isinstance(pot, int)
    assert e["det"].is_cooling_down("AAAUSDT", "stop_hunt", "LONG")


def test_oi_divergence_from_cache(env):
    e = env
    e["hub"].set("AAAUSDT", flat(), price=100.0, hi24=102.0, lo24=80.0)
    e["det"].poll_once()
    e["oi"]["v"] = (96.0, 100.0, 0.0)                       # 1H -4% + 距高点 1.96%
    recs = poll(e)
    assert ("oi_divergence", "SHORT") in fired(recs)
    assert "oi_src=cache" in [r for r in recs if r["strategy"] == "oi_divergence"][0]["reason"]


def test_ema_trend_long_on_4h_close(env):
    e = env
    c4 = [80.0 + i * 0.4 for i in range(100)]
    now_ms = e["clk"].t * 1000
    e["hub"].set("AAAUSDT", flat(c=c4[-2]), k4h=_rows(c4, H4, now_ms), price=c4[-2], chg24=1.0)
    e["ind"]["rsi"] = 55.0
    e["det"].poll_once()
    advance(e["hub"], "AAAUSDT", "4h")
    e20 = td._ema([float(k[4]) for k in e["hub"].k[("AAAUSDT", "4h")][:-1]], 20)
    e["hub"].price("AAAUSDT", e20 * 1.01)
    assert ("ema_trend", "LONG") in fired(poll(e))


def test_ema_trend_short_branch(env):
    e = env
    c4 = [120.0 - i * 0.4 for i in range(100)]
    now_ms = e["clk"].t * 1000
    e["hub"].set("AAAUSDT", flat(c=c4[-2]), k4h=_rows(c4, H4, now_ms), price=c4[-2], chg24=-1.0)
    e["ind"]["rsi"] = 50.0
    e["det"].poll_once()
    advance(e["hub"], "AAAUSDT", "4h")
    e20 = td._ema([float(k[4]) for k in e["hub"].k[("AAAUSDT", "4h")][:-1]], 20)
    e["hub"].price("AAAUSDT", e20 * 1.02)                   # -3%~+2.5% 内
    assert ("ema_trend", "SHORT") in fired(poll(e))


def test_ema_trend_inner_retrigger_on_1h_close(env):
    e = env
    c4 = [80.0 + i * 0.4 for i in range(100)]
    now_ms = e["clk"].t * 1000
    e["hub"].set("AAAUSDT", flat(c=c4[-2]), k4h=_rows(c4, H4, now_ms), price=c4[-2], chg24=1.0)
    e["ind"]["rsi"] = 55.0
    e["det"].poll_once()
    advance(e["hub"], "AAAUSDT", "4h")
    e20 = td._ema([float(k[4]) for k in e["hub"].k[("AAAUSDT", "4h")][:-1]], 20)
    e["hub"].price("AAAUSDT", e20 * 1.02)                   # 外圈 2%
    assert ("ema_trend", "LONG") in fired(poll(e))
    e["hub"].price("AAAUSDT", e20 * 1.005)                  # 进内圈，但无 1H 收盘
    assert "ema_trend" not in strats(poll(e, 60))
    advance(e["hub"], "AAAUSDT", "1h")
    got = poll(e, 60)
    assert ("ema_trend", "LONG") in fired(got)
    assert "inner_retrigger" in [r for r in got if r["strategy"] == "ema_trend"][0]["reason"]
    advance(e["hub"], "AAAUSDT", "1h")
    assert "ema_trend" not in strats(poll(e, 60))           # 每个冷却周期只一次


# ═══════════════ 边沿 / 冷却 / 持久化 ═══════════════
def test_edge_no_repeat_while_condition_holds(env):
    e = env
    e["hub"].set("AAAUSDT", None, chg24=0.0)
    e["det"].poll_once()
    e["hub"].chg("AAAUSDT", -20.0)
    assert "reversal" in strats(poll(e))
    assert "reversal" not in strats(poll(e, 5000))


def test_cooldown(env):
    e = env
    e["hub"].set("AAAUSDT", None, chg24=0.0)
    e["det"].poll_once()

    def step(c, dt):
        e["hub"].chg("AAAUSDT", c)
        return "reversal" in strats(poll(e, dt))

    assert step(-11.0, 1) is True
    assert step(-5.0, 1) is False
    assert step(-11.0, 100) is False         # 900s 冷却内
    assert step(-5.0, 1) is False
    assert step(-11.0, 900) is True


def test_state_persist_and_startup_existing(env):
    e = env
    e["hub"].set("AAAUSDT", None, chg24=0.0)
    e["hub"].set("BBBUSDT", None, chg24=0.0)
    e["det"].poll_once()
    e["hub"].chg("AAAUSDT", -11.0)
    assert "reversal" in strats(poll(e))
    e["det"].save_state()
    assert (e["tmp"] / "trigger_state.json").exists()
    # 重启：AAA 仍在冷却 → 不触发；BBB 重启前未满足、重启后已满足 → startup_existing
    e["hub"].chg("BBBUSDT", -11.0)
    det2 = e["make"]()
    assert det2.load_state() is True
    assert det2.is_cooling_down("AAAUSDT", "reversal", "LONG")
    e["clk"].t += 1
    got = det2.poll_once()
    assert "reversal" not in strats(got, "AAAUSDT")
    b = [r for r in got if r["symbol"] == "BBBUSDT" and r["strategy"] == "reversal"]
    assert b and b[0]["startup_existing"] is True


def test_startup_existing_token_cap(env, monkeypatch):
    e = env
    monkeypatch.setattr(td, "STARTUP_TOKEN_MAX", 2)
    for i in range(5):
        e["hub"].set(f"S{i}USDT", None, chg24=-11.0)
    (e["tmp"] / "trigger_state.json").write_text(json.dumps({"st": [], "fund": {}}))
    det = e["make"]()
    det.load_state()
    got = det.poll_once()
    assert sum(1 for r in got if r["startup_existing"]) == 2
    assert len(det._queue) == 3


# ═══════════════ 过滤 / 令牌桶 / 输出 ═══════════════
def test_position_filter_same_and_hedge(env, caplog):
    e = env
    e["pos"]["v"] = [{"symbol": "AAAUSDT", "direction": "LONG", "status": "open"},
                     {"symbol": "BBBUSDT", "direction": "SHORT", "status": "open"}]
    e["hub"].set("AAAUSDT", None, chg24=0.0)
    e["hub"].set("BBBUSDT", None, chg24=0.0)
    e["det"].poll_once()
    e["hub"].chg("AAAUSDT", -11.0)
    e["hub"].chg("BBBUSDT", -11.0)
    with caplog.at_level("INFO", logger="fx.trigger"):
        got = poll(e)
    assert not [r for r in got if r["strategy"] == "reversal"]
    assert any("AAAUSDT reversal LONG suppressed:same_dir" in m for m in caplog.messages)
    assert any("BBBUSDT reversal LONG suppressed:hedge_risk" in m for m in caplog.messages)


def test_pending_signal_and_analyzing_filter(env, caplog):
    e = env

    class W:
        def pending_symbols(self):
            return ["AAAUSDT"]

    det = e["make"](signal_watcher=W())
    e["hub"].set("AAAUSDT", None, chg24=0.0)
    e["hub"].set("BBBUSDT", None, chg24=0.0)
    det.poll_once()
    det.mark_analyzing("BBBUSDT")
    assert det.is_analyzing("BBBUSDT")
    e["hub"].chg("AAAUSDT", -11.0)
    e["hub"].chg("BBBUSDT", -11.0)
    with caplog.at_level("INFO", logger="fx.trigger"):
        assert poll(e, det=det) == []
    assert any("suppressed:pending_signal" in m for m in caplog.messages)
    assert any("suppressed:analyzing" in m for m in caplog.messages)
    det.clear_analyzing("BBBUSDT")
    assert not det.is_analyzing("BBBUSDT")


def test_positions_refresh_every_30s(env):
    e = env
    calls = {"n": 0}

    def reader():
        calls["n"] += 1
        return []

    det = e["make"](positions_reader=reader)
    e["hub"].set("AAAUSDT", None)
    for _ in range(10):
        poll(e, 1, det=det)
    assert calls["n"] == 1
    poll(e, 30, det=det)
    assert calls["n"] == 2


def test_token_bucket_strategy_cap_and_expiry(env, monkeypatch):
    e = env
    monkeypatch.setattr(td, "_cfg", lambda n, d: {"TRIGGER_TOKEN_CAPACITY": 5, "TRIGGER_TOKEN_RATE": 0,
                                                    "TRIGGER_TOKEN_TTL": 300,
                                                    "TRIGGER_TOKEN_STRATEGY_MAX_PCT": 0.4}.get(n, d))
    det = e["make"]()
    for i in range(4):
        e["hub"].set(f"S{i}USDT", None, chg24=0.0)
    det.poll_once()
    for i in range(4):
        e["hub"].chg(f"S{i}USDT", -11.0)
    got = poll(e, det=det)
    assert len(got) == 2 and all(r["token_result"] == "allowed" for r in got)   # 5×40%=2
    poll(e, 301, det=det)
    det.flush()
    lines = [json.loads(x) for x in (e["tmp"] / "trigger_log.jsonl").read_text().splitlines()]
    assert sum(1 for r in lines if r.get("token_result") == "expired") == 2


def test_trigger_log_fields_and_size_warning(env, caplog, monkeypatch):
    e = env
    monkeypatch.setattr(td, "LOG_WARN_BYTES", 10)
    det = e["make"]()
    e["hub"].set("AAAUSDT", flat())
    det.poll_once()
    e["hub"].chg("AAAUSDT", -11.0)
    with caplog.at_level("INFO", logger="fx.trigger"):
        poll(e, det=det)
        det.flush()
    rec = json.loads((e["tmp"] / "trigger_log.jsonl").read_text().splitlines()[0])
    assert set(rec) == {"trigger_id", "ts", "exchange_ts", "symbol", "strategy", "direction", "reason", "price",
                        "config_version", "startup_existing", "suppressed", "token_result", "data_freshness_ms",
                        "hub_snapshot", "post_price"}
    assert set(rec["hub_snapshot"]) == {"chg24", "rsi_1h", "ema20_1h", "atr_1h_pct", "funding",
                                        "last_kline_close_time"}
    assert rec["post_price"] == {"15m": None, "1h": None, "4h": None, "max_favorable": None, "max_adverse": None}
    assert rec["price"] == 100.0                     # lastPrice，不是 markPrice(105)
    assert rec["exchange_ts"] == e["hub"].mp["AAAUSDT"]["time"]
    assert rec["config_version"].startswith("phase3.1+") and rec["token_result"] == "allowed"
    assert any("[trigger] AAAUSDT reversal LONG" in m for m in caplog.messages)
    assert any("超过 10MB" in m for m in caplog.messages)


def test_post_price_backfill(env):
    e = env
    e["hub"].set("AAAUSDT", None, chg24=0.0, price=100.0)
    e["det"].poll_once()
    e["hub"].chg("AAAUSDT", -11.0)
    tid = [r for r in poll(e) if r["strategy"] == "reversal"][0]["trigger_id"]
    for p, dt in ((103.0, 300), (98.0, 300), (101.0, 300)):      # 15m 到点 101
        e["hub"].price("AAAUSDT", p)
        e["clk"].t += dt
        e["det"].update_post_prices()
    e["hub"].price("AAAUSDT", 105.0)
    e["clk"].t += 2700
    e["det"].update_post_prices()                                # 1h
    e["clk"].t += 3 * 3600
    e["det"].update_post_prices()                                # 4h
    e["det"].flush()
    lines = [json.loads(x) for x in (e["tmp"] / "trigger_log.jsonl").read_text().splitlines()]
    orig = [r for r in lines if r.get("trigger_id") == tid and "type" not in r][0]
    assert orig["post_price"]["15m"] is None                     # 原行不改
    pp = [r for r in lines if r.get("type") == "post_price" and r["trigger_id"] == tid]
    assert [r["horizon"] for r in pp] == ["15m", "1h", "4h"]
    last = pp[-1]["post_price"]
    assert last["15m"] == 101.0 and last["1h"] == 105.0 and last["4h"] == 105.0
    assert last["max_favorable"] == pytest.approx(0.05) and last["max_adverse"] == pytest.approx(-0.02)


def test_write_scan_signals(tmp_path):
    p = tmp_path / "s.jsonl"
    td.write_scan_signals([], path=p, ts_ms=1)
    td.write_scan_signals([{"symbol": "A", "sig_type": "pullback", "direction": "LONG", "route_quality": 70}],
                          path=p, ts_ms=2)
    lines = [json.loads(x) for x in p.read_text().splitlines()]
    assert lines[0] == {"ts": 1, "signals": []}
    assert lines[1] == {"ts": 2, "symbol": "A", "strategy": "pullback", "direction": "LONG", "route_quality": 70}


# ═══════════════ 对账 ═══════════════
def test_reconcile_miss_type1_and_fp(env, caplog):
    e = env
    det = e["det"]
    e["hub"].set("AAAUSDT", None, chg24=0.0)
    det.poll_once()
    td.write_scan_signals([
        {"symbol": "ZZZUSDT", "sig_type": "breakout", "direction": "LONG", "route_quality": 60},
        {"symbol": "FFFUSDT", "sig_type": "funding_arb", "direction": "LONG", "route_quality": 50},
    ], path=det._slog, ts_ms=int(e["clk"].t * 1000))
    e["hub"].chg("AAAUSDT", -11.0)
    with caplog.at_level("INFO", logger="fx.trigger"):
        poll(e)
        assert det.stats["miss"] == 1
        poll(e, 901)
    assert det.stats["fp"] == 1
    assert any("[trigger-miss-type1] ZZZUSDT breakout" in m for m in caplog.messages)
    assert any("[trigger-fp] AAAUSDT reversal" in m for m in caplog.messages)
    assert not any("FFFUSDT" in m for m in caplog.messages)


def test_reconcile_symbol_level_match(env):
    e = env
    det = e["det"]
    e["hub"].set("AAAUSDT", None, chg24=0.0)
    det.poll_once()
    e["hub"].chg("AAAUSDT", -11.0)
    poll(e)
    td.write_scan_signals([{"symbol": "AAAUSDT", "sig_type": "pullback", "direction": "LONG",
                            "route_quality": 70}], path=det._slog, ts_ms=int(e["clk"].t * 1000))
    poll(e)
    poll(e, 2000)
    assert det.stats["miss"] == 0 and det.stats["fp"] == 0


def test_reconcile_oi_counted_separately(env, caplog):
    e = env
    det = e["det"]
    e["hub"].set("AAAUSDT", None)
    det.poll_once()
    td.write_scan_signals([{"symbol": "QQQUSDT", "sig_type": "oi_divergence", "direction": "SHORT",
                            "route_quality": 55}], path=det._slog, ts_ms=int(e["clk"].t * 1000))
    with caplog.at_level("INFO", logger="fx.trigger"):
        poll(e)
    assert det.stats["oi_miss"] == 1
    assert any("OI来自缓存" in m for m in caplog.messages)


def test_disabled_by_default():
    import config
    assert config.TRIGGER_DETECTOR_ENABLED is False
    td._default = None
    assert td.create() is None
