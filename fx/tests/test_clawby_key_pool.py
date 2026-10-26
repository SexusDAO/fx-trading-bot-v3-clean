"""Clawby 多 key 轮换池：加载、round-robin、单 key 冷却、全部冷却、限速缩放、无 key 报错。全部 mock，不联网。"""
import pytest
import shared.clawby_adapter as C


class Resp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code = status; self._b = body if body is not None else {}; self.headers = headers or {}
    def json(self):
        return self._b
    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


OK = {"data": {"code": "0", "data": [1]}}
LIMITED = {"code": "rate_limited", "retry_after": 1, "limit": 5}


@pytest.fixture(autouse=True)
def _restore():
    old = list(C._CLAWBY_KEYS)
    yield
    C._configure_keys(old, announce=False)


@pytest.fixture
def net(monkeypatch):
    """记录每次请求用的 key；响应序列由 net.seq 控制（缺省全部 200）。"""
    class N: pass
    n = N(); n.used = []; n.seq = []; n.sleeps = []
    def post(url, headers=None, **k):
        n.used.append(headers["X-API-Key"])
        return n.seq.pop(0) if n.seq else Resp(200, OK)
    monkeypatch.setattr(C.requests, "post", post)
    monkeypatch.setattr(C, "_wait_clawby_rate_limit", lambda: None)
    monkeypatch.setattr(C, "_CLAWBY_402_UNTIL", 0.0)
    monkeypatch.setattr(C.time, "sleep", lambda x: n.sleeps.append(x))
    return n


# ── 加载 ──
def _env(monkeypatch, **kv):
    for i in range(1, 20):
        monkeypatch.delenv(f"CLAWBY_API_KEY_{i}", raising=False)
    monkeypatch.delenv("CLAWBY_API_KEY", raising=False)
    for k, v in kv.items():
        monkeypatch.setenv(k, v)


def test_load_numbered_keys_preferred_and_deduped(monkeypatch):
    _env(monkeypatch, CLAWBY_API_KEY_1="a", CLAWBY_API_KEY_2="b", CLAWBY_API_KEY_3="b",
         CLAWBY_API_KEY_4=" c ", CLAWBY_API_KEY="single")
    assert C._load_clawby_keys() == ["a", "b", "c"]


def test_load_single_key_compat(monkeypatch):
    _env(monkeypatch, CLAWBY_API_KEY="only")
    assert C._load_clawby_keys() == ["only"]


def test_load_nothing(monkeypatch):
    _env(monkeypatch)
    assert C._load_clawby_keys() == []


# ── 限速缩放 ──
@pytest.mark.parametrize("n,expect", [(0, 0.25), (1, 0.25), (2, 0.125), (3, 0.0833), (4, 0.0625), (5, 0.05)])
def test_rate_limit_scales_with_key_count(n, expect):
    C._configure_keys([f"k{i}" for i in range(n)], announce=False)
    assert C.CLAWBY_RATE_LIMIT_S == expect


def test_startup_log_line(caplog):
    import logging
    with caplog.at_level(logging.INFO, logger="fx.clawby"):
        C._configure_keys(["a", "b", "c", "d"])
    assert "已加载 4 个 key，限速间隔 0.0625s/次，等效 ~16 次/秒" in caplog.text


# ── 无 key ──
def test_no_keys_raises_clear_environment_error(net):
    C._configure_keys([], announce=False)
    with pytest.raises(EnvironmentError) as e:
        C.clawby_relay("futures_cvd_history", {})
    assert "CLAWBY_API_KEY_1" in str(e.value) and "CLAWBY_API_KEY" in str(e.value)
    assert net.used == []                                    # 没发出任何请求
    with pytest.raises(EnvironmentError):
        C.validate_clawby_config()


def test_validate_returns_count():
    C._configure_keys(["a", "b"], announce=False)
    assert C.validate_clawby_config() == 2


def test_cvd_degrades_to_cg_when_no_keys(monkeypatch):
    C._configure_keys([], announce=False)
    monkeypatch.setattr(C, "_cache_get", lambda k: None); monkeypatch.setattr(C, "_cache_set", lambda k, v: None)
    monkeypatch.setattr(C, "cg_get", lambda *a, **k: [{"time": 1, "taker_buy_vol": "2", "taker_sell_vol": "1", "cum_vol_delta": "1"}])
    assert C.get_cvd_history("ARB")[0]["taker_buy_vol"] == 2.0     # 无 key 时由 CG 兜底，不抛


# ── round-robin ──
def test_single_key_behaves_as_before(net):
    C._configure_keys(["only"], announce=False)
    for _ in range(3):
        assert C.clawby_relay("x", {}) == [1]
    assert net.used == ["only"] * 3


def test_round_robin_uses_keys_in_turn(net):
    C._configure_keys(["a", "b", "c"], announce=False)
    for _ in range(7):
        C.clawby_relay("x", {})
    assert net.used == ["a", "b", "c", "a", "b", "c", "a"]


# ── 单 key 429 → 只冷却该 key，立即换 key，不计重试次数 ──
def test_429_cools_only_that_key_and_switches(net, caplog):
    import logging
    C._configure_keys(["a", "b", "c"], announce=False)
    net.seq = [Resp(429, LIMITED), Resp(200, OK)]
    with caplog.at_level(logging.WARNING, logger="fx.clawby"):
        assert C.clawby_relay("x", {}, _max_retries=0) == [1]        # max_retries=0 也能换 key 成功
    assert net.used == ["a", "b"]
    assert net.sleeps == []                                            # 换 key 不睡眠
    assert "key#1 触发429，冷却1.0s，切换下一个key重试" in caplog.text
    assert C._key_cooldown["a"] > C.time.time() and "b" not in C._key_cooldown
    for _ in range(4):                                                 # 冷却期内 a 被跳过，b/c 照常
        C.clawby_relay("x", {})
    assert "a" not in net.used[2:]


def test_cooldown_expires_and_key_returns(net, monkeypatch):
    C._configure_keys(["a", "b"], announce=False)
    C._mark_key_cooldown("a", 1.0)
    assert C._get_available_key() == "b"
    real = C.time.time()
    monkeypatch.setattr(C.time, "time", lambda: real + 2.0)
    got = {C._get_available_key(), C._get_available_key()}
    assert got == {"a", "b"}


def test_cooldown_uses_body_retry_after_capped_at_5(net):
    C._configure_keys(["a", "b"], announce=False)
    net.seq = [Resp(429, {"retry_after": 999}), Resp(200, OK)]
    C.clawby_relay("x", {})
    remaining = C._key_cooldown["a"] - C.time.time()
    assert 4.0 < remaining <= 5.0


# ── 全部冷却：等最快恢复的 key，不报错 ──
def test_all_cooling_waits_for_soonest(net):
    C._configure_keys(["a", "b"], announce=False)
    C._mark_key_cooldown("a", 3.0); C._mark_key_cooldown("b", 1.0)
    key = C._get_available_key()
    assert key == "b" and len(net.sleeps) == 1 and 0.5 < net.sleeps[0] <= 1.0


def test_all_cooling_nonblocking_returns_none():
    C._configure_keys(["a", "b"], announce=False)
    C._mark_key_cooldown("a", 3.0); C._mark_key_cooldown("b", 3.0)
    assert C._get_available_key(block=False) is None


def test_all_keys_429_then_wait_and_succeed(net):
    C._configure_keys(["a", "b"], announce=False)
    net.seq = [Resp(429, LIMITED), Resp(429, LIMITED), Resp(200, OK)]
    assert C.clawby_relay("x", {}) == [1]                  # 默认 3 次重试：换 key → 全冷却 → 等最快恢复 → 成功
    assert net.used[:2] == ["a", "b"] and len(net.used) == 3
    assert len(net.sleeps) == 1 and net.sleeps[0] <= 1.0   # 只等最快恢复的那个 key，≤1s


def test_all_keys_429_with_zero_retries_raises_without_sleep(net):
    C._configure_keys(["a", "b"], announce=False)
    net.seq = [Resp(429, LIMITED), Resp(429, LIMITED)]
    with pytest.raises(RuntimeError):
        C.clawby_relay("x", {}, _max_retries=0)
    assert net.sleeps == []                                 # CVD/爆仓历史路径不阻塞，立刻交给 CG


def test_single_key_429_retry_sleeps_body_value(net):
    C._configure_keys(["only"], announce=False)
    net.seq = [Resp(429, LIMITED), Resp(200, OK)]
    assert C.clawby_relay("x", {}) == [1]
    assert net.used == ["only", "only"] and net.sleeps == [pytest.approx(1.0, abs=0.05)]


def test_headers_carry_per_request_key(net):
    C._configure_keys(["a", "b"], announce=False)
    h = C._clawby_headers("b")
    assert h["X-API-Key"] == "b" and h["Content-Type"] == "application/json" and "X-API-Key" not in C.CLAWBY_HEADERS_BASE
