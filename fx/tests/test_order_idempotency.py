"""下单幂等：place_sl 带 clientAlgoId、place_batch_orders 子单带 newClientOrderId，
写超时/重发时优先按 id 对账。全部 mock，不联网。"""
import json
import logging

import pytest

from shared import binance_api as A


@pytest.fixture(autouse=True)
def _quiet(monkeypatch):
    logging.disable(logging.CRITICAL)
    monkeypatch.setattr(A.time, "sleep", lambda *_: None)
    monkeypatch.setattr(A, "reset_session", lambda *a, **k: None)
    monkeypatch.setattr(A, "is_hedge_mode", lambda: True)
    yield
    logging.disable(logging.NOTSET)


# ── place_sl ───────────────────────────────────────────────────────────────
def test_place_sl_sends_client_algo_id(monkeypatch):
    seen = []
    monkeypatch.setattr(A, "_signed_post", lambda path, p: seen.append((path, dict(p))) or {"algoId": 11})
    r = A.place_sl("BTCUSDT", "SELL", "LONG", 100.0, 1.0)
    assert r["algoId"] == 11 and r["orderId"] == 11
    assert seen[0][0] == "/fapi/v1/algoOrder"
    assert seen[0][1]["clientAlgoId"].startswith("fxsl")


def test_place_sl_ids_unique_per_call(monkeypatch):
    ids = []
    monkeypatch.setattr(A, "_signed_post", lambda path, p: ids.append(p["clientAlgoId"]) or {"algoId": 1})
    A.place_sl("BTCUSDT", "SELL", "LONG", 100.0, 1.0)
    A.place_sl("BTCUSDT", "SELL", "LONG", 100.0, 1.0)
    assert len(set(ids)) == 2


def test_place_sl_resend_checks_client_id_first(monkeypatch):
    """首次发送抛连接错误（结果未知），重发前按 clientAlgoId 查到已提交 → 不再 POST。"""
    posts = []
    state = {}

    def post(path, p):
        posts.append(p["clientAlgoId"])
        state["cid"] = p["clientAlgoId"]
        raise RuntimeError("ConnectionError: Max retries exceeded")

    def get(path, p=None):
        assert path == "/fapi/v1/openAlgoOrders"
        return {"orders": [{"clientAlgoId": state["cid"], "algoId": 777}]}

    monkeypatch.setattr(A, "_signed_post", post)
    monkeypatch.setattr(A, "_signed_get", get)
    r = A.place_sl("BTCUSDT", "SELL", "LONG", 100.0, 1.0)
    assert r["algoId"] == 777 and r.get("_recovered")
    assert len(posts) == 1          # 没有重复 POST


def test_place_sl_resend_when_not_found_posts_same_id(monkeypatch):
    posts = []

    def post(path, p):
        posts.append(p["clientAlgoId"])
        if len(posts) == 1:
            raise RuntimeError("ConnectionError: Max retries exceeded")
        return {"algoId": 5}

    monkeypatch.setattr(A, "_signed_post", post)
    monkeypatch.setattr(A, "_signed_get", lambda path, p=None: {"orders": []})
    r = A.place_sl("BTCUSDT", "SELL", "LONG", 100.0, 1.0)
    assert r["algoId"] == 5
    assert len(posts) == 2 and posts[0] == posts[1]   # 重发使用同一个 clientAlgoId


def test_place_sl_dup_2021_recovers_by_client_id(monkeypatch):
    state = {}

    def post(path, p):
        state["cid"] = p["clientAlgoId"]
        raise A.BinanceAPIError("dup", code=-2021) if _has_code_kw() else RuntimeError("code=-2021")

    monkeypatch.setattr(A, "_signed_post", post)
    monkeypatch.setattr(A, "_signed_get", lambda path, p=None: {"orders": [
        {"clientAlgoId": state["cid"], "algoId": 42, "symbol": "BTCUSDT"}]})
    r = A.place_sl("BTCUSDT", "SELL", "LONG", 100.0, 1.0)
    assert r["algoId"] == 42


def _has_code_kw():
    import inspect
    return "code" in inspect.signature(A.BinanceAPIError.__init__).parameters


# ── place_batch_orders ─────────────────────────────────────────────────────
def _specs():
    return [
        {"symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT", "quantity": "1", "price": "100"},
        {"symbol": "ETHUSDT", "side": "BUY", "type": "LIMIT", "quantity": "2", "price": "50"},
    ]


def test_batch_orders_get_client_ids_and_input_not_mutated(monkeypatch):
    sent = {}

    def post(path, p):
        sent["batch"] = json.loads(p["batchOrders"])
        return [{"orderId": 1}, {"orderId": 2}]

    monkeypatch.setattr(A, "_signed_post", post)
    specs = _specs()
    out = A.place_batch_orders(specs)
    assert [o["orderId"] for o in out] == [1, 2]
    cids = [o["newClientOrderId"] for o in sent["batch"]]
    assert len(set(cids)) == 2 and all(c.startswith("fx") for c in cids)
    assert all("newClientOrderId" not in s for s in specs)   # 不改调用方入参


def test_batch_orders_keeps_caller_client_id(monkeypatch):
    sent = {}
    monkeypatch.setattr(A, "_signed_post", lambda path, p: sent.setdefault("b", json.loads(p["batchOrders"])) and [{"orderId": 1}])
    s = _specs()[:1]
    s[0]["newClientOrderId"] = "mine1"
    A.place_batch_orders(s)
    assert sent["b"][0]["newClientOrderId"] == "mine1"


def test_batch_write_timeout_reconciles_by_client_id(monkeypatch):
    sent = {}

    def post(path, p):
        sent["batch"] = json.loads(p["batchOrders"])
        raise A.WriteTimeoutError("timeout")

    def get(path, p=None):
        assert path == "/fapi/v1/order"
        cid = p["origClientOrderId"]
        if cid == sent["batch"][0]["newClientOrderId"]:
            return {"orderId": 901, "status": "FILLED"}   # 已成交单：挂单列表里查不到，但按 id 能查到
        raise RuntimeError("-2013 Order does not exist")

    monkeypatch.setattr(A, "_signed_post", post)
    monkeypatch.setattr(A, "_signed_get", get)
    monkeypatch.setattr(A, "get_open_orders", lambda sym=None: [])
    out = A.place_batch_orders(_specs())
    assert out[0]["orderId"] == 901
    assert out[1]["error_code"] == "WRITE_TIMEOUT_UNKNOWN"


def test_batch_write_timeout_falls_back_to_attribute_match(monkeypatch):
    def post(path, p):
        raise A.WriteTimeoutError("timeout")

    def get(path, p=None):
        raise RuntimeError("-2013")

    monkeypatch.setattr(A, "_signed_post", post)
    monkeypatch.setattr(A, "_signed_get", get)
    now = int(A.time.time() * 1000)
    monkeypatch.setattr(A, "get_open_orders", lambda sym=None: (
        [{"orderId": 5, "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT", "origQty": "1", "price": "100", "time": now}]
        if sym == "BTCUSDT" else []))
    out = A.place_batch_orders(_specs())
    assert out[0]["orderId"] == 5
    assert out[1]["error_code"] == "WRITE_TIMEOUT_UNKNOWN"
