"""WS 代理降级：握手前失败 → 直连；连上之后的断线不算代理故障；过期后再试代理；日志脱敏。全部 mock。"""
from shared.ws_proxy import ProxyFallback, mask_proxy

P = "socks5://user:secret@1.2.3.4:6688"


def mk(t, retry=300.0):
    return ProxyFallback(P, name="t", retry_sec=retry, clock=lambda: t["v"])


def test_no_proxy_always_direct():
    fb = ProxyFallback(None)
    assert fb.current() is None and fb.failed(Exception("x")) is False


def test_handshake_failure_degrades_to_direct():
    t = {"v": 0.0}
    fb = mk(t)
    assert fb.current() == P
    assert fb.failed(Exception("proxy down")) is True
    assert fb.degraded and fb.current() is None          # 下一次重连直连


def test_failure_after_opened_does_not_degrade():
    t = {"v": 0.0}
    fb = mk(t)
    fb.current()
    fb.opened()
    assert fb.failed(Exception("ws closed")) is False
    assert fb.current() == P                              # 仍用代理


def test_retry_proxy_after_window():
    t = {"v": 0.0}
    fb = mk(t, retry=300)
    fb.current(); fb.failed(Exception("x"))
    t["v"] = 299
    assert fb.current() is None
    t["v"] = 301
    assert fb.current() == P and not fb.degraded          # 到期再试代理
    assert fb.failed(Exception("still down")) is True     # 再失败 → 再降级
    assert fb.current() is None


def test_failed_while_direct_is_noop():
    t = {"v": 0.0}
    fb = mk(t)
    fb.current(); fb.failed(Exception("x"))
    fb.current()                                          # 直连
    assert fb.failed(Exception("direct also fails")) is False


def test_mask_proxy_hides_credentials():
    assert mask_proxy(P) == "socks5://1.2.3.4:6688"
    assert "secret" not in mask_proxy(P)


def test_failed_log_does_not_leak_password(caplog):
    import logging
    t = {"v": 0.0}
    fb = mk(t)
    fb.current()
    with caplog.at_level(logging.WARNING, logger="ws_proxy"):
        fb.failed(Exception("boom"))
    assert "secret" not in caplog.text and "1.2.3.4:6688" in caplog.text


def test_config_default_has_no_hardcoded_credentials():
    import re
    src = open(__import__("os").path.join(__import__("os").path.dirname(__file__), "..", "config.py"), encoding="utf-8").read()
    hits = [m.group(0) for m in re.finditer(r"socks5://([^\"'\s:@]+):([^\"'\s@]+)@", src)
            if (m.group(1), m.group(2)) != ("user", "pass")]      # 注释里的 user:pass 是占位符
    assert hits == []
