"""运行：cd fx && python3 -m unittest tests.test_ensure_deps -v
[v3.89 #24/#24b] _ensure_deps 只检查依赖：缺 PySocks 且配置了 SOCKS 代理 → critical + 退出；未配置 → 仅 warning；不运行 pip。
main.py 导入有副作用，按源码抽取函数。"""
import ast, logging, os, subprocess, sys, types, unittest
from pathlib import Path
from unittest import mock

SRC = (Path(__file__).parent.parent / "main.py").read_text(encoding="utf-8")
_NAMES = ("_socks_proxy_configured", "_ensure_deps")
_nodes = [n for n in ast.parse(SRC).body if isinstance(n, ast.FunctionDef) and n.name in _NAMES]
_PROXY_ENVS = ("SOCKS_PROXY", "BINANCE_PROXY", "PROXY_URL", "ALL_PROXY", "HTTPS_PROXY", "HTTP_PROXY")


def _load():
    ns = {"logging": logging, "sys": sys, "subprocess": subprocess, "Path": Path, "__file__": "/nonexistent/main.py"}
    for n in _nodes:
        exec(ast.get_source_segment(SRC, n), ns)
    return ns


def _clean_env(**extra):
    env = {k: v for k, v in os.environ.items() if k not in _PROXY_ENVS}
    env.update(extra)
    return mock.patch.dict(os.environ, env, clear=True)


def _no_config():
    return mock.patch.dict(sys.modules, {"config": None})


class TestEnsureDeps(unittest.TestCase):
    def test_missing_with_socks_configured_exits_without_pip(self):
        f = _load()["_ensure_deps"]
        with mock.patch.dict(sys.modules, {"socks": None}), _clean_env(SOCKS_PROXY="127.0.0.1:1080"), _no_config(), \
             mock.patch("subprocess.run") as run, \
             self.assertLogs("fx.startup", level="CRITICAL") as cm, self.assertRaises(SystemExit) as ex:
            f()
        self.assertEqual(ex.exception.code, 1)
        run.assert_not_called()
        self.assertTrue(any("PySocks" in m for m in cm.output))

    def test_missing_with_socks_scheme_url_exits(self):
        f = _load()["_ensure_deps"]
        with mock.patch.dict(sys.modules, {"socks": None}), _clean_env(BINANCE_PROXY="socks5h://u:p@h:1080"), _no_config(), \
             self.assertLogs("fx.startup", level="CRITICAL"), self.assertRaises(SystemExit):
            f()

    def test_missing_without_socks_only_warns_and_continues(self):
        f = _load()["_ensure_deps"]
        with mock.patch.dict(sys.modules, {"socks": None}), _clean_env(HTTPS_PROXY="http://h:8080"), _no_config(), \
             mock.patch("subprocess.run") as run, \
             self.assertLogs("fx.startup", level="WARNING") as cm:
            f()                                              # 不退出
        run.assert_not_called()
        self.assertTrue(all(m.startswith("WARNING") for m in cm.output))

    def test_missing_with_no_proxy_at_all_only_warns(self):
        f = _load()["_ensure_deps"]
        with mock.patch.dict(sys.modules, {"socks": None}), _clean_env(), _no_config(), \
             self.assertLogs("fx.startup", level="WARNING"):
            f()

    def test_config_socks_proxy_detected(self):
        ns = _load()
        cfg = types.ModuleType("config"); cfg.PROXY_URL = "socks5://h:1"
        with _clean_env(), mock.patch.dict(sys.modules, {"config": cfg}):
            self.assertTrue(ns["_socks_proxy_configured"]())
        cfg2 = types.ModuleType("config"); cfg2.PROXY_URL = "http://h:1"
        with _clean_env(), mock.patch.dict(sys.modules, {"config": cfg2}):
            self.assertFalse(ns["_socks_proxy_configured"]())

    def test_present_dependency_passes_silently(self):
        f = _load()["_ensure_deps"]
        with mock.patch.dict(sys.modules, {"socks": types.ModuleType("socks")}), mock.patch("subprocess.run") as run:
            f()
        run.assert_not_called()

    def test_source_has_no_pip_install(self):
        for n in _nodes:
            self.assertNotIn('"pip"', ast.get_source_segment(SRC, n))


if __name__ == "__main__":
    unittest.main()
