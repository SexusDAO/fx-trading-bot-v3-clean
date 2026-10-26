"""运行：cd fx && python3 -m unittest tests.test_ensure_deps -v
_ensure_deps：缺 PySocks 时自动 pip 安装（requests[socks] + PySocks），装完重建 binance_api 的 Session；
已安装则静默通过、不调用 pip。安装失败（check=True）时异常向上抛出。
main.py 导入有副作用，按源码抽取函数。
（决定：保留启动时自动安装行为；原先「只告警/退出、不运行 pip」的期望已过期，测试按现状对齐。）
"""
import ast, logging, subprocess, sys, types, unittest
from pathlib import Path
from unittest import mock

SRC = (Path(__file__).parent.parent / "main.py").read_text(encoding="utf-8")
_nodes = [n for n in ast.parse(SRC).body if isinstance(n, ast.FunctionDef) and n.name == "_ensure_deps"]


def _load():
    ns = {"logging": logging, "sys": sys, "subprocess": subprocess, "Path": Path, "__file__": "/nonexistent/main.py"}
    for n in _nodes:
        exec(ast.get_source_segment(SRC, n), ns)
    return ns["_ensure_deps"]


def _fake_binance_api():
    api = types.ModuleType("shared.binance_api")
    api.reset_session = mock.Mock()
    pkg = types.ModuleType("shared")
    pkg.binance_api = api
    return pkg, api


class TestEnsureDeps(unittest.TestCase):
    def test_present_dependency_passes_silently(self):
        f = _load()
        with mock.patch.dict(sys.modules, {"socks": types.ModuleType("socks")}), mock.patch("subprocess.run") as run:
            f()
        run.assert_not_called()

    def test_missing_pysocks_installs_via_pip_and_rebuilds_session(self):
        f = _load()
        pkg, api = _fake_binance_api()
        with mock.patch.dict(sys.modules, {"socks": None, "shared": pkg, "shared.binance_api": api}), \
             mock.patch("subprocess.run") as run:
            f()                                              # 不退出
        run.assert_called_once()
        cmd = run.call_args[0][0]
        self.assertEqual(cmd[1:4], ["-m", "pip", "install"])
        self.assertIn("PySocks", cmd)
        self.assertIn("requests[socks]", cmd)
        self.assertTrue(run.call_args[1].get("check"))
        api.reset_session.assert_called_once()

    def test_session_rebuild_failure_is_tolerated(self):
        f = _load()
        pkg, api = _fake_binance_api()
        api.reset_session.side_effect = RuntimeError("boom")
        with mock.patch.dict(sys.modules, {"socks": None, "shared": pkg, "shared.binance_api": api}), \
             mock.patch("subprocess.run"):
            f()                                              # 重建失败只打印，不抛

    def test_pip_failure_propagates(self):
        f = _load()
        with mock.patch.dict(sys.modules, {"socks": None}), \
             mock.patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "pip")):
            with self.assertRaises(subprocess.CalledProcessError):
                f()

    def test_source_keeps_auto_pip_install(self):
        """回归：确认自动安装行为被有意保留（若改为只告警，需同步修改本测试）。"""
        for n in _nodes:
            seg = ast.get_source_segment(SRC, n)
            self.assertIn('"pip"', seg)
            self.assertIn("PySocks", seg)


if __name__ == "__main__":
    unittest.main()
