"""ai_panel 单测：storage 读写与增量、runner 重试（mock HTTP）、analyzer 有效回答判定。"""
import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.ai_panel import analyzer, config, runner, storage  # noqa: E402


def _ok(content="x" * 50, ptok=10, ctok=300, finish="stop"):
    return 200, {"choices": [{"message": {"content": content}, "finish_reason": finish}],
                 "usage": {"prompt_tokens": ptok, "completion_tokens": ctok}}


class TestStorage(unittest.TestCase):
    def test_save_load_and_merge(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            storage.save(d, "Q1", "题", {"m1": {"answer": "a", "error": None}})
            storage.save(d, "Q1", "题", {"m2": {"answer": "b", "error": None}})
            data = storage.load(d, "Q1")
            self.assertEqual(set(data["responses"]), {"m1", "m2"})
            self.assertEqual(data["question_text"], "题")

    def test_incremental(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            storage.save(d, "Q1", "题", {"m1": {"answer": "a", "error": None},
                                        "m2": {"answer": "", "error": "HTTP 429"}})
            ex = storage.load(d, "Q1")
            self.assertEqual(storage.models_to_run(ex, ["m1", "m2", "m3"]), ["m2", "m3"])
            self.assertEqual(storage.models_to_run(ex, ["m1", "m2"], force=True), ["m1", "m2"])

    def test_load_all_ignores_non_json(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            storage.save(d, "Q1", "t", {"m": {"answer": "a"}})
            (d / "note.txt").write_text("x")
            self.assertEqual(list(storage.load_all(d)), ["Q1"])


class TestRunner(unittest.TestCase):
    def setUp(self):
        os.environ["PANEL_API_KEY"] = "test-key"

    def test_token_param(self):
        self.assertEqual(runner.token_param_for({"name": "gpt-6.1-sol"}), "max_completion_tokens")
        self.assertEqual(runner.token_param_for({"name": "o4-mini"}), "max_completion_tokens")
        self.assertEqual(runner.token_param_for({"name": "claude-opus-5"}), "max_tokens")
        self.assertEqual(runner.token_param_for({"name": "x", "token_param": "max_completion_tokens"}),
                         "max_completion_tokens")

    def test_retry_then_success(self):
        calls = []

        async def post(url, headers, payload, timeout):
            calls.append(1)
            return (500, {"error": "boom"}) if len(calls) < 3 else _ok()
        r = asyncio.run(runner.call_model({"name": "m"}, "s", "q", post=post, retries=2, delay=0))
        self.assertEqual(len(calls), 3)
        self.assertEqual(r["finish_reason"], "stop")
        self.assertIsNone(r["error"])

    def test_retry_exhausted(self):
        calls = []

        async def post(url, headers, payload, timeout):
            calls.append(1)
            raise TimeoutError("t")
        r = asyncio.run(runner.call_model({"name": "m"}, "s", "q", post=post, retries=2, delay=0))
        self.assertEqual(len(calls), 3)
        self.assertEqual(r["answer"], "")
        self.assertIn("TimeoutError", r["error"])

    def test_no_retry_on_402(self):
        calls = []

        async def post(url, headers, payload, timeout):
            calls.append(1)
            return 402, {"error": "quota"}
        r = asyncio.run(runner.call_model({"name": "m"}, "s", "q", post=post, retries=2, delay=0))
        self.assertEqual(len(calls), 1)
        self.assertIn("402", r["error"])

    def test_payload_has_no_history(self):
        p = runner.build_payload({"name": "gpt-6.1-sol", "max_tokens": 99}, "SYS", "Q")
        self.assertEqual([m["role"] for m in p["messages"]], ["system", "user"])
        self.assertEqual(p["max_completion_tokens"], 99)
        self.assertNotIn("max_tokens", p)

    def test_missing_key(self):
        os.environ.pop("PANEL_API_KEY", None)
        with self.assertRaises(RuntimeError):
            config.get_api_key()


class TestAnalyzer(unittest.TestCase):
    def test_is_valid(self):
        good = {"answer": "x", "output_tokens": 500, "finish_reason": "stop", "error": None}
        self.assertTrue(analyzer.is_valid(good))
        self.assertFalse(analyzer.is_valid({**good, "output_tokens": 200}))     # 不大于 200
        self.assertFalse(analyzer.is_valid({**good, "finish_reason": "length"}))
        self.assertFalse(analyzer.is_valid({**good, "answer": ""}))
        self.assertFalse(analyzer.is_valid({**good, "error": "HTTP 500"}))

    def test_quality_label(self):
        self.assertEqual(analyzer.quality_label({"answer": "x", "output_tokens": 500, "finish_reason": "stop"}), "有效")
        self.assertEqual(analyzer.quality_label({"answer": "x", "output_tokens": 100, "finish_reason": "length"}), "截断")
        self.assertEqual(analyzer.quality_label({"answer": "", "error": "e"}), "失败")

    def test_build_prompt_only_valid(self):
        resp = {"a": {"answer": "好回答", "output_tokens": 500, "finish_reason": "stop"},
                "b": {"answer": "半截", "output_tokens": 100, "finish_reason": "length"}}
        p = analyzer.build_prompt("题目", resp)
        self.assertIn("好回答", p)
        self.assertNotIn("半截", p)

    def test_insufficient_data_skips_api(self):
        os.environ["PANEL_API_KEY"] = "k"

        async def post(*a, **k):
            raise AssertionError("不应调用 API")
        data = {"question_id": "Q", "question_text": "t",
                "responses": {"a": {"answer": "x", "output_tokens": 500, "finish_reason": "stop"}}}
        res = asyncio.run(analyzer.analyze_question(data, post=post))
        self.assertIsNone(res["summary"])


if __name__ == "__main__":
    unittest.main()


class TestTransportsAgainstLocalServer(unittest.TestCase):
    """用本地假服务器真实走一遍网络层（httpx 与标准库两条路径），不依赖外网。"""
    @classmethod
    def setUpClass(cls):
        import http.server, json, threading

        class H(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                n = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(n))
                if body.get("model") == "slow":
                    import time; time.sleep(3)
                code = 402 if body.get("model") == "noquota" else 200
                out = {"error": "quota"} if code == 402 else {
                    "choices": [{"message": {"content": "ok" * 150}, "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 5, "completion_tokens": 300}}
                data = json.dumps(out).encode()
                self.send_response(code); self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)

            def log_message(self, *a):
                pass
        cls.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()
        cls.url = f"http://127.0.0.1:{cls.srv.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def setUp(self):
        os.environ["PANEL_API_KEY"] = "k"
        self._old = config.PANEL_BASE_URL
        config.PANEL_BASE_URL = self.url

    def tearDown(self):
        config.PANEL_BASE_URL = self._old

    def _run(self, name, **kw):
        return asyncio.run(runner.call_model({"name": name}, "s", "q", retries=0, delay=0, **kw))

    def test_default_transport_success_and_402(self):
        r = self._run("good")
        self.assertEqual(r["finish_reason"], "stop")
        self.assertEqual(r["output_tokens"], 300)
        self.assertIn("402", self._run("noquota")["error"])

    def test_timeout_total(self):
        r = self._run("slow", timeout=1)
        self.assertEqual(r["answer"], "")
        self.assertIsNotNone(r["error"])

    def test_concurrency_limit(self):
        import threading
        active = {"n": 0, "max": 0}
        lock = threading.Lock()

        async def post(url, headers, payload, timeout):
            with lock:
                active["n"] += 1; active["max"] = max(active["max"], active["n"])
            await asyncio.sleep(0.05)
            with lock:
                active["n"] -= 1
            return _ok()
        old = config.PANEL_MAX_CONCURRENCY
        config.PANEL_MAX_CONCURRENCY = 2
        try:
            models = [{"name": f"m{i}"} for i in range(6)]
            res = asyncio.run(runner.run_question("q", models, "s", post=post))
        finally:
            config.PANEL_MAX_CONCURRENCY = old
        self.assertEqual(len(res), 6)
        self.assertLessEqual(active["max"], 2)


class TestContentParsing(unittest.TestCase):
    def test_list_content_blocks(self):
        d = {"choices": [{"message": {"content": [{"type": "text", "text": "甲"}, {"type": "text", "text": "乙"}]},
                          "finish_reason": "stop"}], "usage": {}}
        r = runner._parse_success(d, 1.0)
        self.assertEqual(r["answer"], "甲乙")
        self.assertEqual(r["message_keys"], ["content"])
