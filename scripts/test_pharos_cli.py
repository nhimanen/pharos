"""Tests for the ./pharos Python client (embeddings, graph and admin commands). Run: python3 scripts/test_pharos_cli.py"""
import importlib.machinery
import importlib.util
import pathlib
import subprocess
import sys
import tempfile
import io
import json
import unittest
import urllib.error
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parent.parent


def load():
    loader = importlib.machinery.SourceFileLoader("pharos_cli", str(ROOT / "pharos"))
    spec = importlib.util.spec_from_loader("pharos_cli", loader)
    mod = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


class EmbeddingsCli(unittest.TestCase):
    def setUp(self):
        self.p = load()
        # _jvm_flags() creates the dumps dir: keep it out of the real ~/.pharos
        self.p.PHAROS_DUMPS = pathlib.Path(tempfile.mkdtemp())

    def parse(self, *argv):
        with mock.patch.object(sys, "argv", ["pharos", *argv]), \
             mock.patch.object(self.p, "cmd_embeddings") as m:
            # main() dispatches via args.func, bound at parser build time; capture args instead
            captured = {}
            m.side_effect = lambda a: captured.setdefault("args", a)
            try:
                self.p.main()
            except SystemExit:
                pass
            return captured.get("args")

    def test_enable_defaults_to_granite(self):
        args = self.parse("embeddings", "enable")
        cmd = self.p._embeddings_command(args)
        self.assertEqual(cmd[-3:], ["embeddings", "enable", "--model=granite-r2"])

    def test_enable_jina_force(self):
        args = self.parse("embeddings", "enable", "--model", "jina-v2-code", "--force")
        cmd = self.p._embeddings_command(args)
        self.assertEqual(cmd[-3:], ["enable", "--model=jina-v2-code", "--force"])
        self.assertIn("--model=jina-v2-code", cmd)
        self.assertEqual(cmd[-1], "--force")

    def test_disable_and_status_take_no_model(self):
        for action in ("disable", "status"):
            cmd = self.p._embeddings_command(self.parse("embeddings", action))
            self.assertEqual(cmd[-2:], ["embeddings", action])

    def test_cmd_embeddings_runs_the_jar_without_the_daemon(self):
        args = self.parse("embeddings", "status")
        with mock.patch.object(self.p, "JAR", mock.Mock(exists=lambda: True, __str__=lambda s: "x.jar")), \
             mock.patch.object(subprocess, "run", return_value=mock.Mock(returncode=0)) as run, \
             self.assertRaises(SystemExit) as ex:
            self.p.cmd_embeddings(args)
        self.assertEqual(ex.exception.code, 0)
        self.assertEqual(run.call_args[0][0][-2:], ["embeddings", "status"])

    def test_jvm_flags_opt_out_of_djl_tracking(self):
        # DJL's telemetry probes the EC2 metadata endpoint on every model load otherwise
        self.assertIn("-DOPT_OUT_TRACKING=true", self.p._jvm_flags())

    def test_jina_preset_is_the_readme_id(self):
        args = self.parse("embeddings", "enable", "--model", "jina-v2-code")
        self.assertEqual(self.p._embeddings_command(args)[-1], "--model=jina-v2-code")

    def test_bad_model_rejected_by_parser(self):
        with mock.patch.object(sys, "argv", ["pharos", "embeddings", "enable", "--model", "nope"]), \
             self.assertRaises(SystemExit) as ex:
            self.p.main()
        self.assertNotEqual(ex.exception.code, 0)



class JarResolution(unittest.TestCase):
    def test_newest_version_wins_numerically(self):
        # 1.10.0 is the first two-digit minor: a string sort would pick 1.9.0
        p = load()
        names = ["pharos-1.9.0.jar", "pharos-1.10.0.jar", "pharos-1.4.1.jar", "pharos-1.10.0-SNAPSHOT.jar"]
        self.assertEqual("pharos-1.10.0.jar", pathlib.Path(p._newest_jar(["/t/" + n for n in names])).name)
        self.assertIsNone(p._newest_jar([]))


class FakeResponse(io.BytesIO):
    """What urllib.request.urlopen returns, as a context manager."""
    def __init__(self, payload):
        super().__init__(json.dumps(payload).encode())
        self.headers = {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class GraphAndAdminCommands(unittest.TestCase):
    """trace and usages read the daemon's API; link, stats and remove-index run inside the daemon."""

    def setUp(self):
        self.p = load()
        self.p.PHAROS_DUMPS = pathlib.Path(tempfile.mkdtemp())

    def main(self, *argv):
        """Runs `pharos <argv>` with the daemon mocked; returns (exit code, the urlopen calls)."""
        calls = []

        def urlopen(req, timeout=None):
            calls.append(req)
            url = req if isinstance(req, str) else req.full_url
            if "/api/admin/" in url:
                return FakeResponse({"exitCode": 3, "out": "done\n", "err": "warned\n"})
            if "/api/trace" in url:
                return FakeResponse({"root": "a.B#c()", "direction": "callees", "maxDepth": 2, "totalNodes": 0,
                                     "truncated": False, "nodes": []})
            return FakeResponse({"fqn": "a.B", "callers": ["x.Y#z()"]})

        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(sys, "argv", ["pharos", *argv]), \
             mock.patch.object(self.p, "ensure_daemon"), \
             mock.patch.object(self.p.urllib.request, "urlopen", side_effect=urlopen), \
             mock.patch.object(sys, "stdout", out), mock.patch.object(sys, "stderr", err):
            code = 0
            try:
                self.p.main()
            except SystemExit as e:
                code = e.code
        self.out, self.err = out.getvalue(), err.getvalue()
        return code, calls

    def test_remove_index_runs_in_the_daemon_with_the_cli_arguments(self):
        code, calls = self.main("remove-index", "myproject")
        self.assertEqual(1, len(calls))
        req = calls[0]
        self.assertTrue(req.full_url.endswith("/api/admin/remove-index"))
        self.assertEqual("POST", req.get_method())
        self.assertEqual("1", req.get_header("X-pharos-admin"))   # urllib capitalizes header names
        self.assertEqual({"args": ["myproject"]}, json.loads(req.data))
        self.assertEqual(3, code)                    # the daemon's exit code, passed through
        self.assertEqual("done\n", self.out)
        self.assertEqual("warned\n", self.err)

    def test_remove_index_flags(self):
        _, calls = self.main("remove-index", "--all")
        self.assertEqual({"args": ["--all"]}, json.loads(calls[0].data))
        _, calls = self.main("remove-index", "lucene", "--from-catalog")
        self.assertEqual({"args": ["lucene", "--from-catalog"]}, json.loads(calls[0].data))
        _, calls = self.main("remove-index", ".")    # an invalid registration: the daemon only unregisters it
        self.assertEqual({"args": ["."]}, json.loads(calls[0].data))

    def test_link_and_stats(self):
        _, calls = self.main("link", "a", "b")
        self.assertTrue(calls[0].full_url.endswith("/api/admin/link"))
        self.assertEqual({"args": ["a", "b"]}, json.loads(calls[0].data))
        _, calls = self.main("stats")
        self.assertEqual({"args": []}, json.loads(calls[0].data))
        _, calls = self.main("stats", "--project", "a")
        self.assertTrue(calls[0].full_url.endswith("/api/admin/stats"))
        self.assertEqual({"args": ["--project", "a"]}, json.loads(calls[0].data))

    def test_admin_http_error_exits_1_with_the_message(self):
        def refuse(req, timeout=None):
            raise urllib.error.HTTPError(req.full_url, 403, "Forbidden", {},
                                         io.BytesIO(b'{"error": "admin commands are accepted from this host only"}'))
        err = io.StringIO()
        with mock.patch.object(sys, "argv", ["pharos", "stats"]), \
             mock.patch.object(self.p, "ensure_daemon"), \
             mock.patch.object(self.p.urllib.request, "urlopen", side_effect=refuse), \
             mock.patch.object(sys, "stderr", err), self.assertRaises(SystemExit) as ex:
            self.p.main()
        self.assertEqual(1, ex.exception.code)
        self.assertIn("from this host only", err.getvalue())

    def test_trace_and_usages_query_the_api(self):
        _, calls = self.main("trace", "a.B#c()", "--direction", "both", "--depth", "9", "--max-body", "0")
        url = calls[0]
        self.assertIn("/api/trace?", url)
        self.assertIn("direction=both", url)
        self.assertIn("depth=4", url)                # clamped like the Java CLI
        self.assertIn("maxBodyChars=0", url)
        self.assertIn("No nodes found for: a.B#c()", self.out)
        _, calls = self.main("usages", "a.B", "--kind", "subclasses")
        self.assertIn("/api/usages?", calls[0])
        self.assertIn("kind=subclasses", calls[0])
        self.assertIn("Callers (1):\n  x.Y#z()", self.out)

    def test_formatters_match_the_java_cli(self):
        r = {"root": "a.B#c()", "direction": "callees", "maxDepth": 1, "totalNodes": 2, "truncated": True,
             "nodes": [{"fqn": "a.B#c()", "depth": 0, "filePath": "B.java", "startLine": 3, "endLine": 5,
                        "body": "void c() {\n  d();\n}"},
                       {"fqn": "a.B#d()", "depth": 1}]}
        text = self.p.fmt_trace(r)
        self.assertIn("Direction: callees | Depth: 1 | Nodes: 2 (truncated at 50)", text)
        self.assertIn("[0] a.B#c()\n    B.java:3-5\n    void c() {", text)
        self.assertIn("  \u21b3 [1] a.B#d()", text)
        u = self.p.fmt_usages({"fqn": "a.B", "subclasses": ["a.C"], "fieldReaders": [], "methodsTaking": ["a.D#e(B)"]})
        self.assertEqual("Usages of: a.B\n\nSubclasses (1):\n  a.C\n\nMethods taking (1):\n  a.D#e(B)", u)

    def test_usages_rejects_an_unknown_kind(self):
        with mock.patch.object(sys, "argv", ["pharos", "usages", "a.B", "--kind", "nope"]), \
             mock.patch.object(sys, "stderr", io.StringIO()), self.assertRaises(SystemExit) as ex:
            self.p.main()
        self.assertNotEqual(0, ex.exception.code)


if __name__ == "__main__":
    unittest.main()
