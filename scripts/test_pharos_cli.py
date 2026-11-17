"""Tests for the ./pharos Python client's `embeddings` command. Run: python3 scripts/test_pharos_cli.py"""
import importlib.machinery
import importlib.util
import pathlib
import subprocess
import sys
import unittest
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
        args = self.parse("embeddings", "enable", "--model", "jina-code-v2", "--force")
        cmd = self.p._embeddings_command(args)
        self.assertEqual(cmd[-3:], ["enable", "--model=jina-code-v2", "--force"])
        self.assertIn("--model=jina-code-v2", cmd)
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

    def test_bad_model_rejected_by_parser(self):
        with mock.patch.object(sys, "argv", ["pharos", "embeddings", "enable", "--model", "nope"]), \
             self.assertRaises(SystemExit) as ex:
            self.p.main()
        self.assertNotEqual(ex.exception.code, 0)


if __name__ == "__main__":
    unittest.main()
