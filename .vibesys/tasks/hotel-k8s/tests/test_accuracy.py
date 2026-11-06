"""Tests of accuracy.py, the accuracy check's script:  python3 -m unittest discover -s tests"""

import io
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest import mock

TASK_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(TASK_DIR))

import accuracy  # noqa: E402
import bench  # noqa: E402
import envelope  # noqa: E402
import snapshot  # noqa: E402
from vibesys_evaluator import microservices as ms  # noqa: E402

REFERENCE = envelope.validate(envelope.REFERENCE_APP.read_text(), source="app.yaml",
                              task_text=envelope.TASK_MANIFEST.read_text())


class AccuracyTest(unittest.TestCase):
    def tearDown(self):
        bench._PREPARE_ERROR.clear()

    def test_the_faults_hit_every_candidate_deployment_and_cache(self):
        check = accuracy.check(REFERENCE)
        self.assertEqual((check.phases, check.rate, check.secs), (4, 200, 30))
        by_phase = {}
        for fault in check.faults:
            by_phase.setdefault(fault.after, []).append((fault.action, fault.deployment))
        names = REFERENCE.names
        self.assertEqual(by_phase[0], [("restart", n) for n in names])
        self.assertEqual(by_phase[1], [("exec", c) for c in accuracy.MEMCACHED])
        self.assertEqual(by_phase[2], [("restart_all", n) for n in names])
        # Never MongoDB: its pods restore the snapshot into an emptyDir.
        self.assertFalse([f for f in check.faults if f.deployment.startswith("mongodb")])
        system = accuracy.system(REFERENCE)
        self.assertEqual(set(system.restartable), set(names))
        self.assertEqual(dict(system.caches), {c: "memcached" for c in accuracy.MEMCACHED})
        self.assertIs(accuracy.system(None), bench.SYSTEM)
        self.assertEqual(accuracy.check(None).faults, ())

    def test_a_candidate_outside_the_envelope_fails_and_a_missing_snapshot_is_unknown(self):
        bench._PREPARE_ERROR.append(envelope.EnvelopeError("app.yaml", ["17 CPUs over the 16"]))
        err = io.StringIO()
        with mock.patch.object(bench, "prepare", return_value=None), redirect_stderr(err), \
                self.assertRaises(SystemExit) as caught:
            accuracy.main(["accuracy", "--project-root", "/nonexistent"])
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("FAIL", err.getvalue())
        bench._PREPARE_ERROR[:] = [snapshot.SnapshotError("not ready after 2700s")]
        with mock.patch.object(bench, "prepare", return_value=None), redirect_stderr(err), \
                self.assertRaises(SystemExit) as caught:
            accuracy.main(["accuracy", "--project-root", "/nonexistent"])
        self.assertEqual(caught.exception.code, 2)

    def test_the_accuracy_command_prepares_the_deployed_manifest(self):
        class Ensure:
            def __call__(self, spec_json, **kwargs):
                return snapshot.Ensured(version="0123456789abcdef0123", seconds=1.0, summary={})

        env = {"VIBESYS_K8S_REGISTRY": "registry:5000", "VIBESYS_K8S_BUILDER": "tcp://builder:1234"}
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "deployed.yaml"
            app = bench.prepare(["accuracy", "--project-root", str(Path(directory) / "repo")], target,
                                ensure=Ensure(), environ=env)
            self.assertEqual(app.names, REFERENCE.names)
            self.assertIn("/snapshots/0123456789abcdef0123/", target.read_text())

    def test_the_check_is_a_valid_accuracy_of_the_evaluator(self):
        self.assertIsInstance(accuracy.check(REFERENCE), ms.Accuracy)
        self.assertEqual(accuracy.WORKLOAD.command, ("hotel-accuracy-generator",))


if __name__ == "__main__":
    unittest.main()
