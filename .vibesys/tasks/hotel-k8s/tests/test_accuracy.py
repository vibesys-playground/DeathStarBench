"""Tests of bench.py's accuracy check:  python3 -m unittest discover -s tests"""

import io
import re
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest import mock

TASK_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(TASK_DIR))

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
        check = bench.check(REFERENCE)
        self.assertEqual((check.phases, check.rate, check.secs), (4, 200, 30))
        memcached = ("memcached-profile", "memcached-rate", "memcached-reserve")
        by_phase = {}
        for fault in check.faults:
            by_phase.setdefault(fault.after, []).append((fault.action, fault.deployment))
        names = REFERENCE.names
        self.assertEqual(by_phase[0], [("restart", n) for n in names])
        self.assertEqual(by_phase[1], [("flush", c) for c in memcached])
        self.assertEqual(by_phase[2], [("restart_all", n) for n in names])
        # Never MongoDB: its pods restore the snapshot into an emptyDir.
        self.assertFalse([f for f in check.faults if f.deployment.startswith("mongodb")])
        system = bench.system(REFERENCE)
        self.assertEqual(set(system.restartable), set(names))
        self.assertEqual(dict(system.caches), {c: "memcached" for c in memcached})
        self.assertIs(bench.system(None), bench.SYSTEM)
        self.assertEqual(bench.check(None).faults, ())
        # The MongoDB pods copy the snapshot before they start: longer than the default wait.
        self.assertGreaterEqual(bench.SYSTEM.wait_secs, 1800)

    def test_a_candidate_outside_the_envelope_fails_and_a_missing_snapshot_is_unknown(self):
        bench._PREPARE_ERROR.append(envelope.EnvelopeError("app.yaml", ["17 CPUs over the 16"]))
        err = io.StringIO()
        with mock.patch.object(bench, "prepare", return_value=None), redirect_stderr(err), \
                self.assertRaises(SystemExit) as caught:
            bench.main(["accuracy", "--project-root", "/nonexistent"])
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("FAIL", err.getvalue())
        bench._PREPARE_ERROR[:] = [snapshot.SnapshotError("not ready after 2700s")]
        with mock.patch.object(bench, "prepare", return_value=None), redirect_stderr(err), \
                self.assertRaises(SystemExit) as caught:
            bench.main(["accuracy", "--project-root", "/nonexistent"])
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
        check = bench.check(REFERENCE)
        self.assertIsInstance(check, ms.Accuracy)
        # The check's own workload, constant: the phase pods build the task again.
        self.assertIs(check.workload, bench.ACCURACY_WORKLOAD)
        self.assertIs(bench.check(None).workload, bench.ACCURACY_WORKLOAD)
        self.assertEqual(bench.ACCURACY_WORKLOAD.command, ("hotel-accuracy-generator",))
        self.assertEqual(bench.WORKLOAD.command, ("hotel-generator",))
        self.assertEqual(check.oracle, ("hotel-oracle",))


class PinTest(unittest.TestCase):
    def test_the_image_and_the_go_module_pin_the_same_evaluator_release(self):
        dockerfile = (TASK_DIR / "Dockerfile").read_text()
        go_mod = (TASK_DIR / "evaluator" / "go.mod").read_text()
        base = re.findall(r"^FROM ghcr\.io/uw-syfi/vibesys-evaluator-microservices:(\S+)@sha256:[0-9a-f]{64}$",
                          dockerfile, re.M)
        go = re.findall(r"github\.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go v(\S+)", go_mod)
        self.assertEqual(len(base), 1, "the task image is FROM the evaluator's image, by tag and digest")
        self.assertEqual(len(go), 1)
        # PEP 440 0.3.0rc2 is the Go module's semver 0.3.0-rc.2.
        self.assertEqual(re.sub(r"rc(\d+)$", r"-rc.\1", base[0]), go[0])
        # The image's last stage is the evaluator's; the task's programs go on top of it.
        self.assertTrue(dockerfile[dockerfile.rindex("\nFROM "):].startswith(
            "\nFROM ghcr.io/uw-syfi/vibesys-evaluator-microservices:"))


if __name__ == "__main__":
    unittest.main()
