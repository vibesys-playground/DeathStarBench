"""Unit tests of run.py's pure parts:  python3 -m unittest discover -s tests"""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

TASK_DIR = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("hotel_run", TASK_DIR / "run.py")
run = importlib.util.module_from_spec(spec)
sys.modules["hotel_run"] = run
spec.loader.exec_module(run)


def container(job: dict) -> dict:
    (only,) = job["spec"]["template"]["spec"]["containers"]
    return only


def flag(argv: list[str], name: str) -> str:
    return argv[argv.index(name) + 1]


REFERENCE = run.envelope.load(Path("/nonexistent"))  # the reference application tier


class SeedTest(unittest.TestCase):
    def test_seeds_are_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            run.write_seeds(Path(directory), {"oracle": {"seed": 5}})
            self.assertEqual(json.loads((Path(directory) / "seeds.json").read_text()), {"oracle": {"seed": 5}})


class ManifestTest(unittest.TestCase):
    def test_seed_job_runs_the_benchmarks_seed(self):
        job = run.seed_job("registry/task:x", "ab" * 32)
        self.assertEqual(container(job)["command"], ["python", "/task/bench.py", "seed", "--dataset-digest", "ab" * 32])
        resources = container(job)["resources"]
        self.assertEqual(resources["requests"], resources["limits"])
        self.assertFalse(job["spec"]["template"]["spec"]["automountServiceAccountToken"])

    def test_oracle_job(self):
        job = run.oracle_job(REFERENCE, "registry/task:x", 987654321)
        self.assertEqual(job["kind"], "Job")
        self.assertEqual(job["metadata"]["name"], "oracle")
        pod = job["spec"]["template"]["spec"]
        self.assertEqual(pod["serviceAccountName"], run.EVALUATOR_ACCOUNT)
        self.assertEqual(pod["restartPolicy"], "Never")
        self.assertEqual(job["spec"]["backoffLimit"], 0)
        command = container(job)["command"]
        self.assertEqual(command[:2], ["sh", "-c"])
        self.assertIn("python /task/dataset.py /tmp/dataset.json", command[2])
        argv = command[4:]
        self.assertEqual(argv[0], "hotel-correctness")
        self.assertEqual(flag(argv, "--dataset"), "/tmp/dataset.json")
        self.assertEqual(flag(argv, "--workload"), "/task/workload/oracle.toml")
        self.assertEqual(flag(argv, "--mode"), "accuracy")
        self.assertEqual(flag(argv, "--seed"), "987654321")
        self.assertEqual(flag(argv, "--base-url"), "http://frontend:5000")
        self.assertNotIn("--run-command-json", argv)
        hook = json.loads(flag(argv, "--restart-command-json"))
        self.assertEqual(hook[0], "podctl")
        self.assertEqual(hook[-1], "restart")  # the oracle appends one or all
        self.assertEqual(flag(hook, "--deployments").split(","), REFERENCE.names + ["memcached-reserve"])
        self.assertIn("--registered-auto", hook)
        self.assertNotIn("--registered", hook)
        self.assertEqual(flag(hook, "--probed"), "frontend")
        self.assertEqual([hook[i + 1] for i, a in enumerate(hook) if a == "--probe-path"], run.PROBE_PATHS)
        # The manifest is what kubectl apply receives.
        self.assertEqual(json.loads(json.dumps(job)), job)

    def test_ready_job_and_account(self):
        argv = container(run.ready_job(REFERENCE, "i"))["command"]
        self.assertEqual(argv[-1], "ready")
        self.assertEqual(flag(argv, "--deployments").split(","), REFERENCE.names)
        rbac = run.evaluator_rbac("hk8s-0001")
        kinds = {item["kind"]: item for item in rbac["items"]}
        self.assertEqual(set(kinds), {"ServiceAccount", "Role", "RoleBinding"})
        rules = {tuple(rule["resources"]): set(rule["verbs"]) for rule in kinds["Role"]["rules"]}
        self.assertEqual(rules, {("pods",): {"get", "list", "watch", "delete"}, ("deployments",): {"get", "list"}})
        self.assertEqual(kinds["RoleBinding"]["subjects"][0]["namespace"], "hk8s-0001")

    def test_candidate_deployments_drive_podctl(self):
        """Any grouping: every Deployment is waited for and restarted, the frontend's probed."""
        text = (TASK_DIR / "tests" / "testdata" / "regrouped.yaml").read_text()
        app = run.envelope.validate(text, source="regrouped.yaml", task_text=run.MANIFEST.read_text())
        ready = container(run.ready_job(app, "i"))["command"]
        self.assertEqual(flag(ready, "--deployments").split(","), app.names)
        self.assertEqual(flag(ready, "--probed").split(","), list(app.frontend))
        hook = run.podctl_command(app, "restart")
        self.assertEqual(flag(hook, "--deployments").split(","), app.names + ["memcached-reserve"])

if __name__ == "__main__":
    unittest.main()
