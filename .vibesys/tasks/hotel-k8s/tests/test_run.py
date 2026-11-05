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
        job = run.oracle_job("registry/task:x", 987654321)
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
        self.assertEqual(flag(hook, "--deployments").split(","), run.APP_DEPLOYMENTS + ["memcached-reserve"])
        self.assertNotIn("frontend", flag(hook, "--registered").split(","))
        self.assertEqual(flag(hook, "--probed"), "frontend")
        self.assertEqual([hook[i + 1] for i, a in enumerate(hook) if a == "--probe-path"], run.PROBE_PATHS)
        # The manifest is what kubectl apply receives.
        self.assertEqual(json.loads(json.dumps(job)), job)

    def test_ready_job_and_account(self):
        argv = container(run.ready_job("i"))["command"]
        self.assertEqual(argv[-1], "ready")
        self.assertEqual(flag(argv, "--deployments").split(","), run.APP_DEPLOYMENTS)
        rbac = run.evaluator_rbac("hk8s-0001")
        kinds = {item["kind"]: item for item in rbac["items"]}
        self.assertEqual(set(kinds), {"ServiceAccount", "Role", "RoleBinding"})
        rules = {tuple(rule["resources"]): set(rule["verbs"]) for rule in kinds["Role"]["rules"]}
        self.assertEqual(rules, {("pods",): {"get", "list", "watch", "delete"}, ("deployments",): {"get", "list"}})
        self.assertEqual(kinds["RoleBinding"]["subjects"][0]["namespace"], "hk8s-0001")

    def test_manifest_has_two_replicas_of_each_application_service(self):
        manifest = run.MANIFEST.read_text()
        _, services = run.split_manifest(manifest)
        documents = services.split("\n---\n")
        self.assertEqual(len(documents), len(run.APP_DEPLOYMENTS))
        for document in documents:
            self.assertIn("\n  replicas: 2\n", document)
            self.assertEqual(document.count('cpu: "1"'), 2)  # request = limit


if __name__ == "__main__":
    unittest.main()
