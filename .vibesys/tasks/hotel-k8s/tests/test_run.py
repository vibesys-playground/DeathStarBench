"""Unit tests of run.py's pure parts:  python3 -m unittest discover -s tests"""

import importlib.util
import json
import random
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
    def test_fresh_seeds_per_sample(self):
        draws = [run.draw_sample_seeds() for _ in range(50)]
        for seeds in draws:
            for phase in ("closed", "open"):
                for key in ("seed", "fixture_seed"):
                    self.assertTrue(0 <= seeds[phase][key] <= run.SEED_LIMIT)
                    self.assertLess(seeds[phase][key], 2**63 - 1)
        self.assertEqual(len({seeds["closed"]["fixture_seed"] for seeds in draws}), len(draws))
        self.assertEqual(len({seeds["closed"]["seed"] for seeds in draws}), len(draws))

    def test_open_fixture_seed_is_derived_from_the_closed_one(self):
        # TrialSlot (evaluator/internal/apps/hotel) makes runs whose fixture seeds
        # differ by 1 to RunSlots-1 disjoint; the Go property test covers it.
        values = iter([7, 100, 200])
        seeds = run.draw_sample_seeds(lambda limit: next(values))
        self.assertEqual(seeds, {
            "closed": {"seed": 100, "fixture_seed": 7},
            "open": {"seed": 200, "fixture_seed": 8},
        })
        for closed in (0, 47, run.SEED_LIMIT - 1, random.randrange(run.SEED_LIMIT)):
            self.assertEqual(run.open_fixture_seed(closed) - closed, 1)

    def test_seeds_are_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            seeds = run.draw_sample_seeds()
            run.write_seeds(Path(directory), seeds)
            self.assertEqual(json.loads((Path(directory) / "seeds.json").read_text()), seeds)


class ManifestTest(unittest.TestCase):
    def test_loadgen_job_passes_the_phase_seeds(self):
        seeds = {"seed": 123, "fixture_seed": 456}
        job = run.loadgen_job("registry/loadgen:x", "open", seeds, 701.234)
        argv = container(job)["command"]
        self.assertEqual(argv[:2], ["sh", "-c"])
        self.assertEqual(flag(argv, "--seed"), "123")
        self.assertEqual(flag(argv, "--fixture-seed"), "456")
        self.assertEqual(flag(argv, "--rate"), "701.23")
        self.assertEqual(flag(argv, "--workload"), "/workload/open.toml")
        self.assertNotIn("--rate", run.loadgen_job("i", "closed", seeds, None)["spec"]["template"]["spec"]["containers"][0]["command"])
        resources = container(job)["resources"]
        self.assertEqual(resources["requests"], resources["limits"])
        self.assertFalse(job["spec"]["template"]["spec"]["automountServiceAccountToken"])

    def test_oracle_job(self):
        job = run.oracle_job("registry/loadgen:x", 987654321)
        self.assertEqual(job["kind"], "Job")
        self.assertEqual(job["metadata"]["name"], "oracle")
        pod = job["spec"]["template"]["spec"]
        self.assertEqual(pod["serviceAccountName"], run.EVALUATOR_ACCOUNT)
        self.assertEqual(pod["restartPolicy"], "Never")
        self.assertEqual(job["spec"]["backoffLimit"], 0)
        argv = container(job)["command"]
        self.assertEqual(argv[0], "hotel-correctness")
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
