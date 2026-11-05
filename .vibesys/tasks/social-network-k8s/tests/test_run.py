"""run.py's pure parts:  python -m unittest discover -s tests"""

import importlib.util
import json
import sys
import unittest
from pathlib import Path

TASK_DIR = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("sn_run", TASK_DIR / "run.py")
run = importlib.util.module_from_spec(spec)
sys.modules["sn_run"] = run
spec.loader.exec_module(run)


class ManifestTest(unittest.TestCase):
    def test_split(self):
        data, app = run.split_manifest(run.MANIFEST.read_text())
        self.assertEqual(app.count("\nkind: Deployment\n"), len(run.STATELESS))
        self.assertIn("tier: data", data)
        self.assertNotIn("\n    tier: app\n", data)

    def test_seed_job(self):
        job = run.SEED_JOB.format(image="registry/seed:x", cpu=run.SEED_CPU, memory=run.SEED_MEMORY,
                                  timeout=run.SEED_TIMEOUT_SECONDS)
        self.assertIn('command: ["seed", "--timeout", "1200s"]', job)


class CommandTest(unittest.TestCase):
    def test_restart_hook(self):
        argv = json.loads(run.restart_command_json("hk8s-sn-ab", 4242))
        self.assertEqual(argv[1:], [str(TASK_DIR / "run.py"), "ctl", "--namespace", "hk8s-sn-ab", "--port", "4242"])

    def test_ctl_refuses_other_targets(self):
        with self.assertRaisesRegex(run.JobError, "not a stateless"):
            run.ctl("hk8s-sn-ab", 1, "restart", "user-mongodb")
        with self.assertRaisesRegex(run.JobError, "not a cache"):
            run.ctl("hk8s-sn-ab", 1, "flush", "home-timeline-redis")

    def test_only_the_check(self):
        with self.assertRaises(SystemExit):
            run.main(["benchmark", "--project-root", "/nonexistent"])

    def test_job_timeout(self):
        self.assertEqual(run.job_timeout_seconds(), 2700)


if __name__ == "__main__":
    unittest.main()
