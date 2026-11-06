"""Tests of accuracy.py, the accuracy check's script:  python -m unittest discover -s tests"""

import re
import sys
import unittest
from pathlib import Path

TASK_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(TASK_DIR))

import accuracy  # noqa: E402
import bench  # noqa: E402
from vibesys_evaluator import microservices as ms  # noqa: E402


def deployments(tier: str) -> list[str]:
    """The names of the manifest's Deployments of a tier."""
    manifest = (TASK_DIR / bench.SYSTEM.manifest).read_text()
    names = []
    for document in manifest.split("\n---\n"):
        if "\nkind: Deployment\n" in f"\n{document}\n" and f"\n    tier: {tier}\n" in document:
            names.append(re.search(r"^  name: (\S+)$", document, re.M).group(1))
    return names


class AccuracyTest(unittest.TestCase):
    def test_the_faults_hit_every_service_and_cache_but_never_a_store(self):
        self.assertEqual(sorted(accuracy.APPLICATION), sorted(deployments("app")))
        by_phase = {}
        for fault in accuracy.CHECK.faults:
            by_phase.setdefault(fault.after, []).append((fault.action, fault.deployment))
        self.assertEqual(by_phase[0], [("restart", n) for n in accuracy.APPLICATION])
        self.assertEqual(by_phase[1], [("exec", c) for c in accuracy.MEMCACHED + accuracy.REDIS_CACHES])
        self.assertEqual(by_phase[2], [("restart_all", n) for n in accuracy.APPLICATION])
        data = deployments("data")
        for cache in accuracy.MEMCACHED + accuracy.REDIS_CACHES:
            self.assertIn(cache, data)
        targets = {f.deployment for f in accuracy.CHECK.faults}
        self.assertNotIn("home-timeline-redis", targets)  # a store: home timelines live only there
        self.assertFalse([t for t in targets if "mongodb" in t])
        self.assertEqual(set(accuracy.SYSTEM.restartable), set(accuracy.APPLICATION))
        self.assertEqual(dict(accuracy.SYSTEM.caches), {
            "post-storage-memcached": "memcached", "url-shorten-memcached": "memcached",
            "user-memcached": "memcached", "social-graph-redis": "redis", "user-timeline-redis": "redis",
        })

    def test_the_check_runs_the_accuracy_workload_and_the_oracle(self):
        self.assertIsInstance(accuracy.CHECK, ms.Accuracy)
        self.assertEqual((accuracy.CHECK.phases, accuracy.CHECK.rate, accuracy.CHECK.secs), (4, 300, 30))
        self.assertEqual(accuracy.WORKLOAD.command, ("sn-accuracy-generator",))
        self.assertEqual(accuracy.CHECK.oracle, ("sn-oracle",))
        dockerfile = (TASK_DIR / "Dockerfile").read_text()
        for program in ("sn-accuracy-generator", "sn-oracle", "sn-generator", "seed"):
            self.assertIn(f"/out/{program} ", dockerfile)


if __name__ == "__main__":
    unittest.main()
