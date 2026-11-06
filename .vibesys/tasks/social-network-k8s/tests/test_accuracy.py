"""Tests of bench.py's accuracy check:  python -m unittest discover -s tests"""

import re
import sys
import unittest
from pathlib import Path

TASK_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(TASK_DIR))

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
        self.assertEqual(sorted(bench.APPLICATION), sorted(deployments("app")))
        by_phase = {}
        for fault in bench.ACCURACY.faults:
            by_phase.setdefault(fault.after, []).append((fault.action, fault.deployment))
        self.assertEqual(by_phase[0], [("restart", n) for n in bench.APPLICATION])
        self.assertEqual(by_phase[1], [("flush", c) for c in bench.CACHES])
        self.assertEqual(by_phase[2], [("restart_all", n) for n in bench.APPLICATION])
        data = deployments("data")
        for cache in bench.CACHES:
            self.assertIn(cache, data)
        targets = {f.deployment for f in bench.ACCURACY.faults}
        self.assertNotIn("home-timeline-redis", targets)  # a store: home timelines live only there
        self.assertFalse([t for t in targets if "mongodb" in t])
        self.assertEqual(set(bench.SYSTEM.restartable), set(bench.APPLICATION))
        self.assertEqual(dict(bench.SYSTEM.caches), {
            "post-storage-memcached": "memcached", "url-shorten-memcached": "memcached",
            "user-memcached": "memcached", "social-graph-redis": "redis", "user-timeline-redis": "redis",
        })

    def test_the_check_runs_its_own_workload_and_the_oracle(self):
        self.assertIsInstance(bench.ACCURACY, ms.Accuracy)
        self.assertEqual((bench.ACCURACY.phases, bench.ACCURACY.rate, bench.ACCURACY.secs), (4, 300, 30))
        self.assertIs(bench.ACCURACY.workload, bench.ACCURACY_WORKLOAD)
        self.assertEqual(bench.ACCURACY_WORKLOAD.command, ("sn-accuracy-generator",))
        self.assertEqual(bench.WORKLOAD.command, ("sn-generator",))
        self.assertEqual(bench.ACCURACY.oracle, ("sn-oracle",))
        dockerfile = (TASK_DIR / "Dockerfile").read_text()
        for program in ("sn-accuracy-generator", "sn-oracle", "sn-generator", "seed"):
            self.assertIn(f"/out/{program} ", dockerfile)


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
