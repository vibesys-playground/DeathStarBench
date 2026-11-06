"""bench.py and dataset.py:  python -m unittest discover -s tests   (needs vibesys-evaluator)"""

import dataclasses
import json
import re
import sys
import unittest
from pathlib import Path

TASK_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(TASK_DIR))

import bench  # noqa: E402
import dataset as ds  # noqa: E402


class DatasetTest(unittest.TestCase):
    def test_the_file_is_what_the_generator_reads(self):
        # snworkload.DatasetFile: {"seed": uint64, "digest": string}. vibesys-evaluator writes a
        # dataclass dataset as an object of its fields (declaration order), so Dataset's fields
        # are the file's.
        self.assertEqual([f.name for f in dataclasses.fields(ds.Dataset)], ["seed", "digest"])
        self.assertEqual(dataclasses.asdict(ds.generate()), {"seed": 0x5EED50C1A1, "digest": ds.DIGEST})

    def test_the_pins_are_the_go_datasets(self):
        go = (TASK_DIR / "evaluator" / "internal" / "dataset")
        self.assertIn(f"DefaultSeed uint64 = {hex(ds.SEED)}", (go / "dataset.go").read_text())
        (digest,) = re.findall(r'defaultDigest = "([0-9a-f]{64})"', (go / "dataset_test.go").read_text())
        self.assertEqual(ds.DIGEST, digest)


class SeedTest(unittest.TestCase):
    DATA = ds.generate()

    def test_command(self):
        self.assertEqual(bench.seed_command(self.DATA),
                         ["seed", "--dataset-seed", "407708418465", "--timeout", "1200s"])

    def test_summary(self):
        line = json.dumps({"dataset_digest": ds.DIGEST, "skipped": False})
        self.assertFalse(bench.seed_summary(f"seed: connecting\n{line}\n", self.DATA)["skipped"])
        with self.assertRaisesRegex(RuntimeError, "no summary"):
            bench.seed_summary("seed: connecting\n", self.DATA)
        with self.assertRaisesRegex(RuntimeError, "disagree"):
            bench.seed_summary(json.dumps({"dataset_digest": "00" * 32}), self.DATA)


class SystemTest(unittest.TestCase):
    def test_data_tier_and_application(self):
        manifest = (TASK_DIR / bench.SYSTEM.manifest).read_text()
        documents = manifest.split("\n---\n")
        deployments = [d for d in documents if "\nkind: Deployment\n" in f"\n{d}\n"]
        data = [d for d in deployments if "\n    tier: data\n" in d]
        app = [d for d in deployments if "\n    tier: app\n" in d]
        self.assertEqual(len(app), 12)  # 11 services and nginx-thrift
        self.assertEqual(len(data) + len(app), len(deployments))
        self.assertIn("name: nginx-thrift", "\n".join(d for d in documents if "kind: Service\n" in d))
        self.assertEqual(bench.SYSTEM.data_tier, "tier=data")
        self.assertEqual(dict(bench.SYSTEM.images), {"SOCIAL_NETWORK_IMAGE": "socialNetwork"})
        self.assertIs(bench.SYSTEM.seed, bench.seed)

    def test_generator_is_on_the_image(self):
        self.assertEqual(list(bench.WORKLOAD.command), ["sn-generator"])
        self.assertIn("/out/sn-generator", (TASK_DIR / "Dockerfile").read_text())


class Phase:
    def __init__(self, ops=0.0, p99=0.0):
        self.ops_per_sec, self.p99_ms = ops, p99


class FakeRun:
    def __init__(self, closed, p99s):
        self.closed, self.p99s, self.calls, self.reported = list(closed), list(p99s), [], None

    def closed_loop(self, **kwargs):
        self.calls.append(("closed", kwargs))
        return Phase(ops=self.closed.pop(0))

    def open_loop(self, **kwargs):
        self.calls.append(("open", kwargs))
        return Phase(p99=self.p99s.pop(0))

    def report(self, values):
        self.reported = values


class BenchmarkTest(unittest.TestCase):
    def test_phases(self):
        run = FakeRun([1000.0, 1200.0], [30.0, 10.0, 20.0])
        bench.benchmark(run)
        self.assertEqual(run.calls, [
            ("closed", {"clients": 32, "secs": 20, "warmup": 10}),
            ("closed", {"clients": 32, "secs": 20, "warmup": 10}),
        ] + [("open", {"rate": 770.0, "secs": 60, "warmup": 10, "arrivals": "uniform"})] * 3)
        self.assertEqual(run.reported, {"operations_per_second": 1100.0, "latency_ms.p99": 20.0})
        self.assertEqual(set(run.reported), set(bench.METRICS))


if __name__ == "__main__":
    unittest.main()
