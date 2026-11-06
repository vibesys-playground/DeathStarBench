"""snapshot.py and the data tier's restore:  python3 -m unittest discover -s tests"""

import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

TASK_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(TASK_DIR))

import snapshot  # noqa: E402

MANIFEST = (TASK_DIR / "k8s" / "manifest.yaml").read_text()
VERSION = "0123456789abcdef0123"
GO_COLLECTIONS = (TASK_DIR / "evaluator" / "internal" / "snapshot" / "collections.go").read_text()
GO_BUILD = (TASK_DIR / "evaluator" / "internal" / "snapshot" / "build.go").read_text()


def deployments(text: str) -> dict[str, dict]:
    return {d["metadata"]["name"]: d for d in yaml.safe_load_all(text) if d and d["kind"] == "Deployment"}


class RestoreTest(unittest.TestCase):
    """Every MongoDB pod restores its database from the snapshot the Go builder writes."""

    def test_every_mongodb_restores_its_database_of_the_version(self):
        rendered = deployments(snapshot.render(MANIFEST, VERSION))
        databases = re.search(r"var Databases = \[\]string\{(.*?)\}", GO_COLLECTIONS).group(1)
        databases = re.findall(r'"([a-z-]+)"', databases)
        mongos = {name: d for name, d in rendered.items() if name.startswith("mongodb-")}
        self.assertEqual(sorted(f"{name.removeprefix('mongodb-')}-db" for name in mongos), sorted(databases))
        image = re.search(r'const MongoImage = "([^"]+)"', GO_COLLECTIONS).group(1)
        for name, d in mongos.items():
            pod = d["spec"]["template"]["spec"]
            (restore,), (mongod,) = pod["initContainers"], pod["containers"]
            database = f"{name.removeprefix('mongodb-')}-db"
            script = restore["command"][-1]
            self.assertIn(f"/snapshots/{VERSION}/{database}", script)
            self.assertIn('"$src/COMPLETE"', script)  # the builder's CompleteFile
            self.assertIn('cp -a "$src/db/." /data/db/', script)  # the builder's DataDir
            self.assertEqual(restore["image"], image)
            self.assertEqual(mongod["image"], image)  # the files are this mongod's
            self.assertEqual(mongod["args"], ["--wiredTigerCacheSizeGB", "0.25"])
            volumes = {v["name"]: v for v in pod["volumes"]}
            self.assertEqual(volumes["snapshots"]["hostPath"], {"path": snapshot.SNAPSHOT_ROOT, "type": "Directory"})
            mounts = {m["name"]: m for m in restore["volumeMounts"]}
            self.assertTrue(mounts["snapshots"]["readOnly"])
            data = [m for m in mongod["volumeMounts"] if m["mountPath"] == "/data/db"]
            self.assertEqual([m["name"] for m in data], [n for n in mounts if n != "snapshots"])
            self.assertIn("emptyDir", volumes[data[0]["name"]])  # a sample's writes stay in its namespace
        self.assertIn('CompleteFile = "COMPLETE"', GO_BUILD)
        self.assertIn('DataDir      = "db"', GO_BUILD)

    def test_memcached_caches_are_fixed(self):
        for name, d in deployments(MANIFEST).items():
            if name.startswith("memcached-"):
                (container,) = d["spec"]["template"]["spec"]["containers"]
                self.assertEqual(container["args"], ["-m", "64"])

    def test_render_needs_a_version(self):
        self.assertNotIn(snapshot.VERSION_PLACEHOLDER, snapshot.render(MANIFEST, VERSION))
        for bad in ["", "../x", "ABC", "v1"]:
            with self.assertRaises(snapshot.SnapshotError):
                snapshot.render(MANIFEST, bad)


class DataImageTest(unittest.TestCase):
    def test_the_dockerfile_has_the_data_stage_of_the_data_tiers_mongod(self):
        dockerfile = (TASK_DIR / "Dockerfile").read_text()
        image = re.search(r'const MongoImage = "docker.io/([^"]+)"', GO_COLLECTIONS).group(1)
        stage = re.search(r"^FROM (\S+) AS data$", dockerfile, re.M)
        self.assertIsNotNone(stage, "the Dockerfile has no data stage")
        self.assertEqual(stage.group(1), image)
        self.assertIn("COPY --from=evaluator /out/hotel-data /usr/local/bin/", dockerfile)
        # The task image (the last stage) runs hotel-data verify in the seed.
        last = dockerfile[dockerfile.rindex("\nFROM "):]
        self.assertRegex(last, r"COPY --from=evaluator [^\n]*/out/hotel-data")

    def test_the_tag_follows_the_files_it_is_built_from(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "evaluator").mkdir()
            (root / "Dockerfile").write_text("FROM x AS data\n")
            (root / "evaluator" / "a.go").write_text("package a\n")
            (root / "bench.py").write_text("x = 1\n")
            first = snapshot.tree_digest(root)
            (root / "bench.py").write_text("x = 2\n")  # not in the data image
            (root / "evaluator" / "__pycache__").mkdir()
            (root / "evaluator" / "__pycache__" / "a.pyc").write_bytes(b"\0")
            self.assertEqual(snapshot.tree_digest(root), first)
            (root / "evaluator" / "a.go").write_text("package b\n")
            self.assertNotEqual(snapshot.tree_digest(root), first)
            self.assertEqual(snapshot.data_image("reg:5000", root), f"reg:5000/hotel-k8s/data:{snapshot.tree_digest(root)}")

    def test_build_targets_the_data_stage(self):
        seen = []

        def run(argv, **kwargs):  # noqa: ANN001, ANN202
            seen.append(argv)
            return subprocess.CompletedProcess(argv, 0, stdout="")

        image = snapshot.build_data_image("reg:5000", "tcp://b:1", run=run)
        (argv,) = seen
        self.assertEqual(argv[argv.index("--opt") + 1], "target=data")
        self.assertIn(f"type=image,name={image},push=true,registry.insecure=true", argv)
        with self.assertRaises(snapshot.SnapshotError):
            snapshot.build_data_image("reg:5000", "tcp://b:1", run=lambda argv, **k: subprocess.CompletedProcess(argv, 1, stdout="boom"))


class FakeKubectl:
    """kubectl against a fake API: the data Job finishes after `polls` status reads."""

    def __init__(self, *, polls=2, succeed=True, namespace_exists=False):  # noqa: ANN001
        self.calls, self.polls, self.succeed, self.exists = [], polls, succeed, namespace_exists
        self.created = None

    def __call__(self, args, *, input_text=None, check=True):  # noqa: ANN001, ANN204
        self.calls.append(args)
        out, code = "", 0
        if args[:2] == ["create", "namespace"]:
            out, code = ("namespaces \"hk8s-data\" AlreadyExists", 1) if self.exists else ("created", 0)
        elif args[:2] == ["create", "-o"]:
            self.created = json.loads(input_text)
            out = "job.batch/data-x1\n"
        elif args[2:3] == ["get"]:
            self.polls -= 1
            status = {} if self.polls > 0 else ({"succeeded": 1} if self.succeed else {"failed": 1})
            out = json.dumps({"status": status})
        elif args[2:3] == ["logs"]:
            out = "building\n" + json.dumps({"version": VERSION, "databases": [], "seconds": 5.0}) + "\n"
        return subprocess.CompletedProcess(args, code, stdout=out)


class EnsureTest(unittest.TestCase):
    def ensure(self, kubectl, **kwargs):  # noqa: ANN001, ANN202
        clock = iter(range(0, 10_000, 10))
        return snapshot.ensure('{"format":3}', registry="reg:5000", builder="tcp://b:1", log=lambda line: None,
                               kubectl=kubectl, build=lambda registry, builder: f"{registry}/hotel-k8s/data:t",
                               clock=lambda: next(clock), sleep=lambda s: None, **kwargs)

    def test_the_job_builds_and_reports_the_version(self):
        kubectl = FakeKubectl(namespace_exists=True)
        ensured = self.ensure(kubectl, deadline_seconds=3600)
        self.assertEqual(ensured.version, VERSION)
        job = kubectl.created
        self.assertEqual(job["metadata"]["namespace"], snapshot.DATA_NAMESPACE)
        pod = job["spec"]["template"]["spec"]
        (container,) = pod["containers"]
        self.assertEqual(container["image"], "reg:5000/hotel-k8s/data:t")
        self.assertEqual(container["env"], [{"name": "HOTEL_DATASET_SPEC", "value": '{"format":3}'}])
        self.assertIn("hotel-data ensure --dataset /tmp/dataset.json --root /snapshots", container["command"][-1])
        self.assertEqual(pod["volumes"][0]["hostPath"], {"path": snapshot.SNAPSHOT_ROOT, "type": "DirectoryOrCreate"})
        self.assertEqual(container["resources"], {"requests": snapshot.JOB_REQUESTS, "limits": snapshot.JOB_LIMITS})
        self.assertEqual(job["spec"]["backoffLimit"], 0)

    def test_a_failed_job_fails_with_its_log(self):
        with self.assertRaises(snapshot.SnapshotError) as caught:
            self.ensure(FakeKubectl(succeed=False), deadline_seconds=3600)
        self.assertIn("failed; log tail", str(caught.exception))

    def test_past_the_deadline_the_job_keeps_building(self):
        kubectl = FakeKubectl(polls=10**6)
        with self.assertRaises(snapshot.SnapshotError) as caught:
            self.ensure(kubectl, deadline_seconds=100)
        self.assertIn("keeps building", str(caught.exception))
        self.assertFalse([c for c in kubectl.calls if "delete" in c])  # the Job is left to finish

    def test_the_namespace_must_be_creatable(self):
        class Refused(FakeKubectl):
            def __call__(self, args, **kwargs):  # noqa: ANN001, ANN204
                if args[:2] == ["create", "namespace"]:
                    return subprocess.CompletedProcess(args, 1, stdout="forbidden")
                return super().__call__(args, **kwargs)

        with self.assertRaises(snapshot.SnapshotError):
            self.ensure(Refused(), deadline_seconds=3600)


if __name__ == "__main__":
    unittest.main()
