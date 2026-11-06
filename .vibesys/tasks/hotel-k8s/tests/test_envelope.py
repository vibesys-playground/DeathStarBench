"""Tests of envelope.py, the application tier's validator:  python3 -m unittest discover -s tests"""

import copy
import random
import sys
import shutil
import subprocess
import tempfile
import unittest
from fractions import Fraction
from pathlib import Path

import yaml

TASK_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(TASK_DIR))

import envelope  # noqa: E402

TASK_TEXT = envelope.TASK_MANIFEST.read_text()
REFERENCE_TEXT = envelope.REFERENCE_APP.read_text()
REFERENCE_DOCS = [d for d in yaml.safe_load_all(REFERENCE_TEXT) if d]
REPO_ROOT = TASK_DIR.parent.parent.parent


def dump(docs: list[dict]) -> str:
    return yaml.dump_all(docs, Dumper=getattr(yaml, "CSafeDumper", yaml.SafeDumper), sort_keys=False)


def validate(docs: list[dict]) -> envelope.App:
    return envelope.validate(dump(docs), source="app.yaml", task_text=TASK_TEXT)


def mutated(change, name: str = "geo") -> list[dict]:  # noqa: ANN001
    """The reference with `change(deployment, pod_spec, container)` applied to one Deployment."""
    docs = copy.deepcopy(REFERENCE_DOCS)
    for doc in docs:
        if doc["kind"] == "Deployment" and doc["metadata"]["name"] == name:
            pod = doc["spec"]["template"]["spec"]
            change(doc, pod, pod["containers"][0])
    return docs


def set_resources(container: dict, cpu, memory) -> None:  # noqa: ANN001
    container["resources"] = {"requests": {"cpu": cpu, "memory": memory}, "limits": {"cpu": cpu, "memory": memory}}


class Rejects(unittest.TestCase):
    def assertRejected(self, docs: list[dict], *needles: str) -> str:
        with self.assertRaises(envelope.EnvelopeError) as caught:
            validate(docs)
        message = str(caught.exception)
        for needle in needles:
            self.assertIn(needle, message)
        return message


class ReferenceTest(Rejects):
    def test_reference_is_exactly_the_envelope(self):
        app = validate(REFERENCE_DOCS)
        self.assertEqual(app.cpu, envelope.APP_CPU)
        self.assertEqual(app.memory, envelope.APP_MEMORY)
        self.assertEqual(app.names, ["frontend", "geo", "profile", "rate", "recommendation", "reservation", "search", "user"])
        self.assertEqual(app.frontend, ("frontend",))
        self.assertTrue(all(d.replicas == 2 and d.cpu == 1 for d in app.deployments))
        mib = {d.name: d.memory // 2**20 for d in app.deployments}
        self.assertEqual(mib, {"frontend": 384, "geo": 1280, "profile": 256, "rate": 256, "recommendation": 1024,
                               "reservation": 256, "search": 256, "user": 384})

    def test_the_memory_envelope_is_8_gib(self):
        """An eleventh of the data's 112 GB (OBJECTIVE.md, Data): 8 GiB, not the 32 GiB before."""
        self.assertEqual(envelope.APP_MEMORY, 8 * 2**30)
        self.assertEqual(envelope.APP_CPU, 16)

    def test_base_candidate_carries_the_reference(self):
        """The base's own app.yaml is the reference topology, so its numbers are unchanged."""
        base = REPO_ROOT / envelope.CANDIDATE_APP
        if not base.is_file():
            self.skipTest("not in the DeathStarBench checkout")
        self.assertEqual([d for d in yaml.safe_load_all(base.read_text()) if d], REFERENCE_DOCS)
        self.assertEqual(envelope.load(REPO_ROOT).source, envelope.CANDIDATE_APP.as_posix())

    def test_task_manifest_keeps_the_data_tier_and_the_frontend_service(self):
        task = envelope.task_objects(TASK_TEXT)
        deployments = {name for kind, name in task.names if kind == "Deployment"}
        self.assertEqual(len(deployments), 11)  # 6 MongoDB, 3 memcached, consul, jaeger
        self.assertIn(("Service", "frontend"), task.names)
        docs = [d for d in yaml.safe_load_all(TASK_TEXT) if d]
        for doc in docs:
            if doc["kind"] == "Deployment":
                self.assertEqual(doc["metadata"]["labels"]["tier"], "data")

    def test_regrouped_fixture_fits(self):
        app = envelope.validate(
            (TASK_DIR / "tests" / "testdata" / "regrouped.yaml").read_text(), source="r", task_text=TASK_TEXT
        )
        self.assertEqual(app.frontend, ("web",))
        self.assertEqual(app.cpu, 15)


class EnvelopeSumTest(Rejects):
    def test_one_millicore_over_fails(self):
        docs = mutated(lambda d, p, c: set_resources(c, "1001m", "1280Mi"))
        message = self.assertRejected(docs, "envelope", "16.002 CPUs over the 16")
        self.assertIn("geo: 2 x 1.001", message)

    def test_one_byte_over_fails(self):
        docs = mutated(lambda d, p, c: set_resources(c, "1", 1280 * 2**20 + 1))
        self.assertRejected(docs, "envelope", "memory limits sum", "over the 8 GiB")

    def test_the_old_32_gib_split_fails(self):
        docs = copy.deepcopy(REFERENCE_DOCS)
        for doc in docs:
            if doc["kind"] == "Deployment":
                set_resources(doc["spec"]["template"]["spec"]["containers"][0], "1", "2Gi")
        self.assertRejected(docs, "the memory limits sum to 32 GiB over the 8 GiB")

    def test_seventeen_cpus_fail(self):
        docs = mutated(lambda d, p, c: d["spec"].__setitem__("replicas", 3))
        self.assertRejected(docs, "17 CPUs over the 16")

    def test_random_splits_pass_exactly_when_within(self):
        """Property: for random replica counts and limits, the verdict is sum <= envelope."""
        rng = random.Random(20261105)
        for _ in range(200):
            docs = copy.deepcopy(REFERENCE_DOCS)
            cpu_total, memory_total = Fraction(0), 0
            for doc in docs:
                if doc["kind"] != "Deployment":
                    continue
                replicas = rng.randint(2, 4)
                millicores = rng.choice([100, 250, 500, 750, 1000, 1500, 2000, 3000])
                mebibytes = rng.choice([64, 128, 256, 512, 1024])
                doc["spec"]["replicas"] = replicas
                container = doc["spec"]["template"]["spec"]["containers"][0]
                set_resources(container, f"{millicores}m", f"{mebibytes}Mi")
                cpu_total += replicas * Fraction(millicores, 1000)
                memory_total += replicas * mebibytes * 2**20
            within = cpu_total <= 16 and memory_total <= 8 * 2**30
            if within:
                app = validate(docs)
                self.assertEqual((app.cpu, app.memory), (cpu_total, memory_total))
            else:
                self.assertRejected(docs, "envelope")

    def test_quantities(self):
        for text, value in [("1", 1), ("1500m", Fraction(3, 2)), (0.5, Fraction(1, 2)), (2, 2),
                            ("2Gi", 2**31), ("512Mi", 2**29), ("1G", 10**9), (".5", Fraction(1, 2))]:
            self.assertEqual(envelope.parse_quantity(text), value, text)
        for bad in ["1e3", "-1", "1 Gi", "", "1Gb", True, None]:
            with self.assertRaises(ValueError, msg=bad):
                envelope.parse_quantity(bad)


class ContainerRulesTest(Rejects):
    def test_requests_must_equal_limits(self):
        def change(d, p, c):
            c["resources"]["requests"]["cpu"] = "500m"
        self.assertRejected(mutated(change), "Deployment geo: spec.template.spec.containers[0].resources.requests.cpu",
                            "requests must equal limits")

    def test_equal_spelled_differently_is_equal(self):
        def change(d, p, c):
            c["resources"]["requests"]["cpu"] = "1000m"
            c["resources"]["requests"]["memory"] = "1.25Gi"
        validate(mutated(change))

    def test_missing_requests_resources_and_extra_resources(self):
        self.assertRejected(mutated(lambda d, p, c: c["resources"].pop("requests")), "resources.requests: missing")
        self.assertRejected(mutated(lambda d, p, c: c.pop("resources")), "resources: missing")
        self.assertRejected(mutated(lambda d, p, c: c["resources"]["limits"].pop("memory")), "limits.memory: missing")

        def gpu(d, p, c):
            c["resources"]["limits"]["nvidia.com/gpu"] = 1
        self.assertRejected(mutated(gpu), "limits.nvidia.com/gpu: not allowed")

    def test_replicas_at_least_two(self):
        self.assertRejected(mutated(lambda d, p, c: d["spec"].__setitem__("replicas", 1)),
                            "Deployment geo: spec.replicas: 1: at least 2")
        self.assertRejected(mutated(lambda d, p, c: d["spec"].pop("replicas")), "spec.replicas: None")

    def test_gomaxprocs_equals_the_cpu_limit(self):
        def literal(value, cpu):  # noqa: ANN001, ANN202
            def change(d, p, c):
                set_resources(c, cpu, "1280Mi")
                c["env"] = [e for e in c["env"] if e["name"] != "GOMAXPROCS"] + [{"name": "GOMAXPROCS", "value": value}]
            return change
        validate(mutated(literal("1", "1")))
        validate(mutated(literal("1", "500m")))  # rounded up, as the downward API does
        message = self.assertRejected(mutated(literal("2", "1500m")), "17 CPUs")  # over, but GOMAXPROCS right
        self.assertNotIn("GOMAXPROCS", message)
        self.assertRejected(mutated(literal("2", "1")), "GOMAXPROCS='2' but the CPU limit is 1")
        self.assertRejected(mutated(literal("1", "1500m")), "want 2")
        self.assertRejected(mutated(lambda d, p, c: c.__setitem__("env", [e for e in c["env"] if e["name"] != "GOMAXPROCS"])),
                            "sets GOMAXPROCS 0 times")

        def divisor(d, p, c):
            c["env"][0]["valueFrom"]["resourceFieldRef"]["divisor"] = "1m"
        self.assertRejected(mutated(divisor), "env[0].valueFrom.resourceFieldRef")

        def twice(d, p, c):
            c["env"].append({"name": "GOMAXPROCS", "value": "8"})
        self.assertRejected(mutated(twice), "sets GOMAXPROCS 2 times")

    def test_only_the_candidate_image(self):
        self.assertRejected(mutated(lambda d, p, c: c.__setitem__("image", "busybox")),
                            "containers[0].image: 'busybox': only the candidate image")

    def test_no_privilege_host_or_node_selection(self):
        cases = {
            "hostNetwork: not allowed": lambda d, p, c: p.__setitem__("hostNetwork", True),
            "hostPID: not allowed": lambda d, p, c: p.__setitem__("hostPID", True),
            "nodeName: not allowed": lambda d, p, c: p.__setitem__("nodeName", "n1"),
            "nodeSelector: must be empty": lambda d, p, c: p.__setitem__("nodeSelector", {"a": "b"}),
            "tolerations: must be empty": lambda d, p, c: p.__setitem__("tolerations", [{"operator": "Exists"}]),
            "affinity: must be empty": lambda d, p, c: p.__setitem__("affinity", {"nodeAffinity": {}}),
            "priorityClassName: not allowed": lambda d, p, c: p.__setitem__("priorityClassName", "high"),
            "initContainers: not allowed": lambda d, p, c: p.__setitem__("initContainers", [copy.deepcopy(c)]),
            "securityContext.privileged: not allowed": lambda d, p, c: c.__setitem__("securityContext", {"privileged": True}),
            "capabilities.add: not allowed": lambda d, p, c: c.__setitem__("securityContext", {"capabilities": {"add": ["NET_ADMIN"]}}),
            "envFrom: not allowed": lambda d, p, c: c.__setitem__("envFrom", [{"configMapRef": {"name": "geo"}}]),
            "ports[0].hostPort: not allowed": lambda d, p, c: c["ports"][0].__setitem__("hostPort", 80),
            "volumes[1].hostPath: not allowed": lambda d, p, c: p["volumes"].append({"name": "h", "hostPath": {"path": "/"}}),
            "volumes[1].persistentVolumeClaim: not allowed": lambda d, p, c: p["volumes"].append(
                {"name": "v", "persistentVolumeClaim": {"claimName": "x"}}),
            "serviceAccountName": lambda d, p, c: p.__setitem__("serviceAccountName", "evaluator"),
            "metadata.labels.tier: reserved": lambda d, p, c: d["metadata"]["labels"].__setitem__("tier", "data"),
        }
        for needle, change in cases.items():
            with self.subTest(needle):
                self.assertRejected(mutated(change), "Deployment geo", needle)

    def test_message_lists_every_violation(self):
        def change(d, p, c):
            d["spec"]["replicas"] = 1
            c["image"] = "x"
            p["hostNetwork"] = True
        message = self.assertRejected(mutated(change))
        self.assertEqual(message.count("\n  - "), 3, message)


class ObjectRulesTest(Rejects):
    def test_only_deployments_services_and_configmaps(self):
        for kind, api in [("DaemonSet", "apps/v1"), ("Pod", "v1"), ("Secret", "v1"), ("PersistentVolumeClaim", "v1"),
                          ("Role", "rbac.authorization.k8s.io/v1"), ("Namespace", "v1")]:
            with self.subTest(kind):
                docs = copy.deepcopy(REFERENCE_DOCS) + [{"apiVersion": api, "kind": kind, "metadata": {"name": "x"}}]
                self.assertRejected(docs, f"{kind} ({api}) not allowed")

    def test_configmap_of_its_own_is_allowed(self):
        def mount(d, p, c):
            p["volumes"].append({"name": "merged", "configMap": {"name": "merged"}})
        configmap = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "merged"}, "data": {"a": "b"}}
        validate(mutated(mount) + [configmap])
        self.assertRejected(mutated(mount), "volumes[1].configMap.name", "'merged' is not a ConfigMap")

    def test_task_names_are_the_tasks(self):
        def rename(name):  # noqa: ANN001, ANN202
            return lambda d, p, c: d["metadata"].__setitem__("name", name)
        self.assertRejected(mutated(rename("mongodb-geo")), "Deployment mongodb-geo", "the task's data tier owns")
        docs = copy.deepcopy(REFERENCE_DOCS) + [{"apiVersion": "v1", "kind": "Service", "metadata": {"name": "frontend"},
                                                 "spec": {"ports": [{"port": 5000}], "selector": {"app": "x"}}}]
        self.assertRejected(docs, "Service frontend", "the task owns this Service")
        docs = copy.deepcopy(REFERENCE_DOCS) + [{"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "geo"}}]
        self.assertRejected(docs, "ConfigMap geo", "the task owns this ConfigMap")

    def test_services_are_cluster_internal(self):
        docs = copy.deepcopy(REFERENCE_DOCS)
        service = next(d for d in docs if d["kind"] == "Service")
        service["spec"]["type"] = "NodePort"
        service["spec"]["ports"][0]["nodePort"] = 30000
        self.assertRejected(docs, "spec.type: must be ClusterIP", "nodePort: not allowed")
        service["spec"] = {"type": "ExternalName", "externalName": "example.com"}
        self.assertRejected(docs, "externalName: not allowed")

    def test_frontend_service_contract(self):
        def unlabel(d, p, c):
            d["spec"]["template"]["metadata"]["labels"]["service"] = "web"
            d["spec"]["selector"]["matchLabels"]["service"] = "web"
        self.assertRejected(mutated(unlabel, "frontend"), "Service frontend", "no Deployment's pods carry its selector")

        def merged_frontend(d, p, c):
            d["metadata"]["name"] = "web"
        app = validate(mutated(merged_frontend, "frontend"))
        self.assertEqual(app.frontend, ("web",))

    def test_pods_must_not_impersonate_the_data_tier(self):
        def hijack(d, p, c):
            d["spec"]["template"]["metadata"]["labels"].update(
                {"app.kubernetes.io/name": "mongodb-geo", "service": "mongodb-geo"})
            d["spec"]["selector"]["matchLabels"].update({"app.kubernetes.io/name": "mongodb-geo", "service": "mongodb-geo"})
        self.assertRejected(mutated(hijack), "match the task's Service mongodb-geo")

    def test_selectors_do_not_overlap(self):
        def broad(d, p, c):
            d["spec"]["selector"]["matchLabels"] = {"app.kubernetes.io/instance": "hotel"}
        self.assertRejected(mutated(broad), "Deployment geo: spec.selector", "also selects the pods of Deployment")

    def test_variables_and_namespace(self):
        text = REFERENCE_TEXT.replace("image: ${HOTEL_IMAGE}", "image: ${OTHER_IMAGE}", 1)
        with self.assertRaises(envelope.EnvelopeError) as caught:
            envelope.validate(text, source="app.yaml", task_text=TASK_TEXT)
        self.assertIn("unknown variable ${OTHER_IMAGE}", str(caught.exception))
        self.assertRejected(mutated(lambda d, p, c: d["metadata"].__setitem__("namespace", "kube-system")),
                            "metadata.namespace: must be ${NAMESPACE}")

    def test_malformed_objects_are_reported(self):
        docs = copy.deepcopy(REFERENCE_DOCS) + [{"apiVersion": "v1", "kind": "ConfigMap", "metadata": ["x"]}, "text"]
        self.assertRejected(docs, "ConfigMap None: metadata: want a mapping", "document 17: not a mapping")

    def test_not_yaml(self):
        with self.assertRaises(envelope.EnvelopeError) as caught:
            envelope.validate("a: [", source="app.yaml", task_text=TASK_TEXT)
        self.assertIn("not YAML", str(caught.exception))


class LoadTest(unittest.TestCase):
    def test_candidate_file_fallback_and_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.assertIn("the reference", envelope.load(root).source)
            path = root / envelope.CANDIDATE_APP
            path.parent.mkdir(parents=True)
            path.write_text(dump(mutated(lambda d, p, c: d["spec"].__setitem__("replicas", 1))))
            with self.assertRaises(envelope.EnvelopeError) as caught:
                envelope.load(root)
            self.assertIn("hotelReservation/deploy/app.yaml breaks", str(caught.exception))
            path.unlink()
            path.symlink_to(envelope.REFERENCE_APP)
            with self.assertRaises(envelope.EnvelopeError):
                envelope.load(root)

    def test_composed_manifest_renders_with_the_application_after_the_seed(self):
        # vibesys-evaluator applies the objects labelled tier=data (k8s.App(data_tier=)) before the
        # seed and the rest after it: the candidate's Deployments are all in the rest.
        app = envelope.validate(
            (TASK_DIR / "tests" / "testdata" / "regrouped.yaml").read_text(), source="r", task_text=TASK_TEXT
        )
        docs = [d for d in yaml.safe_load_all(envelope.compose(app, "0123456789abcdef0123")) if d]

        def data_tier(doc: dict) -> bool:
            return doc.get("metadata", {}).get("labels", {}).get("tier") == "data"

        rest = [d for d in docs if not data_tier(d)]
        self.assertEqual([d["metadata"]["name"] for d in rest if d["kind"] == "Deployment"], app.names)
        self.assertIn(("Service", "frontend"), {(d["kind"], d["metadata"]["name"]) for d in docs})
        self.assertTrue(any(data_tier(d) and d["kind"] == "Deployment" for d in docs))


class FakeEnsure:
    """snapshot.ensure as the bench calls it: records the call, returns a version or fails."""

    def __init__(self, error=None):  # noqa: ANN001
        self.calls, self.error = [], error

    def __call__(self, spec_json, **kwargs):  # noqa: ANN001, ANN204
        import snapshot  # noqa: PLC0415

        self.calls.append((spec_json, kwargs))
        if self.error:
            raise snapshot.SnapshotError(self.error)
        return snapshot.Ensured(version="0123456789abcdef0123", seconds=1.0, summary={})


ENV = {"VIBESYS_K8S_REGISTRY": "registry:5000", "VIBESYS_K8S_BUILDER": "tcp://builder:1234"}


class BenchPrepareTest(unittest.TestCase):
    def tearDown(self):
        import bench  # noqa: PLC0415

        bench._PREPARE_ERROR.clear()

    def test_prepare_writes_the_deployed_manifest_or_keeps_the_violation(self):
        import bench
        import dataset

        # The task image holds regular files only: vibesys-cli builds it without symlinks (j10).
        self.assertFalse([p for p in TASK_DIR.rglob("*") if p.is_symlink()])
        with tempfile.TemporaryDirectory() as directory:
            root, target = Path(directory) / "repo", Path(directory) / "out" / "manifest.yaml"
            ensure = FakeEnsure()
            app = bench.prepare(["benchmark", "--project-root", str(root), "--vs-output", "x"], target,
                                ensure=ensure, environ=ENV)
            self.assertEqual(target.read_text(), envelope.compose(app, "0123456789abcdef0123"))
            self.assertIn("/snapshots/0123456789abcdef0123/reservation-db", target.read_text())
            self.assertNotIn("@DATA_SNAPSHOT_VERSION@", target.read_text())
            (spec_json, kwargs), = ensure.calls
            self.assertEqual(spec_json.encode(), dataset.encode(dataset.SPEC))
            self.assertEqual((kwargs["registry"], kwargs["builder"]), ("registry:5000", "tcp://builder:1234"))
            self.assertIsNone(bench.prepare(["phase", "--work", "w"], target, ensure=ensure, environ=ENV))
            self.assertEqual(len(ensure.calls), 1)
            path = root / envelope.CANDIDATE_APP
            path.parent.mkdir(parents=True)
            path.write_text(dump(mutated(lambda d, p, c: d["spec"].__setitem__("replicas", 3))))
            self.assertIsNone(bench.prepare(["benchmark", "--project-root", str(root)], target, ensure=ensure, environ=ENV))
            self.assertFalse(target.exists())
            self.assertEqual(len(ensure.calls), 1)  # the envelope fails first: no snapshot work
            with self.assertRaises(envelope.EnvelopeError) as caught:
                bench.dataset()
            self.assertIn("17 CPUs over the 16", str(caught.exception))

    def test_a_snapshot_failure_fails_the_sample(self):
        import bench
        import snapshot

        with tempfile.TemporaryDirectory() as directory:
            root, target = Path(directory) / "repo", Path(directory) / "manifest.yaml"
            ensure = FakeEnsure("the data snapshot is not ready after 2700s")
            self.assertIsNone(bench.prepare(["benchmark", "--project-root", str(root)], target, ensure=ensure, environ=ENV))
            self.assertFalse(target.exists())
            with self.assertRaises(snapshot.SnapshotError) as caught:
                bench.dataset()
            self.assertIn("not ready", str(caught.exception))
            bench._PREPARE_ERROR.clear()
            self.assertIsNone(bench.prepare(["benchmark", "--project-root", str(root)], target, ensure=FakeEnsure(), environ={}))
            with self.assertRaises(snapshot.SnapshotError):
                bench.dataset()


class BenchSeedTest(unittest.TestCase):
    def test_seed_verifies_every_server_against_the_spec(self):
        import bench
        import dataset

        seen = []

        def run(argv, check):  # noqa: ANN001, ANN202
            seen.append((argv, Path(argv[argv.index("--dataset") + 1]).read_bytes()))
            return subprocess.CompletedProcess(argv, 0)

        bench.seed(dataset.generate(), run=run)
        (argv, spec), = seen
        self.assertEqual(argv[:2], ["hotel-data", "verify"])
        self.assertEqual(spec, dataset.encode(dataset.SPEC))
        servers = [argv[i + 1] for i, a in enumerate(argv) if a == "--server"]
        self.assertEqual(servers, [f"{db}={address}" for db, address in bench.MONGO.items()])
        with self.assertRaises(RuntimeError):
            bench.seed(dataset.generate(), run=lambda argv, check: subprocess.CompletedProcess(argv, 1))


if __name__ == "__main__":
    unittest.main()

