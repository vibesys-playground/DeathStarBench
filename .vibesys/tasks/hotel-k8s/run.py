#!/usr/bin/env python3
"""Trusted wrapper for the hotel-k8s task: check and benchmark jobs.

    run.py check     --project-root ROOT
    run.py benchmark --project-root ROOT --vs-output PATH     (result protocol 2)
    run.py sweep                                              (delete stale namespaces only)

Every job builds the candidate image from ROOT/hotelReservation and the task's
evaluator image (loadgen, seed, hotel-correctness, podctl) with buildctl, pushes
them to the registry, deploys the task-owned data tier into a fresh namespace of
the cluster, seeds MongoDB with the task's dataset (a Job), deploys the
application services (two replicas each), waits until every replica serves (Job
"ready"), runs the oracle (Job "oracle") or the load generator (Jobs
"loadgen-closed", "loadgen-open") inside the namespace, and deletes the
namespace on every exit path (normal, error, SIGTERM, SIGINT, SIGHUP). Only
SIGKILL (which is what the CLI sends on cancel and timeout) leaks a namespace;
the next job sweeps stale ones (see sweep_stale_namespaces).

Environment (set by the vibesys-cli `kubernetes` resource, or by hand):
    KUBECONFIG              kubeconfig of the cluster (required)
    VIBESYS_K8S_REGISTRY    registry address as both the builder and the cluster's
                            nodes resolve it, for example localhost:5001 (required)
    VIBESYS_K8S_BUILDER     BuildKit address, for example tcp://127.0.0.1:1234 (required)
    VIBESYS_ARTIFACTS_DIR   optional; logs, summaries, seeds.json and pod usage go there
"""

from __future__ import annotations

import argparse
import atexit
import calendar
import hashlib
import json
import os
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import tomllib
import time
from pathlib import Path

TASK_DIR = Path(__file__).resolve().parent
MANIFEST = TASK_DIR / "k8s" / "manifest.yaml"
WORKLOADS = TASK_DIR / "workload"
NAMESPACE_LABEL = "vibesys.dev/hotel-k8s"
CREATED_LABEL = "vibesys.dev/hotel-k8s-created"  # epoch seconds
JOB_LABEL = "vibesys.dev/hotel-k8s-job"
SWEEP_MARGIN_SECONDS = 600

def tool(name: str) -> str:
    """Prebuilt binary: in the job image they are on PATH under /opt/vibesys/bin."""
    found = shutil.which(name)
    if not found:
        raise JobError(f"{name} not found on PATH (it is built into the task's Docker image)")
    return found


LOADGEN_BASE = "busybox:1.36@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
FRONTEND_URL = "http://frontend:5000"
# The application Deployments (two replicas each). They start only after the
# seed Job has filled MongoDB: each service seeds its own test data into an
# empty collection, and geo and recommendation load their collection at startup.
APP_DEPLOYMENTS = ["frontend", "geo", "profile", "rate", "recommendation", "reservation", "search", "user"]
# The application services that register every instance in consul (gRPC).
REGISTERED_DEPLOYMENTS = [name for name in APP_DEPLOYMENTS if name != "frontend"]
# Deployments the oracle's restart hook restarts: the stateless services and the
# reservation cache tier. MongoDB stays up, so its emptyDir keeps the data.
RESTART_DEPLOYMENTS = APP_DEPLOYMENTS + ["memcached-reserve"]
# Every frontend pod must answer these with 200 before the application is ready.
PROBE_PATHS = [
    "/user?username=Cornell_30&password=0000000000",
    "/recommendations?require=rate&lat=37.7867&lon=-122.4112",
    "/hotels?inDate=2300-01-01&outDate=2300-01-02&lat=37.7867&lon=-122.4112",
]
READY_TIMEOUT_SECONDS = 300
# Load-generator pod: requests == limits. It is sized so the generator is never
# the bottleneck: 32 closed-loop workers or 512 open-loop workers.
LOADGEN_CPU = "6"
LOADGEN_MEMORY = "4Gi"
# Oracle pod (requests == limits): one client at a time, bursts of 8.
ORACLE_CPU = "2"
ORACLE_MEMORY = "2Gi"
ORACLE_TIMEOUT_SECONDS = 1200
# Ready Job pod: podctl polls the API, consul and the frontend pods.
READY_CPU = "500m"
READY_MEMORY = "128Mi"
# Seed Job pod: requests == limits. It generates the dataset (about 0.5 s) and
# writes about 30 MB of documents into the six MongoDB servers concurrently.
SEED_CPU = "2"
SEED_MEMORY = "1Gi"
SEED_TIMEOUT_SECONDS = 600
# The service account of the oracle and ready Jobs: podctl reads Deployments and
# pods and deletes pods of the namespace, nothing else. The application's pods
# run as the namespace's default account, which may do nothing.
EVALUATOR_ACCOUNT = "evaluator"
# Seeds are drawn below this bound (they are int64 in the engine; the open
# phase's fixture seed is the closed one's plus 1).
SEED_LIMIT = 2**62


class JobError(Exception):
    """A failure that ends the job with a message."""


def log(message: str) -> None:
    print(f"[hotel-k8s {time.strftime('%H:%M:%S')}] {message}", file=sys.stderr, flush=True)


def require_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise JobError(f"{name} is not set (see run.py docstring)")
    return value


def run(argv, *, input_text=None, timeout=None, check=True, capture=True):
    result = subprocess.run(
        argv,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode != 0:
        tail = (result.stdout or "")[-2000:]
        raise JobError(f"{' '.join(argv[:4])} failed ({result.returncode}): {tail}")
    return result


class Kube:
    def __init__(self, namespace: str | None = None):
        self.namespace = namespace

    def kubectl(self, *args, namespaced=True, **kwargs):
        argv = ["kubectl"]
        if namespaced and self.namespace:
            argv += ["-n", self.namespace]
        return run(argv + list(args), **kwargs)


def tree_digest(root: Path, files=None) -> str:
    digest = hashlib.sha256()
    paths = sorted(p for p in (files or root.rglob("*")) if p.is_file() or p.is_symlink())
    for path in paths:
        rel = path.relative_to(root).as_posix()
        digest.update(rel.encode() + b"\0")
        if path.is_symlink():
            digest.update(b"L" + os.readlink(path).encode())
        else:
            digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()[:16]


def buildctl_build(context: Path, image: str) -> None:
    builder = require_env("VIBESYS_K8S_BUILDER")
    run(
        [
            "buildctl", "--addr", builder, "build",
            "--frontend", "dockerfile.v0",
            "--local", f"context={context}",
            "--local", f"dockerfile={context}",
            "--output", f"type=image,name={image},push=true,registry.insecure=true",
        ],
        timeout=1500,
    )


def build_hotel_image(project_root: Path) -> str:
    source = project_root / "hotelReservation"
    if not (source / "Dockerfile").is_file():
        raise JobError(f"{source}/Dockerfile not found")
    registry = require_env("VIBESYS_K8S_REGISTRY")
    image = f"{registry}/hotel-k8s/hotel:{tree_digest(source)}"
    log(f"building candidate image {image}")
    buildctl_build(source, image)
    return image


def build_loadgen_image(scratch: Path) -> str:
    """The in-cluster evaluator image: loadgen, seed, the oracle, podctl and the workloads."""
    registry = require_env("VIBESYS_K8S_REGISTRY")
    context = scratch / "loadgen"
    (context / "workload").mkdir(parents=True)
    binaries = ("loadgen", "seed", "hotel-correctness", "podctl")
    for name in binaries:
        shutil.copy(tool(name), context / name)
    for name in ("closed.toml", "open.toml"):
        shutil.copy(WORKLOADS / name, context / "workload" / name)
    (context / "Dockerfile").write_text(
        f"FROM {LOADGEN_BASE}\n"
        f"COPY {' '.join(binaries)} /usr/local/bin/\n"
        "COPY workload/ /workload/\n"
    )
    image = f"{registry}/hotel-k8s/loadgen:{tree_digest(context)}"
    log(f"building evaluator image {image}")
    buildctl_build(context, image)
    return image


def split_manifest(manifest: str) -> tuple[str, str]:
    """Split the rendered manifest into (data tier and config, application Deployments)."""
    data, app = [], []
    for document in manifest.split("\n---\n"):
        kind = name = ""
        in_metadata = False
        for line in document.splitlines():
            if line.startswith("kind:"):
                kind = line.split(":", 1)[1].strip()
            elif line.startswith("metadata:"):
                in_metadata = True
            elif in_metadata and line.startswith("  name:") and not name:
                name = line.split(":", 1)[1].strip().strip('"')
            elif line and not line.startswith((" ", "#")):
                in_metadata = False
        (app if kind == "Deployment" and name in APP_DEPLOYMENTS else data).append(document)
    if len(app) != len(APP_DEPLOYMENTS):
        raise JobError(f"manifest has {len(app)} of the application Deployments {APP_DEPLOYMENTS}")
    return "\n---\n".join(data), "\n---\n".join(app)


class Deployment:
    """A fresh namespace with the hotel application deployed in it."""

    def __init__(self, scratch: Path):
        self.scratch = scratch
        self.name = f"hk8s-{secrets.token_hex(4)}"
        self.kube = Kube(self.name)
        self.created = False

    # -- lifecycle ---------------------------------------------------------
    def create(self, hotel_image: str, evaluator_image: str, artifacts: Path | None) -> None:
        self.created = True  # even a half-created namespace must be deleted
        self.kube.kubectl("create", "namespace", self.name, namespaced=False)
        self.kube.kubectl(
            "label", "namespace", self.name, f"{NAMESPACE_LABEL}=1",
            f"{CREATED_LABEL}={int(time.time())}", f"{JOB_LABEL}={job_label()}",
            namespaced=False,
        )
        manifest = MANIFEST.read_text()
        manifest = manifest.replace("${NAMESPACE}", self.name).replace("${HOTEL_IMAGE}", hotel_image)
        data_tier, services = split_manifest(manifest)
        log(f"deploying the data tier into namespace {self.name}")
        self.kube.kubectl("apply", "-f", "-", input_text=data_tier)
        self.kube.kubectl("apply", "-f", "-", input_text=json.dumps(evaluator_rbac(self.name)))
        self.kube.kubectl(
            "wait", "--for=condition=Available", "deployment", "-l", "backend=mongodb",
            "--timeout=300s", timeout=330,
        )
        seed_database(self, evaluator_image, artifacts)
        log("deploying the application services")
        self.kube.kubectl("apply", "-f", "-", input_text=services)
        # Available needs every replica: maxUnavailable (25%) rounds down to 0.
        self.kube.kubectl(
            "wait", "--for=condition=Available", "deployment", "--all", "--timeout=300s",
            timeout=330,
        )
        self.wait_ready(evaluator_image, artifacts)

    def wait_ready(self, image: str, artifacts: Path | None) -> None:
        """Job "ready": every replica Ready, registered in consul, every frontend pod probed."""
        started = time.monotonic()
        self.kube.kubectl("apply", "-f", "-", input_text=json.dumps(ready_job(image)))
        succeeded, logs = wait_job(self, "ready", READY_TIMEOUT_SECONDS + 60)
        if artifacts:
            (artifacts / "ready.log").write_text(logs)
        if not succeeded:
            raise JobError(f"the application did not become ready; Job ready log tail:\n{logs[-1500:]}")
        log(f"every replica serves ({time.monotonic() - started:.0f}s):\n" + logs.strip()[-1500:])

    def delete(self) -> None:
        if not self.created:
            return
        self.created = False
        log(f"deleting namespace {self.name}")
        try:
            Kube().kubectl(
                "delete", "namespace", self.name, "--wait=true", "--timeout=240s",
                "--ignore-not-found", namespaced=False, check=False, timeout=270,
            )
        except subprocess.TimeoutExpired:
            log(f"namespace {self.name} deletion timed out")


def job_label() -> str:
    """The CLI job id, from $VIBESYS_ARTIFACTS_DIR (.../<task>/<job>/<step>); else a random token."""
    artifacts = os.environ.get("VIBESYS_ARTIFACTS_DIR", "")
    name = Path(artifacts).parent.name if artifacts else ""
    return name if name and all(c.isalnum() or c in "-_." for c in name) else secrets.token_hex(4)


def job_timeout_seconds() -> int:
    """The longest a job of this task may run, from the manifest's own timeouts."""
    manifest = tomllib.loads((TASK_DIR / "vibesys.input.toml").read_text())
    return max(int(manifest[section]["timeout_seconds"]) for section in ("accuracy", "benchmark"))


def sweep_stale_namespaces(now: float | None = None, ttl: float | None = None) -> list[str]:
    """Delete this task's namespaces that a killed job left behind.

    The CLI ends a cancelled or timed-out job with SIGKILL, so no cleanup runs. A namespace
    is stale when its creation label is older than the longest a job may run plus a margin,
    which no live job can reach, so a concurrent job's namespace is never touched.
    """
    ttl = job_timeout_seconds() + SWEEP_MARGIN_SECONDS if ttl is None else ttl
    now = time.time() if now is None else now
    result = Kube().kubectl(
        "get", "namespace", "-l", NAMESPACE_LABEL, "-o", "json", namespaced=False, check=False
    )
    if result.returncode != 0:
        return []
    swept = []
    for item in json.loads(result.stdout).get("items", []):
        meta = item["metadata"]
        name = meta["name"]
        if not name.startswith("hk8s-"):
            continue
        created = meta.get("labels", {}).get(CREATED_LABEL)
        if created is not None and created.isdigit():
            age = now - int(created)
        else:  # a namespace from before the label existed
            age = now - calendar.timegm(
                time.strptime(meta["creationTimestamp"], "%Y-%m-%dT%H:%M:%SZ")
            )
        if age > ttl:
            log(f"sweeping stale namespace {name} (job {meta['labels'].get(JOB_LABEL, '?')}, "
                f"{age:.0f}s old, limit {ttl:.0f}s)")
            Kube().kubectl("delete", "namespace", name, "--wait=false", namespaced=False, check=False)
            swept.append(name)
    return swept


# ---- in-namespace Jobs (rendered as JSON, which kubectl apply accepts) ----

def evaluator_rbac(namespace: str) -> dict:
    """The service account of the ready and oracle Jobs, allowed to restart pods."""
    return {
        "apiVersion": "v1", "kind": "List",
        "items": [
            {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": EVALUATOR_ACCOUNT}},
            {
                "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
                "metadata": {"name": EVALUATOR_ACCOUNT},
                "rules": [
                    {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch", "delete"]},
                    {"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get", "list"]},
                ],
            },
            {
                "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
                "metadata": {"name": EVALUATOR_ACCOUNT},
                "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": EVALUATOR_ACCOUNT},
                "subjects": [{"kind": "ServiceAccount", "name": EVALUATOR_ACCOUNT, "namespace": namespace}],
            },
        ],
    }


def job(name: str, image: str, command: list[str], cpu: str, memory: str,
        account: str | None = None) -> dict:
    """A one-pod Job, requests == limits, never retried."""
    spec: dict = {
        "restartPolicy": "Never",
        "terminationGracePeriodSeconds": 5,
        "containers": [{
            "name": name.split("-")[0],
            "image": image,
            "imagePullPolicy": "IfNotPresent",
            "resources": {"requests": {"cpu": cpu, "memory": memory}, "limits": {"cpu": cpu, "memory": memory}},
            "command": command,
        }],
    }
    if account:
        spec["serviceAccountName"] = account
    else:
        spec["automountServiceAccountToken"] = False
    return {
        "apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": name},
        "spec": {"backoffLimit": 0, "ttlSecondsAfterFinished": 600, "template": {"spec": spec}},
    }


def podctl_command(*action: str) -> list[str]:
    """podctl over the application: ready, or the restart hook (the oracle appends the mode)."""
    command = [
        "podctl",
        "--deployments", ",".join(APP_DEPLOYMENTS if action == ("ready",) else RESTART_DEPLOYMENTS),
        "--registered", ",".join(REGISTERED_DEPLOYMENTS),
        "--probed", "frontend", "--probe-port", "5000",
        "--timeout", f"{READY_TIMEOUT_SECONDS}s",
    ]
    for path in PROBE_PATHS:
        command += ["--probe-path", path]
    return command + list(action)


def ready_job(image: str) -> dict:
    return job("ready", image, podctl_command("ready"), READY_CPU, READY_MEMORY, EVALUATOR_ACCOUNT)


def oracle_job(image: str, seed: int) -> dict:
    """The semantic oracle, in the namespace, against the frontend Service.

    Its requests each open a connection, so kube-proxy spreads them over the
    frontend pods; its restarts go through podctl (one pod or all pods of each
    Deployment, see the accuracy adapter's Restarter)."""
    command = [
        "hotel-correctness", "--mode", "accuracy",
        "--workload", "/workload/closed.toml",
        "--base-url", FRONTEND_URL,
        "--seed", str(seed),
        "--startup-timeout", str(READY_TIMEOUT_SECONDS),
        "--restart-command-json", json.dumps(podctl_command("restart")),
    ]
    return job("oracle", image, command, ORACLE_CPU, ORACLE_MEMORY, EVALUATOR_ACCOUNT)


def loadgen_job(image: str, workload: str, seeds: dict, rate: float | None) -> dict:
    """One load-generator phase; its summary and result stream follow markers in its log."""
    flags = [
        "--mode", "benchmark", "--workload", f"/workload/{workload}.toml",
        "--base-url", FRONTEND_URL, "--startup-timeout", "120",
        "--seed", str(seeds["seed"]), "--fixture-seed", str(seeds["fixture_seed"]),
    ]
    if rate is not None:
        flags += ["--rate", f"{rate:.2f}"]
    flags += ["--output-json", "/tmp/summary.json", "--vs-output", "/tmp/stream.jsonl"]
    script = (
        'loadgen "$@"; rc=$?\n'
        'echo "::summary::"; cat /tmp/summary.json 2>/dev/null\n'
        'echo "::stream::"; cat /tmp/stream.jsonl 2>/dev/null\n'
        'exit $rc\n'
    )
    return job(f"loadgen-{workload}", image, ["sh", "-c", script, "loadgen", *flags], LOADGEN_CPU, LOADGEN_MEMORY)


def seed_job(image: str) -> dict:
    return job("seed", image, ["seed", "--timeout", f"{SEED_TIMEOUT_SECONDS}s"], SEED_CPU, SEED_MEMORY)


# ---- seeds ----

def open_fixture_seed(closed: int) -> int:
    """The open phase's fixture seed. Both phases run in one namespace and no
    reservation can be deleted, so their fixtures (reservation nights, sandbox
    cells, added hotel IDs) must be disjoint: the load generator's TrialSlot
    (evaluator/internal/apps/hotel) makes runs whose fixture seeds differ by
    1 to RunSlots-1 disjoint by construction."""
    return closed + 1


def draw_sample_seeds(randbelow=secrets.randbelow) -> dict:
    """Fresh seeds for one benchmark sample: a workload seed per phase and the
    closed phase's fixture seed, from which the open phase's is derived."""
    closed_fixture = randbelow(SEED_LIMIT)
    return {
        "closed": {"seed": randbelow(SEED_LIMIT), "fixture_seed": closed_fixture},
        "open": {"seed": randbelow(SEED_LIMIT), "fixture_seed": open_fixture_seed(closed_fixture)},
    }


def write_seeds(artifacts: Path | None, seeds: dict) -> None:
    """seeds.json: what a replay of this sample passes (--seed, --fixture-seed)."""
    log("seeds: " + json.dumps(seeds, sort_keys=True))
    if artifacts:
        (artifacts / "seeds.json").write_text(json.dumps(seeds, indent=2, sort_keys=True) + "\n")


# ---- per-pod usage, from the kubelet's summary ----

def pod_usage(deployment: "Deployment") -> dict:
    """CPU seconds and network bytes of every pod of the namespace, by pod name."""
    pods = json.loads(deployment.kube.kubectl("get", "pods", "-o", "json").stdout)["items"]
    usage: dict = {}
    for node in sorted({pod["spec"].get("nodeName") for pod in pods} - {None}):
        raw = Kube().kubectl("get", "--raw", f"/api/v1/nodes/{node}/proxy/stats/summary",
                             namespaced=False, timeout=30)
        for pod in json.loads(raw.stdout).get("pods", []):
            ref = pod.get("podRef", {})
            if ref.get("namespace") != deployment.name:
                continue
            usage[ref["name"]] = {
                "cpu_seconds": round(pod.get("cpu", {}).get("usageCoreNanoSeconds", 0) / 1e9, 2),
                "rx_bytes": pod.get("network", {}).get("rxBytes", 0),
                "tx_bytes": pod.get("network", {}).get("txBytes", 0),
                "start": pod.get("startTime", ""),
            }
    for pod in pods:
        name = pod["metadata"]["name"]
        if name in usage:
            usage[name]["deployment"] = pod["metadata"].get("labels", {}).get("app.kubernetes.io/name", "")
    return usage


def report_pod_usage(deployment: "Deployment", artifacts: Path | None, label: str) -> None:
    """Log the application pods' usage (both replicas of each service should work)."""
    try:
        usage = pod_usage(deployment)
    except (JobError, subprocess.TimeoutExpired, KeyError, json.JSONDecodeError) as error:
        log(f"pod usage {label}: unavailable ({error})")
        return
    lines = []
    for name, item in sorted(usage.items()):
        if item.get("deployment") in APP_DEPLOYMENTS:
            lines.append(f"  {name}: {item['cpu_seconds']:.1f} CPU s, rx {item['rx_bytes'] / 1e6:.1f} MB, "
                         f"tx {item['tx_bytes'] / 1e6:.1f} MB (since {item['start']})")
    log(f"pod usage {label}:\n" + "\n".join(lines))
    if artifacts:
        (artifacts / f"pod-usage-{label}.json").write_text(json.dumps(usage, indent=2, sort_keys=True) + "\n")


# ---- jobs ----

def do_check(project_root: Path, scratch: Path) -> int:
    deployment = Deployment(scratch)
    atexit.register(deployment.delete)
    sweep_stale_namespaces()
    artifacts = artifacts_dir()
    seed = secrets.randbelow(SEED_LIMIT)
    write_seeds(artifacts, {"oracle": {"seed": seed}})
    image = build_hotel_image(project_root)
    evaluator_image = build_loadgen_image(scratch)
    deployment.create(image, evaluator_image, artifacts)
    log(f"running the semantic oracle (Job oracle, seed {seed})")
    deployment.kube.kubectl("apply", "-f", "-", input_text=json.dumps(oracle_job(evaluator_image, seed)))
    succeeded, logs = wait_job(deployment, "oracle", ORACLE_TIMEOUT_SECONDS)
    if artifacts:
        (artifacts / "oracle.log").write_text(logs)
    sys.stderr.write(logs if len(logs) < 20000 else logs[:4000] + "\n...\n" + logs[-12000:])
    report_pod_usage(deployment, artifacts, "check")
    log("semantic oracle " + ("passed" if succeeded else "failed"))
    return 0 if succeeded else 1


def artifacts_dir() -> Path | None:
    value = os.environ.get("VIBESYS_ARTIFACTS_DIR", "")
    return Path(value) if value else None


def wait_job(deployment: Deployment, name: str, timeout: float) -> tuple[bool, str]:
    """Wait for a Job to finish; return (succeeded, its pod's log)."""
    deadline = time.monotonic() + timeout
    while True:
        status = json.loads(
            deployment.kube.kubectl("get", f"job/{name}", "-o", "json").stdout
        ).get("status", {})
        if status.get("succeeded") or status.get("failed"):
            break
        if time.monotonic() > deadline:
            raise JobError(f"Job {name} did not finish in time")
        time.sleep(3)
    logs = deployment.kube.kubectl("logs", f"job/{name}", check=False).stdout
    return bool(status.get("succeeded")), logs


def seed_database(deployment: Deployment, image: str, artifacts: Path | None) -> dict:
    """Write the task's dataset into the namespace's MongoDB servers (Job "seed")."""
    log("seeding MongoDB with the task's dataset: starting Job seed")
    started = time.monotonic()
    deployment.kube.kubectl("apply", "-f", "-", input_text=json.dumps(seed_job(image)))
    succeeded, logs = wait_job(deployment, "seed", SEED_TIMEOUT_SECONDS + 60)
    if artifacts:
        (artifacts / "seed.log").write_text(logs)
    if not succeeded:
        raise JobError(f"Job seed failed; log tail:\n{logs[-1500:]}")
    summary = None
    for line in logs.splitlines():
        if line.startswith("{"):
            summary = json.loads(line)
    if summary is None or not summary.get("written"):
        raise JobError(f"Job seed wrote no summary; log tail:\n{logs[-1500:]}")
    if artifacts:
        (artifacts / "seed.summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    documents = sum(item["documents"] for item in summary["written"])
    log(f"seeded {documents} documents (dataset {summary['dataset_digest'][:16]}, "
        f"documents {summary['documents_digest'][:16]}) in {time.monotonic() - started:.0f}s")
    return summary


def run_phase(deployment: Deployment, image: str, workload: str, artifacts: Path | None,
              seeds: dict, rate: float | None = None) -> dict:
    """Run one load-generator Job in the namespace; return its protocol-2 result values.

    seeds are the phase's --seed and --fixture-seed; rate, when given, overrides
    the workload's offered rate (operations/s).
    """
    name = f"loadgen-{workload}"
    log(f"phase {workload}: starting Job {name} (seed {seeds['seed']}, fixture seed {seeds['fixture_seed']})"
        + (f" at {rate:.2f} ops/s" if rate is not None else ""))
    deployment.kube.kubectl("apply", "-f", "-", input_text=json.dumps(loadgen_job(image, workload, seeds, rate)))
    _, logs = wait_job(deployment, name, 1500)
    if artifacts:
        (artifacts / f"{name}.log").write_text(logs)
    summary_text = stream_text = ""
    if "::summary::" in logs and "::stream::" in logs:
        head, _, stream_text = logs.partition("::stream::")
        summary_text = head.partition("::summary::")[2]
        log(f"phase {workload} loadgen output:\n" + head.partition("::summary::")[0][-1500:])
    else:
        raise JobError(f"Job {name} produced no result stream; log tail:\n{logs[-1500:]}")
    if artifacts:
        (artifacts / f"{name}.summary.json").write_text(summary_text)
        (artifacts / f"{name}.stream.jsonl").write_text(stream_text)
    values = None
    for line in stream_text.splitlines():
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        if record["kind"] == "error":
            raise JobError(f"phase {workload} failed: {record['message']}")
        if record["kind"] == "result":
            values = record["values"]
    if values is None:
        raise JobError(f"phase {workload} wrote no result record")
    return values


def write_stream(path: Path, records: list[dict]) -> None:
    path.write_text("".join(json.dumps(record) + "\n" for record in records))


# The open-loop phase offers this fraction of the sample's closed-loop throughput.
OPEN_LOOP_LOAD_FRACTION = 0.7

HELLO = {
    "kind": "hello",
    "protocol": 2,
    "metrics": {
        "operations_per_second": {"unit": "ops/s", "direction": "max"},
        "latency_ms.p99": {"unit": "ms", "direction": "min"},
    },
}


def do_benchmark(project_root: Path, scratch: Path, output: Path) -> int:
    write_stream(output, [HELLO])
    artifacts = artifacts_dir()
    try:
        deployment = Deployment(scratch)
        atexit.register(deployment.delete)
        sweep_stale_namespaces()
        # Fresh seeds for every sample; seeds.json lets any sample be replayed.
        seeds = draw_sample_seeds()
        write_seeds(artifacts, seeds)
        hotel_image = build_hotel_image(project_root)
        loadgen_image = build_loadgen_image(scratch)
        deployment.create(hotel_image, loadgen_image, artifacts)
        closed = run_phase(deployment, loadgen_image, "closed", artifacts, seeds["closed"])
        report_pod_usage(deployment, artifacts, "closed")
        throughput = closed["operations_per_second"]
        # p99 is measured at a fixed fraction of this sample's own closed-loop
        # throughput, so a faster candidate is held to proportionally more load.
        offered = OPEN_LOOP_LOAD_FRACTION * throughput
        open_loop = run_phase(deployment, loadgen_image, "open", artifacts, seeds["open"], rate=offered)
        report_pod_usage(deployment, artifacts, "open")
        p99 = open_loop["latency_ms.p99"]
        log(f"closed loop: {throughput:.1f} ops/s; open loop at {offered:.1f} ops/s "
            f"({OPEN_LOOP_LOAD_FRACTION:.0%}): p99 {p99:.2f} ms")
        if artifacts:
            (artifacts / "offered-rate.json").write_text(json.dumps({
                "closed_loop_operations_per_second": throughput,
                "open_loop_load_fraction": OPEN_LOOP_LOAD_FRACTION,
                "open_loop_offered_operations_per_second": offered,
            }) + "\n")
        write_stream(output, [HELLO, {
            "kind": "result", "label": "",
            "values": {"operations_per_second": throughput, "latency_ms.p99": p99},
        }])
        return 0
    except (JobError, subprocess.TimeoutExpired, KeyError, json.JSONDecodeError) as error:
        message = str(error) or type(error).__name__
        log(f"benchmark failed: {message}")
        write_stream(output, [HELLO, {"kind": "error", "message": message[:2000]}])
        return 1


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("check", "benchmark"):
        p = sub.add_parser(name)
        p.add_argument("--project-root", type=Path, required=True)
        if name == "benchmark":
            p.add_argument("--vs-output", type=Path, required=True)
    sub.add_parser("sweep")
    args = parser.parse_args(argv)

    def terminate(signum, _frame):
        raise SystemExit(128 + signum)

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, terminate)

    scratch = Path(tempfile.mkdtemp(prefix="hotel-k8s-"))
    # The job runs as the invoking user, whose HOME may not exist or be writable;
    # kubectl and buildctl write caches and client config there.
    os.environ.setdefault("KUBECACHEDIR", str(scratch / "kubecache"))
    if not os.access(os.environ.get("HOME", "/nonexistent"), os.W_OK):
        os.environ["HOME"] = str(scratch)
    atexit.register(shutil.rmtree, scratch, ignore_errors=True)
    try:
        require_env("KUBECONFIG")
        if args.command == "sweep":
            sweep_stale_namespaces()
            return 0
        require_env("VIBESYS_K8S_REGISTRY")
        require_env("VIBESYS_K8S_BUILDER")
        if args.command == "check":
            return do_check(args.project_root.resolve(), scratch)
        return do_benchmark(args.project_root.resolve(), scratch, args.vs_output)
    except JobError as error:
        log(f"failed: {error}")
        return 1
    finally:
        # atexit handlers run next; block further signals so cleanup cannot be cut short.
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, signal.SIG_IGN)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
