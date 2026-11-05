#!/usr/bin/env python3
"""Trusted wrapper for the hotel-k8s task's accuracy check (the benchmark is bench.py).

    run.py check     --project-root ROOT
    run.py data                                               (build the data snapshot only)
    run.py sweep                                              (delete stale namespaces only)

The check builds the candidate image from ROOT/hotelReservation and the task
image (this directory: dataset.py, bench.py, hotel-correctness, podctl) with
buildctl, pushes them to the registry, makes sure the node holds the data
snapshot of the task's dataset (snapshot.py: the data Job builds it the first
time, about 25 minutes, and later only checks it), deploys the task-owned data
tier into a fresh namespace of the cluster (each MongoDB pod restores its
database from the snapshot), checks what MongoDB holds (Job "seed": bench.py's
seed, as a benchmark sample checks it), deploys the candidate's
application tier (ROOT/hotelReservation/deploy/app.yaml, validated against the
capacity envelope by envelope.py before anything is built), waits until every
replica serves (Job "ready"),
runs the semantic oracle over dataset.py's dataset file (Job "oracle") inside
the namespace, and deletes the namespace on every exit path (normal, error,
SIGTERM, SIGINT, SIGHUP). Only SIGKILL (which is what the CLI sends on cancel
and timeout) leaks a namespace; the next job sweeps stale ones (see
sweep_stale_namespaces).

Environment (set by the vibesys-cli `kubernetes` resource, or by hand):
    KUBECONFIG              kubeconfig of the cluster (required)
    VIBESYS_K8S_REGISTRY    registry address as both the builder and the cluster's
                            nodes resolve it, for example localhost:5001 (required)
    VIBESYS_K8S_BUILDER     BuildKit address, for example tcp://127.0.0.1:1234 (required)
    VIBESYS_ARTIFACTS_DIR   optional; logs and seeds.json go there
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
sys.path.insert(0, str(TASK_DIR))

import envelope  # noqa: E402 - the task directory is on the path now
import snapshot  # noqa: E402

MANIFEST = envelope.TASK_MANIFEST  # the data tier, the ConfigMaps and the frontend Service
ORACLE_WORKLOAD = "/task/workload/oracle.toml"  # in the task image
NAMESPACE_LABEL = "vibesys.dev/hotel-k8s"
CREATED_LABEL = "vibesys.dev/hotel-k8s-created"  # epoch seconds
JOB_LABEL = "vibesys.dev/hotel-k8s-job"
SWEEP_MARGIN_SECONDS = 600

FRONTEND_URL = "http://frontend:5000"
# The application Deployments are the candidate's (envelope.App.names). They start
# only after the seed Job has checked MongoDB's restored data: each service seeds its own
# test data into an empty collection, and geo and recommendation load their collection at
# startup. The oracle's restart hook restarts them and the reservation cache tier;
# MongoDB stays up, so its emptyDir keeps the data.
RESTART_CACHES = ["memcached-reserve"]
# Every frontend pod must answer these with 200 before the application is ready.
PROBE_PATHS = [
    "/user?username=Cornell_30&password=0000000000",
    "/recommendations?require=rate&lat=37.7867&lon=-122.4112",
    "/hotels?inDate=2300-01-01&outDate=2300-01-02&lat=37.7867&lon=-122.4112",
]
READY_TIMEOUT_SECONDS = 300
# Oracle pod (requests == limits): one client at a time, bursts of 8. It writes
# dataset.py's dataset file (the spec) and generates the dataset from it in memory
# (about 3 s and 150 MB for 1.5M hotels; a hotel's full record and its nights are
# regenerated when asked for).
ORACLE_CPU = "2"
ORACLE_MEMORY = "2Gi"
ORACLE_TIMEOUT_SECONDS = 1200
# Ready Job pod: podctl polls the API, consul and the frontend pods.
READY_CPU = "500m"
READY_MEMORY = "128Mi"
# Seed Job pod: requests == limits. bench.py's seed generates the dataset and checks the
# restored servers against it (hotel-data verify: markers, counts, indexes, sampled
# documents; about a minute).
SEED_CPU = "4"
SEED_MEMORY = "2Gi"
SEED_TIMEOUT_SECONDS = 600
# The data tier's MongoDB pods copy their snapshot before mongod starts (reservation-db:
# about 50 GB, one to two minutes).
DATA_TIER_TIMEOUT_SECONDS = 900
# How long a check waits for the data snapshot: a first build takes about 25 minutes; past
# this the check fails and the data Job keeps building for the next job.
SNAPSHOT_WAIT_SECONDS = 2700
# The service account of the oracle and ready Jobs: podctl reads Deployments and
# pods and deletes pods of the namespace, nothing else. The application's pods
# run as the namespace's default account, which may do nothing.
EVALUATOR_ACCOUNT = "evaluator"
# The oracle's seed is drawn below this bound (an int64 in the engine).
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


def build_task_image() -> str:
    """The task image in the cluster's registry (this directory and its Dockerfile)."""
    registry = require_env("VIBESYS_K8S_REGISTRY")
    image = f"{registry}/hotel-k8s/task:{tree_digest(TASK_DIR, task_files())}"
    log(f"building task image {image}")
    buildctl_build(TASK_DIR, image)
    return image


def task_files() -> list[Path]:
    """The task directory's files, without the caches tools leave."""
    return [p for p in TASK_DIR.rglob("*")
            if "__pycache__" not in p.parts and p.suffix != ".pyc" and (p.is_file() or p.is_symlink())]


def dataset_digest() -> str:
    """The digest bench.py's seed checks: SHA-256 of dataset.py's dataset file."""
    sys.path.insert(0, str(TASK_DIR))
    import dataset  # noqa: PLC0415 - needs vibesys-evaluator, which the task image has

    return hashlib.sha256(dataset.encode(dataset.generate())).hexdigest()


class Deployment:
    """A fresh namespace with the hotel application deployed in it."""

    def __init__(self):
        self.name = f"hk8s-{secrets.token_hex(4)}"
        self.kube = Kube(self.name)
        self.created = False

    # -- lifecycle ---------------------------------------------------------
    def create(self, app: envelope.App, hotel_image: str, evaluator_image: str, digest: str,
               data_version: str, artifacts: Path | None) -> None:
        self.created = True  # even a half-created namespace must be deleted
        self.kube.kubectl("create", "namespace", self.name, namespaced=False)
        self.kube.kubectl(
            "label", "namespace", self.name, f"{NAMESPACE_LABEL}=1",
            f"{CREATED_LABEL}={int(time.time())}", f"{JOB_LABEL}={job_label()}",
            namespaced=False,
        )
        def render(text: str) -> str:
            return text.replace("${NAMESPACE}", self.name).replace("${HOTEL_IMAGE}", hotel_image)

        data_tier = render(snapshot.render(MANIFEST.read_text(), data_version))
        services = render(app.text)
        log(f"deploying the data tier into namespace {self.name}")
        self.kube.kubectl("apply", "-f", "-", input_text=data_tier)
        self.kube.kubectl("apply", "-f", "-", input_text=json.dumps(evaluator_rbac(self.name)))
        self.kube.kubectl(
            "wait", "--for=condition=Available", "deployment", "-l", "backend=mongodb",
            f"--timeout={DATA_TIER_TIMEOUT_SECONDS}s", timeout=DATA_TIER_TIMEOUT_SECONDS + 30,
        )
        seed_database(self, evaluator_image, digest, artifacts)
        log(f"deploying the application tier: {', '.join(app.names)}")
        self.kube.kubectl("apply", "-f", "-", input_text=services)
        # Available needs every replica: maxUnavailable (25%) rounds down to 0.
        self.kube.kubectl(
            "wait", "--for=condition=Available", "deployment", "--all", "--timeout=300s",
            timeout=330,
        )
        self.wait_ready(app, evaluator_image, artifacts)

    def wait_ready(self, app: envelope.App, image: str, artifacts: Path | None) -> None:
        """Job "ready": every replica Ready, every registering pod in consul, every frontend pod probed."""
        started = time.monotonic()
        self.kube.kubectl("apply", "-f", "-", input_text=json.dumps(ready_job(app, image)))
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


def podctl_command(app: envelope.App, *action: str) -> list[str]:
    """podctl over the application: ready, or the restart hook (the oracle appends the mode).

    Every candidate Deployment is waited for (and restarted, with the reservation cache);
    a Deployment whose pods register in consul must have every pod registered
    (--registered-auto: merged services may register only what they serve, or nothing);
    the pods of the Deployments behind the frontend Service are probed."""
    deployments = app.names if action == ("ready",) else app.names + RESTART_CACHES
    command = [
        "podctl",
        "--deployments", ",".join(deployments),
        "--registered-auto",
        "--probed", ",".join(app.frontend), "--probe-port", str(envelope.FRONTEND_PORT),
        "--timeout", f"{READY_TIMEOUT_SECONDS}s",
    ]
    for path in PROBE_PATHS:
        command += ["--probe-path", path]
    return command + list(action)


def ready_job(app: envelope.App, image: str) -> dict:
    return job("ready", image, podctl_command(app, "ready"), READY_CPU, READY_MEMORY, EVALUATOR_ACCOUNT)


def oracle_job(app: envelope.App, image: str, seed: int) -> dict:
    """The semantic oracle, in the namespace, against the frontend Service.

    Its requests each open a connection, so kube-proxy spreads them over the
    frontend pods; its restarts go through podctl (one pod of each candidate
    Deployment while the others serve, or all of their pods; see the accuracy
    adapter's Restarter)."""
    oracle = [
        "hotel-correctness", "--mode", "accuracy",
        "--dataset", "/tmp/dataset.json",
        "--workload", ORACLE_WORKLOAD,
        "--base-url", FRONTEND_URL,
        "--seed", str(seed),
        "--startup-timeout", str(READY_TIMEOUT_SECONDS),
        "--restart-command-json", json.dumps(podctl_command(app, "restart")),
    ]
    script = 'python /task/dataset.py /tmp/dataset.json && exec "$@"'
    return job("oracle", image, ["sh", "-c", script, "oracle", *oracle], ORACLE_CPU, ORACLE_MEMORY,
               EVALUATOR_ACCOUNT)


def seed_job(image: str, digest: str) -> dict:
    """bench.py's seed: it regenerates the dataset, checks its digest and checks MongoDB's data."""
    return job("seed", image, ["python", "/task/bench.py", "seed", "--dataset-digest", digest],
               SEED_CPU, SEED_MEMORY)


# ---- jobs ----

def write_seeds(artifacts: Path | None, seeds: dict) -> None:
    """seeds.json: what a replay of this check passes."""
    log("seeds: " + json.dumps(seeds, sort_keys=True))
    if artifacts:
        (artifacts / "seeds.json").write_text(json.dumps(seeds, indent=2, sort_keys=True) + "\n")


def do_check(project_root: Path) -> int:
    # The envelope is checked first: a candidate outside it fails before any build or deploy.
    app = envelope.load(project_root, log=log)
    deployment = Deployment()
    atexit.register(deployment.delete)
    sweep_stale_namespaces()
    artifacts = artifacts_dir()
    seed = secrets.randbelow(SEED_LIMIT)
    write_seeds(artifacts, {"oracle": {"seed": seed}})
    digest = dataset_digest()
    image = build_hotel_image(project_root)
    evaluator_image = build_task_image()
    data_version = ensure_snapshot()
    deployment.create(app, image, evaluator_image, digest, data_version, artifacts)
    log(f"running the semantic oracle (Job oracle, seed {seed})")
    deployment.kube.kubectl("apply", "-f", "-", input_text=json.dumps(oracle_job(app, evaluator_image, seed)))
    succeeded, logs = wait_job(deployment, "oracle", ORACLE_TIMEOUT_SECONDS)
    if artifacts:
        (artifacts / "oracle.log").write_text(logs)
    sys.stderr.write(logs if len(logs) < 20000 else logs[:4000] + "\n...\n" + logs[-12000:])
    log("semantic oracle " + ("passed" if succeeded else "failed"))
    return 0 if succeeded else 1


def ensure_snapshot(deadline_seconds: float = SNAPSHOT_WAIT_SECONDS) -> str:
    """The node's data snapshot of the task's dataset, built if missing; its version."""
    import dataset  # noqa: PLC0415 - needs vibesys-evaluator, which the task image has

    try:
        ensured = snapshot.ensure(
            dataset.encode(dataset.generate()).decode(),
            registry=require_env("VIBESYS_K8S_REGISTRY"), builder=require_env("VIBESYS_K8S_BUILDER"),
            deadline_seconds=deadline_seconds, log=log,
        )
    except snapshot.SnapshotError as error:
        raise JobError(str(error)) from None
    return ensured.version


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


def seed_database(deployment: Deployment, image: str, digest: str, artifacts: Path | None) -> None:
    """Check the namespace's MongoDB servers hold the task's dataset (Job "seed")."""
    log("checking MongoDB's restored data: starting Job seed")
    started = time.monotonic()
    deployment.kube.kubectl("apply", "-f", "-", input_text=json.dumps(seed_job(image, digest)))
    succeeded, logs = wait_job(deployment, "seed", SEED_TIMEOUT_SECONDS + 60)
    if artifacts:
        (artifacts / "seed.log").write_text(logs)
    if not succeeded:
        raise JobError(f"Job seed failed; log tail:\n{logs[-1500:]}")
    log(f"MongoDB holds dataset {digest[:16]} (checked in {time.monotonic() - started:.0f}s):\n" + logs.strip()[-1500:])


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check").add_argument("--project-root", type=Path, required=True)
    sub.add_parser("data")
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
        if args.command == "data":
            log(f"data snapshot {ensure_snapshot(snapshot.JOB_DEADLINE_SECONDS)} is ready")
            return 0
        return do_check(args.project_root.resolve())
    except (JobError, envelope.EnvelopeError) as error:
        log(f"failed: {error}")
        return 1
    finally:
        # atexit handlers run next; block further signals so cleanup cannot be cut short.
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, signal.SIG_IGN)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
