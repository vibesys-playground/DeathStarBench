#!/usr/bin/env python3
"""Trusted wrapper for the hotel-k8s task: check and benchmark jobs.

    run.py check     --project-root ROOT
    run.py benchmark --project-root ROOT --vs-output PATH     (result protocol 2)
    run.py sweep                                              (delete stale namespaces only)
    run.py ctl       --namespace NS --port PORT start|stop    (used by the oracle)

Every job builds the candidate image from ROOT/hotelReservation and the task's
evaluator image (loadgen, seed) with buildctl, pushes them to the registry,
deploys the task-owned data tier into a fresh namespace of the cluster, seeds
MongoDB with the task's dataset (a Job), deploys the application services,
waits for readiness, runs the oracle or the load generator, and deletes the
namespace on every exit path (normal, error, SIGTERM, SIGINT, SIGHUP). Only
SIGKILL (which is what the CLI sends on cancel and timeout) leaks a namespace;
the next job sweeps stale ones (see sweep_stale_namespaces).

Environment (set by the vibesys-cli `kubernetes` resource, or by hand):
    KUBECONFIG              kubeconfig of the cluster (required)
    VIBESYS_K8S_REGISTRY    registry address as both the builder and the cluster's
                            nodes resolve it, for example localhost:5001 (required)
    VIBESYS_K8S_BUILDER     BuildKit address, for example tcp://127.0.0.1:1234 (required)
    VIBESYS_ARTIFACTS_DIR   optional; phase summaries and logs are copied there
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
import socket
import subprocess
import sys
import tempfile
import tomllib
import threading
import time
import urllib.error
import urllib.request
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
# Deployments the oracle's crash events restart: the stateless services and the
# reservation cache tier. MongoDB stays up, so its emptyDir keeps the data.
CRASH_DEPLOYMENTS = [
    "frontend", "geo", "profile", "rate", "recommendation", "reservation", "search", "user",
    "memcached-reserve",
]
PROBE_PATHS = [
    "/user?username=Cornell_30&password=0000000000",
    "/recommendations?require=rate&lat=37.7867&lon=-122.4112",
    "/hotels?inDate=2300-01-01&outDate=2300-01-02&lat=37.7867&lon=-122.4112",
]
# Load-generator pod: requests == limits. It is sized so the generator is never
# the bottleneck: 32 closed-loop workers or 512 open-loop workers.
LOADGEN_CPU = "6"
LOADGEN_MEMORY = "4Gi"
# Seed Job pod: requests == limits. It generates the dataset (about 0.5 s) and
# writes about 30 MB of documents into the six MongoDB servers concurrently.
SEED_CPU = "2"
SEED_MEMORY = "1Gi"
SEED_TIMEOUT_SECONDS = 600
# The application Deployments start only after the seed Job has filled MongoDB:
# each service seeds its own test data into an empty collection, and geo and
# recommendation load their collection once at startup.
APP_DEPLOYMENTS = ["frontend", "geo", "profile", "rate", "recommendation", "reservation", "search", "user"]


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


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


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
    """The in-cluster evaluator image: loadgen, seed and the workloads."""
    registry = require_env("VIBESYS_K8S_REGISTRY")
    context = scratch / "loadgen"
    (context / "workload").mkdir(parents=True)
    shutil.copy(tool("loadgen"), context / "loadgen")
    shutil.copy(tool("seed"), context / "seed")
    for name in ("closed.toml", "open.toml"):
        shutil.copy(WORKLOADS / name, context / "workload" / name)
    (context / "Dockerfile").write_text(
        f"FROM {LOADGEN_BASE}\n"
        "COPY loadgen seed /usr/local/bin/\n"
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
        self.port = 0
        self._forward_stop = threading.Event()
        self._forward_thread: threading.Thread | None = None
        self._forward_proc: subprocess.Popen | None = None

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
        self.kube.kubectl(
            "wait", "--for=condition=Available", "deployment", "-l", "backend=mongodb",
            "--timeout=300s", timeout=330,
        )
        seed_database(self, evaluator_image, artifacts)
        log("deploying the application services")
        self.kube.kubectl("apply", "-f", "-", input_text=services)
        self.kube.kubectl(
            "wait", "--for=condition=Available", "deployment", "--all", "--timeout=300s",
            timeout=330,
        )

    def delete(self) -> None:
        self._forward_stop.set()
        proc = self._forward_proc
        if proc is not None and proc.poll() is None:
            proc.terminate()
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

    # -- port forward ------------------------------------------------------
    def start_forward(self) -> None:
        """Keep a kubectl port-forward to the frontend alive on one free local port."""
        self.port = free_port()
        self._forward_thread = threading.Thread(target=self._forward_loop, daemon=True)
        self._forward_thread.start()

    def _forward_loop(self) -> None:
        while not self._forward_stop.is_set():
            proc = subprocess.Popen(
                ["kubectl", "-n", self.name, "port-forward", "--address", "127.0.0.1",
                 "service/frontend", f"{self.port}:5000"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
            )
            self._forward_proc = proc
            while proc.poll() is None and not self._forward_stop.wait(0.5):
                pass
            if proc.poll() is None:
                proc.terminate()
            self._forward_stop.wait(0.5)

    def wait_ready(self, timeout: float = 300.0) -> None:
        wait_frontend_ready(self.port, timeout)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


def wait_frontend_ready(port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    pending = list(PROBE_PATHS)
    last = ""
    while pending and time.monotonic() < deadline:
        path = pending[0]
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as response:
                if response.status == 200:
                    pending.pop(0)
                    continue
                last = f"{path}: status {response.status}"
        except (urllib.error.URLError, OSError) as error:
            last = f"{path}: {error}"
        time.sleep(1)
    if pending:
        raise JobError(f"frontend not ready after {timeout:.0f}s ({last})")


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


# ---- crash control used by the oracle (--run-command-json / --stop-command-json) ----

def ctl(namespace: str, port: int, action: str) -> int:
    kube = Kube(namespace)
    replicas = 0 if action == "stop" else 1
    for name in CRASH_DEPLOYMENTS:
        kube.kubectl("scale", f"deployment/{name}", f"--replicas={replicas}")
    if action == "stop":
        kube.kubectl(
            "wait", "--for=delete", "pod", "-l", "app in (" + ",".join(CRASH_DEPLOYMENTS) + ")",
            "--timeout=120s", check=False, timeout=150,
        )
        return 0
    kube.kubectl("wait", "--for=condition=Available", "deployment", "--all", "--timeout=300s",
                 timeout=330)
    wait_frontend_ready(port, 300.0)
    return 0


def command_json(namespace: str, port: int, action: str) -> str:
    return json.dumps([
        sys.executable, str(Path(__file__).resolve()), "ctl",
        "--namespace", namespace, "--port", str(port), action,
    ])


# ---- jobs ----

def do_check(project_root: Path, scratch: Path) -> int:
    deployment = Deployment(scratch)
    atexit.register(deployment.delete)
    sweep_stale_namespaces()
    artifacts = artifacts_dir()
    image = build_hotel_image(project_root)
    evaluator_image = build_loadgen_image(scratch)
    deployment.create(image, evaluator_image, artifacts)
    deployment.start_forward()
    deployment.wait_ready()
    log("running the semantic oracle")
    completed = subprocess.run(
        [
            tool("hotel-correctness"), "--mode", "accuracy",
            "--workload", str(WORKLOADS / "closed.toml"),
            "--base-url", deployment.base_url,
            "--seed", "random",
            "--candidate-dir", str(project_root),
            "--run-command-json", command_json(deployment.name, deployment.port, "start"),
            "--stop-command-json", command_json(deployment.name, deployment.port, "stop"),
            "--startup-timeout", "300",
        ],
        check=False,
    )
    return completed.returncode


LOADGEN_JOB = """\
apiVersion: batch/v1
kind: Job
metadata:
  name: {name}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  template:
    spec:
      restartPolicy: Never
      terminationGracePeriodSeconds: 5
      containers:
      - name: loadgen
        image: {image}
        imagePullPolicy: IfNotPresent
        resources:
          requests: {{cpu: "{cpu}", memory: {memory}}}
          limits: {{cpu: "{cpu}", memory: {memory}}}
        command: ["sh", "-c"]
        args:
        - |
          loadgen --mode benchmark --workload /workload/{workload}.toml \\
            --base-url {url} --startup-timeout 120{extra} \\
            --output-json /tmp/summary.json --vs-output /tmp/stream.jsonl
          rc=$?
          echo "::summary::"; cat /tmp/summary.json 2>/dev/null
          echo "::stream::"; cat /tmp/stream.jsonl 2>/dev/null
          exit $rc
"""


SEED_JOB = """\
apiVersion: batch/v1
kind: Job
metadata:
  name: seed
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  template:
    spec:
      restartPolicy: Never
      terminationGracePeriodSeconds: 5
      containers:
      - name: seed
        image: {image}
        imagePullPolicy: IfNotPresent
        resources:
          requests: {{cpu: "{cpu}", memory: {memory}}}
          limits: {{cpu: "{cpu}", memory: {memory}}}
        command: ["seed", "--timeout", "{timeout}s"]
"""


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
    deployment.kube.kubectl("apply", "-f", "-", input_text=SEED_JOB.format(
        image=image, cpu=SEED_CPU, memory=SEED_MEMORY, timeout=SEED_TIMEOUT_SECONDS,
    ))
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
              rate: float | None = None) -> dict:
    """Run one load-generator Job in the namespace; return its protocol-2 result values.

    rate, when given, overrides the workload's offered rate (operations/s).
    """
    name = f"loadgen-{workload}"
    extra = f" --rate {rate:.2f}" if rate is not None else ""
    log(f"phase {workload}: starting Job {name}" + (f" at {rate:.2f} ops/s" if rate is not None else ""))
    manifest = LOADGEN_JOB.format(
        name=name, image=image, cpu=LOADGEN_CPU, memory=LOADGEN_MEMORY, workload=workload,
        url=FRONTEND_URL, extra=extra,
    )
    deployment.kube.kubectl("apply", "-f", "-", input_text=manifest)
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
        hotel_image = build_hotel_image(project_root)
        loadgen_image = build_loadgen_image(scratch)
        deployment.create(hotel_image, loadgen_image, artifacts)
        deployment.start_forward()
        deployment.wait_ready()
        closed = run_phase(deployment, loadgen_image, "closed", artifacts)
        throughput = closed["operations_per_second"]
        # p99 is measured at a fixed fraction of this sample's own closed-loop
        # throughput, so a faster candidate is held to proportionally more load.
        offered = OPEN_LOOP_LOAD_FRACTION * throughput
        open_loop = run_phase(deployment, loadgen_image, "open", artifacts, rate=offered)
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
    p = sub.add_parser("ctl")
    p.add_argument("--namespace", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("action", choices=("start", "stop"))
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
        if args.command == "ctl":
            return ctl(args.namespace, args.port, args.action)
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
