#!/usr/bin/env python3
"""Trusted wrapper for the hotel-k8s task: check and benchmark jobs.

    run.py check     --project-root ROOT
    run.py benchmark --project-root ROOT --vs-output PATH     (result protocol 2)
    run.py ctl       --namespace NS --port PORT start|stop    (used by the oracle)

Every job builds the candidate image from ROOT/hotelReservation with buildctl,
pushes it to the registry, deploys the task-owned manifests into a fresh
namespace of the cluster, waits for readiness, runs the oracle or the load
generator, and deletes the namespace on every exit path (normal, error, SIGTERM,
SIGINT, SIGHUP). Only SIGKILL leaks a namespace; the next job sweeps stale ones.

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
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

TASK_DIR = Path(__file__).resolve().parent
MANIFEST = TASK_DIR / "k8s" / "manifest.yaml"
WORKLOADS = TASK_DIR / "workload"
NAMESPACE_LABEL = "vibesys.dev/hotel-k8s"
STALE_NAMESPACE_SECONDS = 6 * 3600

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
    registry = require_env("VIBESYS_K8S_REGISTRY")
    context = scratch / "loadgen"
    (context / "workload").mkdir(parents=True)
    shutil.copy(tool("servicebench"), context / "servicebench")
    for name in ("closed.toml", "open.toml"):
        shutil.copy(WORKLOADS / name, context / "workload" / name)
    (context / "Dockerfile").write_text(
        f"FROM {LOADGEN_BASE}\n"
        "COPY servicebench /usr/local/bin/servicebench\n"
        "COPY workload/ /workload/\n"
    )
    image = f"{registry}/hotel-k8s/loadgen:{tree_digest(context)}"
    log(f"building load-generator image {image}")
    buildctl_build(context, image)
    return image


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
    def create(self, hotel_image: str) -> None:
        self.created = True  # even a half-created namespace must be deleted
        self.kube.kubectl("create", "namespace", self.name, namespaced=False)
        self.kube.kubectl(
            "label", "namespace", self.name, f"{NAMESPACE_LABEL}=1", namespaced=False
        )
        manifest = MANIFEST.read_text()
        manifest = manifest.replace("${NAMESPACE}", self.name).replace("${HOTEL_IMAGE}", hotel_image)
        log(f"deploying into namespace {self.name}")
        self.kube.kubectl("apply", "-f", "-", input_text=manifest)
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


def sweep_stale_namespaces() -> None:
    """Delete this task's namespaces that a killed job left behind."""
    result = Kube().kubectl(
        "get", "namespace", "-l", NAMESPACE_LABEL, "-o", "json", namespaced=False, check=False
    )
    if result.returncode != 0:
        return
    now = time.time()
    for item in json.loads(result.stdout).get("items", []):
        created = item["metadata"]["creationTimestamp"]
        age = now - calendar.timegm(time.strptime(created, "%Y-%m-%dT%H:%M:%SZ"))
        if age > STALE_NAMESPACE_SECONDS:
            name = item["metadata"]["name"]
            log(f"sweeping stale namespace {name}")
            Kube().kubectl("delete", "namespace", name, "--wait=false", namespaced=False, check=False)


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
    image = build_hotel_image(project_root)
    deployment.create(image)
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
          servicebench --mode benchmark --workload /workload/{workload}.toml \\
            --base-url {url} --seed 48 --fixture-seed 48 --startup-timeout 120 \\
            --output-json /tmp/summary.json --vs-output /tmp/stream.jsonl
          rc=$?
          echo "::summary::"; cat /tmp/summary.json 2>/dev/null
          echo "::stream::"; cat /tmp/stream.jsonl 2>/dev/null
          exit $rc
"""


def run_phase(deployment: Deployment, image: str, workload: str, artifacts: Path | None) -> dict:
    """Run one servicebench Job in the namespace; return its protocol-2 result values."""
    name = f"loadgen-{workload}"
    log(f"phase {workload}: starting Job {name}")
    manifest = LOADGEN_JOB.format(
        name=name, image=image, cpu=LOADGEN_CPU, memory=LOADGEN_MEMORY, workload=workload,
        url=FRONTEND_URL,
    )
    deployment.kube.kubectl("apply", "-f", "-", input_text=manifest)
    deadline = time.monotonic() + 1500
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
    if artifacts:
        (artifacts / f"{name}.log").write_text(logs)
    summary_text = stream_text = ""
    if "::summary::" in logs and "::stream::" in logs:
        head, _, stream_text = logs.partition("::stream::")
        summary_text = head.partition("::summary::")[2]
        log(f"phase {workload} servicebench output:\n" + head.partition("::summary::")[0][-1500:])
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
    artifacts = Path(os.environ["VIBESYS_ARTIFACTS_DIR"]) if os.environ.get("VIBESYS_ARTIFACTS_DIR") else None
    try:
        deployment = Deployment(scratch)
        atexit.register(deployment.delete)
        sweep_stale_namespaces()
        hotel_image = build_hotel_image(project_root)
        loadgen_image = build_loadgen_image(scratch)
        deployment.create(hotel_image)
        deployment.start_forward()
        deployment.wait_ready()
        closed = run_phase(deployment, loadgen_image, "closed", artifacts)
        open_loop = run_phase(deployment, loadgen_image, "open", artifacts)
        throughput = closed["operations_per_second"]
        p99 = open_loop["latency_ms.p99"]
        log(f"closed loop: {throughput:.1f} ops/s; open loop p99: {p99:.2f} ms")
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
