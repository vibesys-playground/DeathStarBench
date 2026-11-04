#!/usr/bin/env python3
"""Trusted wrapper for the social-network-k8s task: check and benchmark jobs.

    run.py check     --project-root ROOT
    run.py benchmark --project-root ROOT --vs-output PATH     (result protocol 2)
    run.py sweep                                              (delete stale namespaces only)
    run.py ctl       --namespace NS --port PORT restart DEPLOYMENT | flush CACHE
                                                              (the checker's restart hook)

Every job builds the candidate image from ROOT/socialNetwork (the C++ services and
nginx-thrift with its Lua scripts) and the task's evaluator image (loadgen, seed) with
buildctl, pushes them to the registry, deploys the task-owned data tier into a fresh
namespace of the cluster, then the application (two replicas of every service), waits
for readiness, seeds the task's dataset (a Job), runs the checker or the load
generator, and deletes the namespace on every exit path (normal, error, SIGTERM,
SIGINT, SIGHUP). Only SIGKILL (which is what the CLI sends on cancel and timeout)
leaks a namespace; the next job sweeps stale ones (see sweep_stale_namespaces).

Environment (set by the vibesys-cli `kubernetes` resource, or by hand):
    KUBECONFIG              kubeconfig of the cluster (required)
    VIBESYS_K8S_REGISTRY    registry address as both the builder and the cluster's
                            nodes resolve it, for example localhost:5001 (required)
    VIBESYS_K8S_BUILDER     BuildKit address, for example tcp://127.0.0.1:1234 (required)
    VIBESYS_ARTIFACTS_DIR   optional; logs, summaries, seeds.json, offered-rate.json
"""

from __future__ import annotations

import argparse
import atexit
import base64
import calendar
import hashlib
import itertools
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
import tomllib
import urllib.error
import urllib.request
from pathlib import Path

TASK = "social-network-k8s"
TASK_DIR = Path(__file__).resolve().parent
MANIFEST = TASK_DIR / "k8s" / "manifest.yaml"
WORKLOADS = TASK_DIR / "workload"
# Namespaces are hk8s-sn-<hex>. The label keys are this task's own: hotel-k8s sweeps
# namespaces labelled vibesys.dev/hotel-k8s, this task only those labelled with its key.
NAMESPACE_PREFIX = "hk8s-sn-"
NAMESPACE_LABEL = "vibesys.dev/social-network-k8s"
CREATED_LABEL = "vibesys.dev/social-network-k8s-created"  # epoch seconds
JOB_LABEL = "vibesys.dev/social-network-k8s-job"
SWEEP_MARGIN_SECONDS = 600

LOADGEN_BASE = "busybox:1.36@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
FRONTEND_SERVICE = "nginx-thrift"
FRONTEND_PORT = 8080
FRONTEND_URL = f"http://{FRONTEND_SERVICE}:{FRONTEND_PORT}"
# Probes that must answer 200 through nginx-thrift before the deployment is ready.
# They reach nginx, user-timeline (MongoDB, Redis), home-timeline (Redis), post-storage,
# for a user id far from the dataset's (so the probes leave its caches alone).
PROBE_USER = 1999999999
PROBE_PATHS = [
    "/",
    f"/wrk2-api/user-timeline/read?user_id={PROBE_USER}&start=0&stop=1",
    f"/wrk2-api/home-timeline/read?user_id={PROBE_USER}&start=0&stop=1",
]
# Stateless Deployments the checker may restart one pod of, and the cache tiers it may
# flush. home-timeline-redis is not a cache: it is the only store of home timelines.
STATELESS = [
    "compose-post-service", "home-timeline-service", "media-service", "nginx-thrift",
    "post-storage-service", "social-graph-service", "text-service", "unique-id-service",
    "url-shorten-service", "user-mention-service", "user-service", "user-timeline-service",
]
MEMCACHED = ["post-storage-memcached", "url-shorten-memcached", "user-memcached"]
CACHE_REDIS = ["social-graph-redis", "user-timeline-redis"]
# Load-generator pod: requests == limits, sized so the generator is never the bottleneck.
LOADGEN_CPU = "6"
LOADGEN_MEMORY = "4Gi"
# Seed Job pod: requests == limits.
SEED_CPU = "4"
SEED_MEMORY = "2Gi"
SEED_TIMEOUT_SECONDS = 1200
READY_TIMEOUT_SECONDS = 300.0
# The open-loop phase offers this fraction of the sample's closed-loop throughput.
OPEN_LOOP_LOAD_FRACTION = 0.7
# The open phase replays the closed phase's writes (--state-out / --state-in), so it
# can validate timelines exactly on the same deployment.
STATE_JOURNAL = True


class JobError(Exception):
    """A failure that ends the job with a message."""


def log(message: str) -> None:
    print(f"[{TASK} {time.strftime('%H:%M:%S')}] {message}", file=sys.stderr, flush=True)


def tool(name: str) -> str:
    """Prebuilt binary: in the job image they are on PATH under /opt/vibesys/bin."""
    found = shutil.which(name)
    if not found:
        raise JobError(f"{name} not found on PATH (it is built into the task's Docker image)")
    return found


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
        tail = (result.stdout or "")[-3000:]
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
            digest.update(b"x" if os.access(path, os.X_OK) else b"-")
    return digest.hexdigest()[:16]


def buildctl_build(context: Path, image: str, timeout: float = 2400) -> float:
    """Build and push one image; return the seconds it took.

    The builder keeps its layer cache and the Dockerfile's cache mounts (the C++
    stage's ccache) between jobs, so an unchanged stage is not rebuilt.
    """
    builder = require_env("VIBESYS_K8S_BUILDER")
    started = time.monotonic()
    run(
        [
            "buildctl", "--addr", builder, "build",
            "--frontend", "dockerfile.v0",
            "--local", f"context={context}",
            "--local", f"dockerfile={context}",
            "--output", f"type=image,name={image},push=true,registry.insecure=true",
        ],
        timeout=timeout,
    )
    return time.monotonic() - started


def build_candidate_image(project_root: Path, artifacts: Path | None) -> str:
    source = project_root / "socialNetwork"
    if not (source / "Dockerfile").is_file():
        raise JobError(f"{source}/Dockerfile not found")
    registry = require_env("VIBESYS_K8S_REGISTRY")
    image = f"{registry}/{TASK}/social-network:{tree_digest(source)}"
    log(f"building candidate image {image}")
    seconds = buildctl_build(source, image)
    log(f"candidate image built in {seconds:.0f}s")
    if artifacts:
        (artifacts / "build.json").write_text(json.dumps(
            {"image": image, "build_seconds": round(seconds, 1)}) + "\n")
    return image


def build_evaluator_image(scratch: Path) -> str:
    """The in-cluster evaluator image: loadgen, seed (when built) and the workloads."""
    registry = require_env("VIBESYS_K8S_REGISTRY")
    context = scratch / "evaluator"
    (context / "workload").mkdir(parents=True)
    binaries = ["loadgen", "seed"]
    for name in binaries:
        shutil.copy(tool(name), context / name)
    for name in ("closed.toml", "open.toml"):
        shutil.copy(WORKLOADS / name, context / "workload" / name)
    (context / "Dockerfile").write_text(
        f"FROM {LOADGEN_BASE}\n"
        f"COPY {' '.join(binaries)} /usr/local/bin/\n"
        "COPY workload/ /workload/\n"
    )
    image = f"{registry}/{TASK}/evaluator:{tree_digest(context)}"
    log(f"building evaluator image {image}")
    buildctl_build(context, image, timeout=600)
    return image


def split_manifest(manifest: str) -> tuple[str, str]:
    """Split the rendered manifest into (config, Services and data tier; application Deployments)."""
    data, app = [], []
    for document in manifest.split("\n---\n"):
        is_app = "\nkind: Deployment\n" in f"\n{document}\n" and "\n    tier: app\n" in document
        (app if is_app else data).append(document)
    if len(app) != len(STATELESS):
        raise JobError(f"manifest has {len(app)} application Deployments, expected {len(STATELESS)}")
    return "\n---\n".join(data), "\n---\n".join(app)


class Forwarder:
    """A local TCP endpoint that spreads connections over every nginx-thrift pod.

    One supervised `kubectl port-forward` per pod slot (slot i forwards to the i-th
    ready pod by name, re-chosen whenever its forward exits, for example after a pod
    restart), and a listener that hands each accepted connection to the slots in
    turn. A client that opens several connections reaches both replicas.
    """

    def __init__(self, namespace: str, slots: int):
        self.namespace = namespace
        self.port = free_port()
        self._stop = threading.Event()
        self._slot_ports = [free_port() for _ in range(slots)]
        self._procs: list[subprocess.Popen | None] = [None] * slots
        self._turn = itertools.count()
        self._listener: socket.socket | None = None

    def start(self) -> None:
        for slot in range(len(self._slot_ports)):
            threading.Thread(target=self._forward_loop, args=(slot,), daemon=True).start()
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", self.port))
        listener.listen(256)
        self._listener = listener
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
        for proc in self._procs:
            if proc is not None and proc.poll() is None:
                proc.terminate()
        if self._listener is not None:
            try:
                self._listener.close()
            except OSError:
                pass

    def _pod(self, slot: int) -> str | None:
        result = Kube(self.namespace).kubectl(
            "get", "pods", "-l", f"app={FRONTEND_SERVICE}", "-o", "json", check=False, timeout=30)
        if result.returncode != 0:
            return None
        ready = []
        for item in json.loads(result.stdout).get("items", []):
            if item["metadata"].get("deletionTimestamp"):
                continue
            conditions = item.get("status", {}).get("conditions", [])
            if any(c["type"] == "Ready" and c["status"] == "True" for c in conditions):
                ready.append(item["metadata"]["name"])
        ready.sort()
        return ready[slot % len(ready)] if ready else None

    def _forward_loop(self, slot: int) -> None:
        while not self._stop.is_set():
            try:
                pod = self._pod(slot)
            except (subprocess.TimeoutExpired, json.JSONDecodeError):
                pod = None
            if pod is None:
                self._stop.wait(1)
                continue
            proc = subprocess.Popen(
                ["kubectl", "-n", self.namespace, "port-forward", "--address", "127.0.0.1",
                 f"pod/{pod}", f"{self._slot_ports[slot]}:{FRONTEND_PORT}"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
            )
            self._procs[slot] = proc
            while proc.poll() is None and not self._stop.wait(0.5):
                pass
            if proc.poll() is None:
                proc.terminate()
            self._stop.wait(0.5)

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(client,), daemon=True).start()

    def _serve(self, client: socket.socket) -> None:
        upstream = None
        start = next(self._turn)
        for attempt in range(len(self._slot_ports) * 2):
            port = self._slot_ports[(start + attempt) % len(self._slot_ports)]
            try:
                upstream = socket.create_connection(("127.0.0.1", port), timeout=5)
                upstream.settimeout(None)
                break
            except OSError:
                time.sleep(0.2)
        if upstream is None:
            client.close()
            return

        def pump(src: socket.socket, dst: socket.socket) -> None:
            try:
                while True:
                    chunk = src.recv(65536)
                    if not chunk:
                        break
                    dst.sendall(chunk)
            except OSError:
                pass
            finally:
                for sock in (src, dst):
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass

        other = threading.Thread(target=pump, args=(upstream, client), daemon=True)
        other.start()
        pump(client, upstream)
        other.join()
        client.close()
        upstream.close()


class Deployment:
    """A fresh namespace with the Social Network application deployed in it."""

    def __init__(self, scratch: Path):
        self.scratch = scratch
        self.name = f"{NAMESPACE_PREFIX}{secrets.token_hex(4)}"
        self.kube = Kube(self.name)
        self.created = False
        self.forwarder: Forwarder | None = None

    def create(self, image: str, evaluator_image: str, artifacts: Path | None) -> None:
        self.created = True  # even a half-created namespace must be deleted
        self.kube.kubectl("create", "namespace", self.name, namespaced=False)
        self.kube.kubectl(
            "label", "namespace", self.name, f"{NAMESPACE_LABEL}=1",
            f"{CREATED_LABEL}={int(time.time())}", f"{JOB_LABEL}={job_label()}",
            namespaced=False,
        )
        manifest = MANIFEST.read_text()
        manifest = manifest.replace("${NAMESPACE}", self.name).replace("${SOCIAL_NETWORK_IMAGE}", image)
        data_tier, services = split_manifest(manifest)
        started = time.monotonic()
        log(f"deploying the data tier into namespace {self.name}")
        self.kube.kubectl("apply", "-f", "-", input_text=data_tier)
        self.kube.kubectl(
            "wait", "--for=condition=Available", "deployment", "-l", "tier=data",
            "--timeout=300s", timeout=330,
        )
        log(f"data tier available after {time.monotonic() - started:.0f}s")
        # The dataset is written into the data tier before any request reaches the
        # application (the services read what they find).
        seed_dataset(self, evaluator_image, artifacts)
        log("deploying the application")
        self.kube.kubectl("apply", "-f", "-", input_text=services)
        result = self.kube.kubectl(
            "wait", "--for=condition=Available", "deployment", "--all", "--timeout=300s",
            timeout=330, check=False,
        )
        if result.returncode != 0:
            raise JobError("application not available after 300s:\n" + self.describe_unready())
        log(f"all Deployments available after {time.monotonic() - started:.0f}s")

    def describe_unready(self) -> str:
        pods = self.kube.kubectl("get", "pods", "-o", "wide", check=False).stdout
        lines = [pods]
        for line in pods.splitlines()[1:]:
            fields = line.split()
            ready, _, total = fields[1].partition("/") if len(fields) >= 3 else ("", "", "")
            if len(fields) >= 3 and (fields[2] != "Running" or ready != total):
                logs = self.kube.kubectl("logs", fields[0], "--tail=20", check=False).stdout
                previous = self.kube.kubectl("logs", fields[0], "--previous", "--tail=20", check=False).stdout
                lines.append(f"--- {fields[0]}\n{logs[-1500:]}\n--- previous\n{previous[-1500:]}")
        return "\n".join(lines)[-12000:]

    def delete(self) -> None:
        if self.forwarder is not None:
            self.forwarder.stop()
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

    def start_forward(self) -> None:
        self.forwarder = Forwarder(self.name, slots=2)
        self.forwarder.start()

    @property
    def port(self) -> int:
        return self.forwarder.port

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def wait_ready(self, timeout: float = READY_TIMEOUT_SECONDS) -> None:
        wait_frontend_ready(self.port, timeout)

    def save_pods(self, artifacts: Path | None, name: str) -> None:
        """Record the pods (restarts, nodes) of the namespace."""
        result = self.kube.kubectl("get", "pods", "-o", "wide", check=False)
        restarts = []
        for line in result.stdout.splitlines()[1:]:
            fields = line.split()
            if len(fields) >= 4 and fields[3].isdigit() and int(fields[3]) > 0:
                restarts.append(f"{fields[0]} ({fields[3]})")
        if restarts:
            log(f"pods restarted: {', '.join(restarts)}")
        if artifacts:
            (artifacts / name).write_text(result.stdout)
            self.save_logs(artifacts / "logs")

    def save_logs(self, directory: Path, tail: int = 300) -> None:
        """The last lines of every application and data-tier container (and of its previous run)."""
        directory.mkdir(parents=True, exist_ok=True)
        result = self.kube.kubectl("get", "pods", "-l", "tier", "-o", "json", check=False)
        if result.returncode != 0:
            return
        for item in json.loads(result.stdout).get("items", []):
            pod = item["metadata"]["name"]
            text = self.kube.kubectl("logs", pod, f"--tail={tail}", check=False, timeout=60).stdout
            statuses = item.get("status", {}).get("containerStatuses", [])
            if any(status.get("restartCount", 0) > 0 for status in statuses):
                previous = self.kube.kubectl("logs", pod, "--previous", f"--tail={tail}",
                                             check=False, timeout=60).stdout
                text += "\n--- previous container ---\n" + previous
            (directory / f"{pod}.log").write_text(text)


def wait_frontend_ready(port: int, timeout: float, rounds: int = 2) -> None:
    """Every probe must answer 200, `rounds` times over fresh connections (both replicas)."""
    deadline = time.monotonic() + timeout
    pending = list(PROBE_PATHS) * rounds * 2
    last = ""
    while pending and time.monotonic() < deadline:
        path = pending[0]
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as response:
                if response.status == 200:
                    pending.pop(0)
                    continue
                last = f"{path}: status {response.status}"
        except (urllib.error.URLError, OSError) as error:
            last = f"{path}: {error}"
        time.sleep(1)
    if pending:
        raise JobError(f"nginx-thrift not ready after {timeout:.0f}s ({last})")


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
        "get", "namespace", "-l", f"{NAMESPACE_LABEL}=1", "-o", "json", namespaced=False, check=False
    )
    if result.returncode != 0:
        return []
    swept = []
    for item in json.loads(result.stdout).get("items", []):
        meta = item["metadata"]
        name = meta["name"]
        if not name.startswith(NAMESPACE_PREFIX):
            continue
        created = meta.get("labels", {}).get(CREATED_LABEL)
        if created is not None and created.isdigit():
            age = now - int(created)
        else:
            age = now - calendar.timegm(
                time.strptime(meta["creationTimestamp"], "%Y-%m-%dT%H:%M:%SZ")
            )
        if age > ttl:
            log(f"sweeping stale namespace {name} (job {meta['labels'].get(JOB_LABEL, '?')}, "
                f"{age:.0f}s old, limit {ttl:.0f}s)")
            Kube().kubectl("delete", "namespace", name, "--wait=false", namespaced=False, check=False)
            swept.append(name)
    return swept


# ---- the checker's restart hook ----

def ctl(namespace: str, port: int, action: str, target: str) -> int:
    """restart DEPLOYMENT: delete one pod of a stateless Deployment and wait until it is
    replaced and the API answers again. flush CACHE: empty a cache tier in place."""
    kube = Kube(namespace)
    if action == "restart":
        if target not in STATELESS + MEMCACHED + CACHE_REDIS:
            raise JobError(f"ctl restart: {target} is not a stateless Deployment or a cache")
        pods = json.loads(kube.kubectl("get", "pods", "-l", f"app={target}", "-o", "json").stdout)
        names = sorted(item["metadata"]["name"] for item in pods.get("items", [])
                       if not item["metadata"].get("deletionTimestamp"))
        if not names:
            raise JobError(f"ctl restart: no pod of {target}")
        victim = secrets.choice(names)
        log(f"ctl: restarting pod {victim} of {target}")
        kube.kubectl("delete", "pod", victim, "--wait=true", "--timeout=120s", timeout=150)
        kube.kubectl("rollout", "status", f"deployment/{target}", "--timeout=300s", timeout=330)
        kube.kubectl("wait", "--for=condition=Available", "deployment", "--all",
                     "--timeout=300s", timeout=330)
    elif action == "flush":
        if target in MEMCACHED:
            script = ("exec 3<>/dev/tcp/127.0.0.1/11211; printf 'flush_all\\r\\n' >&3; "
                      "read -r reply <&3; echo \"$reply\"; case \"$reply\" in OK*) exit 0;; esac; exit 1")
            kube.kubectl("exec", f"deployment/{target}", "--", "bash", "-c", script, timeout=60)
        elif target in CACHE_REDIS:
            out = kube.kubectl("exec", f"deployment/{target}", "--", "redis-cli", "FLUSHALL",
                               timeout=60).stdout
            if "OK" not in out:
                raise JobError(f"ctl flush {target}: {out.strip()}")
        else:
            raise JobError(f"ctl flush: {target} is not a cache tier "
                           f"({', '.join(MEMCACHED + CACHE_REDIS)})")
        log(f"ctl: flushed {target}")
    else:
        raise JobError(f"ctl: unknown action {action}")
    wait_frontend_ready(port, READY_TIMEOUT_SECONDS)
    return 0


def restart_command_json(namespace: str, port: int) -> str:
    """The checker appends `restart DEPLOYMENT` or `flush CACHE`."""
    return json.dumps([
        sys.executable, str(Path(__file__).resolve()), "ctl",
        "--namespace", namespace, "--port", str(port),
    ])


# ---- Jobs inside the namespace ----

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
      enableServiceLinks: false
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
          {prelude}loadgen --mode benchmark --workload /workload/{workload}.toml \\
            --base-url {url} --startup-timeout 120 --seed {seed} --fixture-seed {fixture_seed}{extra} \\
            --output-json /tmp/summary.json --vs-output /tmp/stream.jsonl
          rc=$?
          echo "::summary::"; cat /tmp/summary.json 2>/dev/null
          echo "::stream::"; cat /tmp/stream.jsonl 2>/dev/null
          echo "::state::"; [ -f /tmp/state.out ] && base64 /tmp/state.out
          echo "::end::"
          exit $rc{mounts}
"""

# The closed phase's write journal (loadgen --state-out) reaches the open phase's Job
# (--state-in) as ConfigMaps of at most this many bytes each (the API limit is 1 MiB).
STATE_CHUNK_BYTES = 700 * 1024
STATE_MOUNTS = """
        volumeMounts:
        - name: state
          mountPath: /state
          readOnly: true
      volumes:
      - name: state
        projected:
          sources:
{sources}"""

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
      enableServiceLinks: false
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
            logs = deployment.kube.kubectl("logs", f"job/{name}", "--tail=50", check=False).stdout
            raise JobError(f"Job {name} did not finish in {timeout:.0f}s; log tail:\n{logs[-1500:]}")
        time.sleep(3)
    logs = deployment.kube.kubectl("logs", f"job/{name}", check=False).stdout
    return bool(status.get("succeeded")), logs


def seed_dataset(deployment: Deployment, image: str, artifacts: Path | None) -> dict | None:
    """Write the task's dataset directly into the data tier (Job "seed")."""
    log("seeding the task's dataset: starting Job seed")
    started = time.monotonic()
    deployment.kube.kubectl("apply", "-f", "-", input_text=SEED_JOB.format(
        image=image, cpu=SEED_CPU, memory=SEED_MEMORY, timeout=SEED_TIMEOUT_SECONDS,
    ))
    succeeded, logs = wait_job(deployment, "seed", SEED_TIMEOUT_SECONDS + 60)
    seconds = time.monotonic() - started
    if artifacts:
        (artifacts / "seed.log").write_text(logs)
    if not succeeded:
        raise JobError(f"Job seed failed after {seconds:.0f}s; log tail:\n{logs[-1500:]}")
    summary = None
    for line in logs.splitlines():
        if line.startswith("{"):
            try:
                summary = json.loads(line)
            except json.JSONDecodeError:
                pass
    if summary is None:
        raise JobError(f"Job seed wrote no summary; log tail:\n{logs[-1500:]}")
    summary["job_seconds"] = round(seconds, 1)
    if artifacts:
        (artifacts / "seed.summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    log(f"seeded the dataset in {seconds:.0f}s: {json.dumps(summary)[:400]}")
    create_indexes(deployment)
    return summary


# Indexes on the keys the services look documents up by, as a production database
# would have (social-graph user_id, post post_id and url-shorten shortened_url are
# created by the seed and by the services themselves).
INDEXES = [
    ("user-mongodb", "user", "user", "{username: 1}", "{unique: true}"),
    ("user-mongodb", "user", "user", "{user_id: 1}", "{unique: true}"),
    ("user-timeline-mongodb", "user-timeline", "user-timeline", "{user_id: 1}", "{unique: true}"),
]


def create_indexes(deployment: Deployment) -> None:
    for target, database, collection, keys, options in INDEXES:
        script = (f"db.getSiblingDB('{database}').getCollection('{collection}')"
                  f".createIndex({keys}, {options})")
        deployment.kube.kubectl("exec", f"deployment/{target}", "--", "mongo", "--quiet",
                                "--eval", script, timeout=120)
    log(f"created {len(INDEXES)} indexes")


def draw_seeds() -> dict:
    """Fresh load seeds for one phase of one sample (recorded so it can be replayed)."""
    return {"seed": secrets.randbelow(2**31), "fixture_seed": secrets.randbelow(2**31)}


def put_state(deployment: Deployment, state: bytes) -> str:
    """Store a write journal as ConfigMaps; return the Job's projected-volume sources."""
    sources = []
    for index in range(0, max(len(state), 1), STATE_CHUNK_BYTES):
        part = f"loadgen-state-{index // STATE_CHUNK_BYTES:02d}"
        path = scratch_file(deployment, part)
        path.write_bytes(state[index:index + STATE_CHUNK_BYTES])
        deployment.kube.kubectl("create", "configmap", part, f"--from-file=part={path}")
        sources.append(f"          - configMap:\n              name: {part}\n"
                       f"              items: [{{key: part, path: {part}}}]")
    return "\n".join(sources)


def scratch_file(deployment: Deployment, name: str) -> Path:
    return deployment.scratch / name


def run_phase(deployment: Deployment, image: str, workload: str, artifacts: Path | None,
              seeds: dict, rate: float | None = None, state_in: bytes | None = None,
              state_out: bool = False) -> tuple[dict, bytes | None]:
    """Run one load-generator Job in the namespace.

    Returns its protocol-2 result values and, with state_out, the write journal the
    load generator left (to replay in a later phase with state_in). rate, when given,
    overrides the workload's offered rate (operations/s).
    """
    name = f"loadgen-{workload}"
    extra = f" --rate {rate:.2f}" if rate is not None else ""
    prelude = mounts = ""
    if state_out:
        extra += " --state-out /tmp/state.out"
    if state_in is not None:
        mounts = STATE_MOUNTS.format(sources=put_state(deployment, state_in))
        prelude = "cat /state/loadgen-state-* > /tmp/state.in || exit 1\n          "
        extra += " --state-in /tmp/state.in"
    log(f"phase {workload}: starting Job {name} (seed {seeds['seed']}, fixture seed "
        f"{seeds['fixture_seed']})" + (f" at {rate:.2f} ops/s" if rate is not None else ""))
    manifest = LOADGEN_JOB.format(
        name=name, image=image, cpu=LOADGEN_CPU, memory=LOADGEN_MEMORY, workload=workload,
        url=FRONTEND_URL, extra=extra, seed=seeds["seed"], fixture_seed=seeds["fixture_seed"],
        prelude=prelude, mounts=mounts,
    )
    deployment.kube.kubectl("apply", "-f", "-", input_text=manifest)
    _, logs = wait_job(deployment, name, 1500)
    if "::summary::" in logs and "::stream::" in logs and "::state::" in logs:
        head, _, rest = logs.partition("::stream::")
        stream_text, _, state_text = rest.partition("::state::")
        state_text = state_text.partition("::end::")[0]
        summary_text = head.partition("::summary::")[2]
        log(f"phase {workload} loadgen output:\n" + head.partition("::summary::")[0][-1500:])
    else:
        if artifacts:
            (artifacts / f"{name}.log").write_text(logs)
        raise JobError(f"Job {name} produced no result stream; log tail:\n{logs[-1500:]}")
    state = None
    if state_out:
        encoded = "".join(state_text.split())
        if not encoded:
            raise JobError(f"phase {workload} left no write journal (--state-out)")
        state = base64.b64decode(encoded)
    if artifacts:
        (artifacts / f"{name}.log").write_text(logs.partition("::summary::")[0])
        (artifacts / f"{name}.summary.json").write_text(summary_text)
        (artifacts / f"{name}.stream.jsonl").write_text(stream_text)
        if state is not None:
            (artifacts / f"{name}.state.gz").write_bytes(state)
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
    return values, state


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


def prepare(project_root: Path, scratch: Path, artifacts: Path | None) -> tuple[Deployment, str]:
    """Build both images, deploy, wait for readiness and seed; return (deployment, evaluator image)."""
    deployment = Deployment(scratch)
    atexit.register(deployment.delete)
    sweep_stale_namespaces()
    image = build_candidate_image(project_root, artifacts)
    evaluator_image = build_evaluator_image(scratch)
    try:
        deployment.create(image, evaluator_image, artifacts)
        deployment.start_forward()
        deployment.wait_ready()
    except JobError:
        deployment.save_pods(artifacts, "pods.txt")
        raise
    return deployment, evaluator_image


def do_check(project_root: Path, scratch: Path) -> int:
    artifacts = artifacts_dir()
    deployment, _ = prepare(project_root, scratch, artifacts)
    log("running the checker")
    started = time.monotonic()
    completed = subprocess.run(
        [
            tool("checker"),
            "--base-url", deployment.base_url,
            "--seed", "random",
            "--restart-command-json", restart_command_json(deployment.name, deployment.port),
            "--deployments", ",".join(STATELESS),
            "--caches", ",".join(MEMCACHED + CACHE_REDIS),
        ],
        check=False,
    )
    log(f"checker exited {completed.returncode} after {time.monotonic() - started:.0f}s")
    deployment.save_pods(artifacts, "pods.txt")
    return completed.returncode


def do_benchmark(project_root: Path, scratch: Path, output: Path) -> int:
    write_stream(output, [HELLO])
    artifacts = artifacts_dir()
    deployment = None
    try:
        deployment, evaluator_image = prepare(project_root, scratch, artifacts)
        seeds = {"closed": draw_seeds(), "open": draw_seeds()}
        if artifacts:
            (artifacts / "seeds.json").write_text(json.dumps(seeds, indent=2) + "\n")
        closed, journal = run_phase(deployment, evaluator_image, "closed", artifacts,
                                    seeds["closed"], state_out=STATE_JOURNAL)
        throughput = closed["operations_per_second"]
        # p99 is measured at a fixed fraction of this sample's own closed-loop
        # throughput, so a faster candidate is held to proportionally more load.
        offered = OPEN_LOOP_LOAD_FRACTION * throughput
        if artifacts:
            (artifacts / "offered-rate.json").write_text(json.dumps({
                "closed_loop_operations_per_second": throughput,
                "open_loop_load_fraction": OPEN_LOOP_LOAD_FRACTION,
                "open_loop_offered_operations_per_second": offered,
            }) + "\n")
        open_loop, _ = run_phase(deployment, evaluator_image, "open", artifacts, seeds["open"],
                                 rate=offered, state_in=journal)
        p99 = open_loop["latency_ms.p99"]
        deployment.save_pods(artifacts, "pods.txt")
        log(f"closed loop: {throughput:.1f} ops/s; open loop at {offered:.1f} ops/s "
            f"({OPEN_LOOP_LOAD_FRACTION:.0%}): p99 {p99:.2f} ms")
        write_stream(output, [HELLO, {
            "kind": "result", "label": "",
            "values": {"operations_per_second": throughput, "latency_ms.p99": p99},
        }])
        return 0
    except (JobError, subprocess.TimeoutExpired, KeyError, json.JSONDecodeError) as error:
        message = str(error) or type(error).__name__
        log(f"benchmark failed: {message}")
        if deployment is not None and deployment.created:
            deployment.save_pods(artifacts, "pods.txt")
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
    p.add_argument("action", choices=("restart", "flush"))
    p.add_argument("target")
    args = parser.parse_args(argv)

    def terminate(signum, _frame):
        raise SystemExit(128 + signum)

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, terminate)

    scratch = Path(tempfile.mkdtemp(prefix=f"{TASK}-"))
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
            return ctl(args.namespace, args.port, args.action, args.target)
        require_env("VIBESYS_K8S_REGISTRY")
        require_env("VIBESYS_K8S_BUILDER")
        if args.command == "check":
            return do_check(args.project_root.resolve(), scratch)
        return do_benchmark(args.project_root.resolve(), scratch, args.vs_output)
    except (JobError, subprocess.TimeoutExpired) as error:
        log(f"failed: {error}")
        return 1
    finally:
        # atexit handlers run next; block further signals so cleanup cannot be cut short.
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, signal.SIG_IGN)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
