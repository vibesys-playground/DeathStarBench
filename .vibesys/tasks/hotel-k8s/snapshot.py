"""The data snapshot on the node: built once per dataset version, copied by every sample.

A sample does not insert its data. Each MongoDB pod of the data tier (k8s/manifest.yaml)
copies its database's files from the node's snapshot (an init container of the data tier's
own MongoDB image, reading the hostPath SNAPSHOT_ROOT/<version>/<database>) into its emptyDir
before mongod starts; a sample's writes stay in its namespace's emptyDirs, so sample k never
changes what sample k+1 starts from. The version (a digest of the documents the generator
writes, the collections and the MongoDB image: evaluator/internal/snapshot.Version) is
substituted into the manifest for VERSION_PLACEHOLDER.

`ensure()` makes the snapshot exist before a sample deploys: it runs `hotel-data ensure` in a
Job of the namespace DATA_NAMESPACE (outside every sample's namespace, so a build survives
the job that started it) on the data image (the task Dockerfile's `data` stage: the data
tier's mongod and the hotel-data command). The Job builds what the snapshot lacks (the first
time a version is asked for: about 25 minutes for the task's dataset, see README.md) and
otherwise only checks it and records its use; concurrent Jobs serialize on the snapshot's
lock. bench.py's `prepare` calls it before a benchmark sample and before an accuracy check;
`python snapshot.py ensure` (in the task image, with the cluster's environment) only makes the
snapshot exist, for example on a new cluster before its first job.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

TASK_DIR = Path(__file__).resolve().parent

DATA_NAMESPACE = "hk8s-data"
DATA_NAMESPACE_LABEL = "vibesys.dev/hotel-k8s-data"
# On the node: kind keeps the node's /var on the host's local disk.
SNAPSHOT_ROOT = "/var/lib/hotel-k8s/snapshots"
VERSION_PLACEHOLDER = "@DATA_SNAPSHOT_VERSION@"

# The data Job: requests are what a check of an existing snapshot needs, limits what a build
# uses (Burstable), so a Job that only checks does not hold the node's CPUs.
JOB_REQUESTS = {"cpu": "2", "memory": "4Gi"}
JOB_LIMITS = {"cpu": "24", "memory": "24Gi"}
# A build of the task's dataset takes about 25 minutes on 24 CPUs; checking takes seconds.
JOB_DEADLINE_SECONDS = 3 * 3600

# The data image: the task Dockerfile's `data` stage, tagged by the files it is built from.
DATA_IMAGE_FILES = ("Dockerfile", "evaluator")


class SnapshotError(Exception):
    """The snapshot could not be made to exist; the message says why and where to look."""


def render(text: str, version: str) -> str:
    """The manifest with the snapshot version substituted (the data tier's restore)."""
    if not version or not all(c in "0123456789abcdef" for c in version):
        raise SnapshotError(f"snapshot version {version!r} is not a hex digest")
    return text.replace(VERSION_PLACEHOLDER, version)


def tree_digest(root: Path, names: tuple[str, ...] = DATA_IMAGE_FILES) -> str:
    """16 hex digits over the relative paths and contents of the named files and trees."""
    import hashlib  # noqa: PLC0415

    digest = hashlib.sha256()
    paths: list[Path] = []
    for name in names:
        path = root / name
        paths.extend(sorted(p for p in path.rglob("*") if p.is_file()) if path.is_dir() else [path])
    for path in sorted(paths, key=lambda p: p.relative_to(root).as_posix()):
        if "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()[:16]


def data_image(registry: str, task_dir: Path = TASK_DIR) -> str:
    return f"{registry}/hotel-k8s/data:{tree_digest(task_dir)}"


def build_data_image(registry: str, builder: str, *, task_dir: Path = TASK_DIR,
                     run: Callable[..., subprocess.CompletedProcess] = subprocess.run) -> str:
    """Build and push the data image (BuildKit's cache makes an unchanged one a no-op)."""
    image = data_image(registry, task_dir)
    argv = [
        "buildctl", "--addr", builder, "build", "--frontend", "dockerfile.v0",
        "--local", f"context={task_dir}", "--local", f"dockerfile={task_dir}",
        "--opt", "target=data",
        "--output", f"type=image,name={image},push=true,registry.insecure=true",
    ]
    result = run(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=2400, check=False)
    if result.returncode != 0:
        raise SnapshotError(f"building the data image {image} failed: {(result.stdout or '')[-2000:]}")
    return image


def job(image: str, spec_json: str) -> dict:
    """The data Job: `hotel-data ensure` on the node's snapshot root."""
    script = 'printf "%s" "$HOTEL_DATASET_SPEC" > /tmp/dataset.json && exec hotel-data ensure --dataset /tmp/dataset.json --root /snapshots'
    return {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"generateName": "data-", "namespace": DATA_NAMESPACE},
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": JOB_DEADLINE_SECONDS,
            "ttlSecondsAfterFinished": 3600,
            "template": {"spec": {
                "restartPolicy": "Never",
                "automountServiceAccountToken": False,
                "terminationGracePeriodSeconds": 30,
                "containers": [{
                    "name": "data",
                    "image": image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["sh", "-c", script],
                    "env": [{"name": "HOTEL_DATASET_SPEC", "value": spec_json}],
                    "resources": {"requests": dict(JOB_REQUESTS), "limits": dict(JOB_LIMITS)},
                    "volumeMounts": [{"name": "snapshots", "mountPath": "/snapshots"}],
                }],
                "volumes": [{"name": "snapshots",
                             "hostPath": {"path": SNAPSHOT_ROOT, "type": "DirectoryOrCreate"}}],
            }},
        },
    }


@dataclass(frozen=True)
class Ensured:
    version: str
    seconds: float
    summary: dict


def ensure(spec_json: str, *, registry: str, builder: str, deadline_seconds: float,
           log: Callable[[str], None],
           kubectl: Callable[..., subprocess.CompletedProcess] | None = None,
           build: Callable[..., str] = build_data_image,
           clock: Callable[[], float] = time.monotonic,
           sleep: Callable[[float], None] = time.sleep) -> Ensured:
    """Run the data Job until the snapshot of spec_json exists; return its version."""
    kubectl = kubectl or _kubectl
    started = clock()
    image = build(registry, builder)
    created = kubectl(["create", "namespace", DATA_NAMESPACE], check=False)
    if created.returncode != 0 and "AlreadyExists" not in (created.stdout or ""):
        raise SnapshotError(f"cannot create namespace {DATA_NAMESPACE}: {created.stdout}")
    kubectl(["label", "namespace", DATA_NAMESPACE, f"{DATA_NAMESPACE_LABEL}=1", "--overwrite"], check=False)
    name = kubectl(["create", "-o", "name", "-f", "-"], input_text=json.dumps(job(image, spec_json))).stdout.strip()
    log(f"data snapshot: {name} in namespace {DATA_NAMESPACE} (image {image})")
    last_line = ""
    while True:
        status = json.loads(kubectl(["-n", DATA_NAMESPACE, "get", name, "-o", "json"]).stdout).get("status", {})
        if status.get("succeeded") or status.get("failed"):
            break
        if clock() - started > deadline_seconds:
            raise SnapshotError(
                f"the data snapshot is not ready after {deadline_seconds:.0f}s; {name} in namespace "
                f"{DATA_NAMESPACE} keeps building it (kubectl -n {DATA_NAMESPACE} logs {name})")
        tail = kubectl(["-n", DATA_NAMESPACE, "logs", name, "--tail=1"], check=False).stdout.strip()
        if tail and tail != last_line:
            log(f"data snapshot: {tail}")
            last_line = tail
        sleep(10)
    logs = kubectl(["-n", DATA_NAMESPACE, "logs", name], check=False).stdout
    if not status.get("succeeded"):
        raise SnapshotError(f"{name} failed; log tail:\n{logs[-2000:]}")
    summary = json.loads(logs.strip().splitlines()[-1])
    log(f"data snapshot {summary['version']} ready in {clock() - started:.0f}s")
    return Ensured(version=summary["version"], seconds=clock() - started, summary=summary)


def _kubectl(args: list[str], *, input_text: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(["kubectl", *args], input=input_text, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=120, check=False)
    if check and result.returncode != 0:
        raise SnapshotError(f"kubectl {' '.join(args[:3])} failed ({result.returncode}): {result.stdout[-1500:]}")
    return result


def require_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise SnapshotError(f"{name} is not set")
    return value


def main(argv: list[str]) -> int:
    """`ensure`: build (or check) the node's snapshot of the task's dataset and wait for it."""
    if argv != ["ensure"]:
        print("usage: python snapshot.py ensure   (KUBECONFIG, VIBESYS_K8S_REGISTRY and VIBESYS_K8S_BUILDER set)",
              file=sys.stderr)
        return 2
    sys.path.insert(0, str(TASK_DIR))
    import dataset  # noqa: PLC0415

    def log(line: str) -> None:
        print(line, file=sys.stderr, flush=True)

    try:
        ensured = ensure(dataset.encode(dataset.generate()).decode(), registry=require_env("VIBESYS_K8S_REGISTRY"),
                         builder=require_env("VIBESYS_K8S_BUILDER"), deadline_seconds=JOB_DEADLINE_SECONDS, log=log)
    except SnapshotError as error:
        log(f"snapshot: {error}")
        return 1
    log(f"data snapshot {ensured.version} is ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
