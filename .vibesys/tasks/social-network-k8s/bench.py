"""social-network-k8s benchmark: DeathStarBench Social Network on Kubernetes, on vibesys-evaluator.

    python /task/bench.py benchmark --project-root ROOT --vs-output PATH   (one sample)

The five parts: (1) the system under test, (2) the initial data (seed),
(3) the workload, (4) the benchmark function, (5) the validity rules. The
data is dataset.py's seed of the Go dataset (evaluator/internal/dataset). The
workload (operations and request distributions) is the Go generator of
evaluator/cmd/generator. A benchmark only measures: whether the answers are
right is the accuracy check's (accuracy.py, on the same system).
"""

from __future__ import annotations

import json
import statistics
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import dataset as ds  # noqa: E402
import vibesys_evaluator as vse  # noqa: E402
from vibesys_evaluator import microservices as ms  # noqa: E402
from vibesys_evaluator.microservices import Generator, get, k8s  # noqa: E402

# ---------------------------------------------------------------- (2) seed

SEED_TIMEOUT_SECONDS = 1200


def seed_command(data: ds.Dataset) -> list[str]:
    """The Go seed (on the task image's PATH): the data tier's addresses are its defaults,
    the manifest's Service names (user-mongodb:27017, home-timeline-redis:6379, ...)."""
    return ["seed", "--dataset-seed", str(data.seed), "--timeout", f"{SEED_TIMEOUT_SECONDS}s"]


def seed_summary(output: str, data: ds.Dataset) -> dict:
    """The seed's summary (its last JSON line), checked to be of this dataset."""
    summary = None
    for line in output.splitlines():
        if line.startswith("{"):
            try:
                summary = json.loads(line)
            except json.JSONDecodeError:
                pass
    if summary is None:
        raise RuntimeError(f"seed wrote no summary; output tail:\n{output[-1500:]}")
    if summary.get("dataset_digest") != data.digest:
        raise RuntimeError(
            f"seed wrote the dataset {summary.get('dataset_digest')}, want {data.digest} "
            "(dataset.py and internal/dataset disagree)"
        )
    return summary


def seed(data: ds.Dataset) -> None:
    """Write the dataset into the data tier (runs in the seed Job, before the services start).

    The seed writes an empty data tier, leaves one that already holds this dataset alone,
    and fails on anything else; it also creates the indexes the services look users and
    user timelines up by.
    """
    completed = subprocess.run(
        seed_command(data), stdout=subprocess.PIPE, text=True, check=False,
        timeout=SEED_TIMEOUT_SECONDS + 60,
    )
    print(completed.stdout, end="", flush=True)
    if completed.returncode != 0:
        raise RuntimeError(f"seed exited {completed.returncode}; output tail:\n{completed.stdout[-1500:]}")
    print(f"seed: {json.dumps(seed_summary(completed.stdout, data))[:400]}", flush=True)


# -------------------------------------------------- (1) system under test

# A user id far from the dataset's, so the probes leave its caches alone.
PROBE_USER = "1999999999"

SYSTEM = k8s.App(
    name="social-network-k8s",
    manifest="k8s/manifest.yaml",  # ${NAMESPACE} and ${SOCIAL_NETWORK_IMAGE} are substituted
    images={"SOCIAL_NETWORK_IMAGE": "socialNetwork"},  # built from PROJECT_ROOT/socialNetwork
    # MongoDB, Redis, memcached and jaeger: applied and available before the seed; the
    # candidate's services and nginx-thrift start after it.
    data_tier="tier=data",
    seed=seed,
    gateway="http://nginx-thrift:8080",
    # nginx, user-timeline (MongoDB, Redis), home-timeline (Redis) and post-storage.
    ready=[
        get("/"),
        get("/wrk2-api/user-timeline/read", query={"user_id": PROBE_USER, "start": "0", "stop": "1"}),
        get("/wrk2-api/home-timeline/read", query={"user_id": PROBE_USER, "start": "0", "stop": "1"}),
    ],
)


# ------------------------------------------------------------ (3) workload
# The operations (50% user-timeline reads, 40% home-timeline reads, 10% compose with a
# read-your-write read, 1% follow and unfollow) and how each request is drawn are Go:
# evaluator/internal/snworkload on the vibesys-evaluator Go SDK. The task image builds it as
# sn-generator (evaluator/cmd/generator); each phase pod runs it under loadgen to generate the
# request stream. Nothing checks the answers here (accuracy.py does, with its own workload).

WORKLOAD = Generator(["sn-generator"])  # on the task image's PATH


# ------------------------------------------------------------ (4) benchmark

CLIENTS = 32
CLOSED_TRIALS, CLOSED_SECS, CLOSED_WARMUP = 2, 20, 10
OPEN_TRIALS, OPEN_SECS, OPEN_WARMUP = 3, 60, 10
OPEN_LOOP_LOAD_FRACTION = 0.7


def benchmark(run: ms.Run) -> None:
    """Capacity in a closed loop, then the p99 latency at 70% of it in an open loop.

    Two closed-loop trials (32 clients, 10 s warmup, 20 s): the throughput is
    their median. Three open-loop trials (10 s warmup, 60 s, evenly spaced
    arrivals) at 0.7 x that throughput: the p99 is their median. The sample's
    writes carry from phase to phase, so nothing is reset.
    """
    closed = [
        run.closed_loop(clients=CLIENTS, secs=CLOSED_SECS, warmup=CLOSED_WARMUP).ops_per_sec
        for _ in range(CLOSED_TRIALS)
    ]
    throughput = statistics.median(closed)
    # p99 is measured at a fixed fraction of this sample's own throughput, so
    # a faster candidate is held to proportionally more load.
    rate = OPEN_LOOP_LOAD_FRACTION * throughput
    p99 = statistics.median(
        run.open_loop(rate=rate, secs=OPEN_SECS, warmup=OPEN_WARMUP, arrivals="uniform").p99_ms
        for _ in range(OPEN_TRIALS)
    )
    run.report({"operations_per_second": throughput, "latency_ms.p99": p99})


# ------------------------------------------------------------- (5) validity

METRICS = {
    "operations_per_second": vse.Metric(unit="ops/s", direction="max"),
    "latency_ms.p99": vse.Metric(unit="ms", direction="min"),
}
# No error in any window (an answer other than 200 is one), every operation of the mix completed,
# and the open loop offers at least 95% of its rate.
VALIDITY = vse.Validity(max_error_rate=0.0, min_sustained_ratio=0.95, min_ops_per_operation=1)


if __name__ == "__main__":
    ms.main(
        dataset=ds.generate,
        system=SYSTEM,
        workload=WORKLOAD,
        benchmark=benchmark,
        metrics=METRICS,
        validity=VALIDITY,
    )
