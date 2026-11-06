"""hotel-k8s benchmark and accuracy check: DeathStarBench Hotel Reservation on Kubernetes, on vibesys-evaluator.

    python /task/bench.py benchmark --project-root ROOT --vs-output PATH   (one sample)
    python /task/bench.py accuracy --project-root ROOT                     (exit 0 PASS, 1 FAIL, 2 UNKNOWN)

The five parts: (1) the system under test, (2) the initial data (seed),
(3) the workload, (4) the benchmark function, (5) the validity rules; and
(6) the accuracy check. The data is dataset.py's. The workloads (operations
and request distributions) are the Go generators of evaluator/cmd; this
script orchestrates: it validates the candidate's application tier against
the capacity envelope, makes sure the node holds the data snapshot
(snapshot.py), deploys, runs the phases and reports. A benchmark only
measures: whether the answers are right is the accuracy check's, which
deploys the same system.
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import dataset as ds  # noqa: E402
import envelope  # noqa: E402
import snapshot  # noqa: E402
import vibesys_evaluator as vse  # noqa: E402
from vibesys_evaluator import microservices as ms  # noqa: E402
from vibesys_evaluator.microservices import Generator, get, k8s  # noqa: E402

# ---------------------------------------------------------------- (2) seed

MONGO = {
    "geo-db": "mongodb-geo:27018",
    "profile-db": "mongodb-profile:27019",
    "rate-db": "mongodb-rate:27020",
    "recommendation-db": "mongodb-recommendation:27021",
    "reservation-db": "mongodb-reservation:27022",
    "user-db": "mongodb-user:27023",
}
VERIFY_SAMPLES = 200  # random hotels and users compared document by document


def seed(data: ds.Spec, *, run=subprocess.run) -> None:
    """Check what the six MongoDB servers hold (runs in the seed Job).

    The data is not written here: each MongoDB pod restored its database from the node's
    snapshot of this dataset before mongod started (snapshot.py; `prepare` made sure it
    exists and put its version into the manifest). `hotel-data verify` checks each server's
    snapshot marker names the dataset's version and that the collections, their indexes and
    counts, and the documents of random hotels and users are the generator's. It runs once
    the data tier is available and before the services start.
    """
    with tempfile.NamedTemporaryFile("wb", suffix=".json") as spec:
        spec.write(ds.encode(data))
        spec.flush()
        argv = ["hotel-data", "verify", "--dataset", spec.name, "--samples", str(VERIFY_SAMPLES)]
        for database, address in MONGO.items():
            argv += ["--server", f"{database}={address}"]
        result = run(argv, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"the data tier does not hold the task's dataset (hotel-data verify exited {result.returncode})")


# -------------------------------------------------- (1) system under test

PROBE_LOCATION = {"lat": "37.7867", "lon": "-122.4112"}

# The deployed manifest is the task's k8s/manifest.yaml (data tier, ConfigMaps, frontend
# Service) followed by the candidate's application tier (hotelReservation/deploy/app.yaml),
# which `prepare` validates against the capacity envelope and writes to DEPLOYED in this
# script's own directory before the package reads it (k8s.App takes one manifest file of
# the task directory). It must be this directory, not a copy: the package starts its pods
# with this script's path (/task/bench.py), and vibesys-cli builds the task image from
# regular files only (no symlinks). The Dockerfile makes k8s/ writable for the benchmark's user.
DEPLOYED = "k8s/deployed.yaml"

SYSTEM = k8s.App(
    name="hotel-k8s",
    manifest=DEPLOYED,  # ${NAMESPACE} and ${HOTEL_IMAGE} are substituted
    images={"HOTEL_IMAGE": "hotelReservation"},  # built from PROJECT_ROOT/hotelReservation
    # MongoDB (restored from the node's snapshot), memcached, consul and jaeger: applied and
    # available before the seed; the candidate's application Deployments start after it.
    data_tier="tier=data",
    seed=seed,
    gateway="http://frontend:5000",
    ready=[
        get("/user", query={"username": "Cornell_30", "password": "0000000000"}),
        get("/recommendations", query={"require": "rate", **PROBE_LOCATION}),
        get("/hotels", query={"inDate": "2300-01-01", "outDate": "2300-01-02", **PROBE_LOCATION}),
    ],
    # The cache tiers (task-owned, k8s/manifest.yaml), which the accuracy check's faults empty.
    caches={cache: "memcached" for cache in ("memcached-profile", "memcached-rate", "memcached-reserve")},
    # Each MongoDB pod copies the node's snapshot (49 GB in all) into its emptyDir before mongod
    # starts: the six copies at once outlasted the default 300 s in 3 of 9 deploys here.
    wait_secs=1800,
)


# ------------------------------------------------------------ (3) workload
# The operations, their weights and how each request is drawn are Go:
# evaluator/internal/hotelworkload on the vibesys-evaluator Go SDK, over the dataset file and the
# pinned application's reference answers (hotelref.Reference). The task image builds it as
# hotel-generator (evaluator/cmd/generator); each phase pod runs it under loadgen.

WORKLOAD = Generator(["hotel-generator"])  # on the task image's PATH


# ------------------------------------------------------------ (4) benchmark

CLIENTS = 32
CLOSED_TRIALS, CLOSED_SECS, CLOSED_WARMUP = 2, 60, 15
OPEN_TRIALS, OPEN_SECS, OPEN_WARMUP = 3, 60, 10
OPEN_LOOP_LOAD_FRACTION = 0.7


def benchmark(run: ms.Run) -> None:
    """Capacity in a closed loop, then the p99 latency at 70% of it in an open loop.

    Two closed-loop trials (32 clients, 15 s warmup, 60 s): the throughput is
    their median. Three open-loop trials (10 s warmup, 60 s, evenly spaced
    arrivals as before) at 0.7 x that throughput: the p99 is their median.
    The sample's writes carry from phase to phase, so nothing is reset.
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
# No error in any window, every operation of the mix completed, and the open loop offers at
# least 95% of its rate.
VALIDITY = vse.Validity(max_error_rate=0.0, min_sustained_ratio=0.95, min_ops_per_operation=1)

_PREPARE_ERROR: list[Exception] = []

# How long a sample waits for the data snapshot: a first build takes about 25 minutes; past
# this the sample fails and the data Job keeps building for the next one.
SNAPSHOT_WAIT_SECONDS = 2700


# The commands that deploy the system: the benchmark and the accuracy check.
DEPLOYING = ("benchmark", "accuracy")


def prepare(argv: list[str], target: Path, *, ensure=snapshot.ensure, environ=os.environ) -> envelope.App | None:
    """For `benchmark` and `accuracy`: validate the candidate's application tier,
    make sure the node holds the data snapshot, and write the deployed manifest (restoring that
    snapshot).

    A failure is kept (`prepare_error()`) and raised by `dataset()`, the first thing a sample
    calls inside its error handling and before it builds or deploys anything, so that the sample
    fails with the validator's (or the snapshot's) message in its records. The envelope is
    checked first: a candidate outside it fails before the snapshot is looked at. Returns the
    candidate's tier, or None for another command or a failure.
    """
    if not argv or argv[0] not in DEPLOYING:
        return None
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--project-root", type=Path, required=True)
    args, _ = parser.parse_known_args(argv[1:])
    target.unlink(missing_ok=True)
    log = lambda line: print(line, file=sys.stderr, flush=True)  # noqa: E731
    try:
        app = envelope.load(args.project_root.resolve(), log=log)
        registry, builder = environ.get("VIBESYS_K8S_REGISTRY", ""), environ.get("VIBESYS_K8S_BUILDER", "")
        if not registry or not builder:
            raise snapshot.SnapshotError("VIBESYS_K8S_REGISTRY and VIBESYS_K8S_BUILDER must be set (the data snapshot's Job)")
        version = ensure(ds.encode(ds.generate()).decode(), registry=registry, builder=builder,
                         deadline_seconds=SNAPSHOT_WAIT_SECONDS, log=log).version
    except (envelope.EnvelopeError, snapshot.SnapshotError) as error:
        _PREPARE_ERROR.append(error)
        return None
    temporary = target.with_suffix(".tmp")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_text(envelope.compose(app, version))
        os.replace(temporary, target)
    except OSError as error:
        raise SystemExit(
            f"bench.py: cannot write the deployed manifest {target}: {error} "
            "(the task image makes k8s/ writable for the benchmark's user; see the Dockerfile)"
        ) from None
    return app


def prepare_error() -> Exception | None:
    """What made `prepare` fail, if it did."""
    return _PREPARE_ERROR[0] if _PREPARE_ERROR else None


def dataset() -> ds.Spec:
    if _PREPARE_ERROR:
        raise _PREPARE_ERROR[0]
    return ds.generate()


# ------------------------------------------------------- (6) accuracy check
# vibesys-evaluator's accuracy run (its docs/spec.md, "Accuracy run") over the system above: the
# candidate's application tier within the capacity envelope, the task's data tier restored from
# the node's data snapshot, the seed's check of what MongoDB holds. It runs ACCURACY_PHASES
# open-loop phases of the accuracy workload (evaluator/internal/hotelaccuracy, built as
# hotel-accuracy-generator) at a fixed ACCURACY_RATE with every answer captured, applies the
# faults below between them, tears the system down and runs the oracle (evaluator/cmd/oracle,
# built as hotel-oracle: internal/hotelspec on vibesys-evaluator's checker) on the history. The
# report, the failures' timelines and the history go to /vibesys/artifacts/accuracy.
#
# The load: a fixed, modest rate (the base serves about 390 operations/s of the benchmark's
# mix in a closed loop; at 200/s its p99 is about 4 ms), so that the answers are judged, not the
# capacity: the base lost no answer and had at most 3 operations of one hotel in flight at once
# (the report's MaxInFlight). Four phases of 30 s: about 26,000 operations, most of them on the
# ten hot hotels.
ACCURACY_PHASES, ACCURACY_RATE, ACCURACY_SECS, ACCURACY_WARMUP = 4, 200, 30, 2

ACCURACY_WORKLOAD = Generator(["hotel-accuracy-generator"])  # on the task image's PATH
ORACLE = ["hotel-oracle"]


def faults(app: envelope.App) -> list[ms.Fault]:
    """The fault schedule over the candidate's Deployments and the task's caches.

    The faults are what the oracle's model says changes nothing (hotelspec's Spec.Fault is nil:
    an acknowledged reservation, capacity, profile or price lives in MongoDB, and the caches are
    rebuilt from it), so a candidate that keeps an acknowledged write only in a pod or in
    memcached fails:

    - after phase 0, one pod of each of the candidate's Deployments, one after the other, each
      waited for until the application serves again (the others serve meanwhile);
    - after phase 1, the three memcached tiers emptied in place (`flush_all`);
    - after phase 2, every pod of each of the candidate's Deployments at once, one Deployment
      after the other.

    MongoDB is never restarted: its pods restore the snapshot into an emptyDir, so a new pod
    would lose the sample's writes by the task's design, not the candidate's.
    """
    return (
        [ms.Fault.restart(name, after=0) for name in app.names]
        + [ms.Fault.flush(cache, after=1) for cache in SYSTEM.caches]
        + [ms.Fault.restart_all(name, after=2) for name in app.names]
    )


def check(app: envelope.App | None) -> ms.Accuracy:
    """The accuracy check of the candidate's tier.

    Its workload is constant (the phase pods build it again); the faults name the candidate's
    Deployments, which only the `accuracy` command (the orchestrator) knows and applies.
    """
    return ms.Accuracy(
        phases=ACCURACY_PHASES, rate=ACCURACY_RATE, secs=ACCURACY_SECS, warmup=ACCURACY_WARMUP,
        workload=ACCURACY_WORKLOAD,
        faults=faults(app) if app is not None else [],
        oracle=ORACLE,
    )


def system(app: envelope.App | None) -> k8s.App:
    """SYSTEM, with the candidate's Deployments open to the accuracy check's restarts."""
    return SYSTEM if app is None else dataclasses.replace(SYSTEM, restartable=tuple(app.names))


def main(argv: list[str]) -> None:
    app = prepare(argv, Path(__file__).resolve().parent / DEPLOYED)
    error = prepare_error()
    # A benchmark sample fails with the error in its records (dataset() raises it); the
    # accuracy check's verdict is FAIL for a candidate outside the envelope, else UNKNOWN.
    if argv[:1] == ["accuracy"] and error is not None:
        if isinstance(error, envelope.EnvelopeError):
            sys.stderr.write(f"FAIL: the candidate's application tier is outside the capacity envelope: {error}\n")
            sys.exit(1)
        sys.stderr.write(f"UNKNOWN: the accuracy run did not complete: {error}\n")
        sys.exit(2)
    ms.main(
        dataset=dataset,
        system=system(app),
        workload=WORKLOAD,
        benchmark=benchmark,
        metrics=METRICS,
        validity=VALIDITY,
        accuracy=check(app),
    )


if __name__ == "__main__":
    main(sys.argv[1:])
