"""hotel-k8s benchmark: DeathStarBench Hotel Reservation on Kubernetes, on vibesys-evaluator.

    python /task/bench.py benchmark --project-root ROOT --vs-output PATH   (one sample)

The five parts: (1) the system under test, (2) the initial data (seed),
(3) the workload, (4) the benchmark function, (5) the validity rules. The
data is dataset.py's. The workload (operations, request distributions, the
model the writes change and the checks of the responses) is the Go generator
of evaluator/cmd/generator, which shares its model with the accuracy oracle;
this script orchestrates: it validates the candidate's application tier
against the capacity envelope, deploys, runs the phases and reports.
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import dataset as ds  # noqa: E402
import envelope  # noqa: E402
import vibesys_evaluator as vse  # noqa: E402
from vibesys_evaluator.deploy import k8s  # noqa: E402
from vibesys_evaluator.service import Generator, get  # noqa: E402

# ---------------------------------------------------------------- (2) seed

MONGO = {
    "geo-db": "mongodb-geo:27018",
    "profile-db": "mongodb-profile:27019",
    "rate-db": "mongodb-rate:27020",
    "recommendation-db": "mongodb-recommendation:27021",
    "reservation-db": "mongodb-reservation:27022",
    "user-db": "mongodb-user:27023",
}
INSERT_BATCH = 1000


def seed(data: ds.Dataset) -> None:
    """Write the dataset into the six MongoDB servers (runs in the seed Job).

    It runs once the data tier is available and before the services start:
    each service seeds its own test data only into an empty collection, and
    geo and recommendation load their collection once at startup. Each
    collection is replaced: dropped, filled in batches, indexed, counted.
    """
    import pymongo  # in the task image

    collections = ds.documents(data)
    print(f"seed: documents digest {ds.documents_digest(collections)}", flush=True)
    errors: list[str] = []

    def write(database: str) -> None:
        client = pymongo.MongoClient(f"mongodb://{MONGO[database]}", serverSelectionTimeoutMS=5000)
        deadline = time.monotonic() + 300
        while True:
            try:
                client.admin.command("ping")
                break
            except pymongo.errors.PyMongoError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
        for collection in (c for c in collections if c.database == database):
            started = time.monotonic()
            db = client[database]
            db.drop_collection(collection.name)
            db.create_collection(collection.name)
            target = db[collection.name]
            documents = [dict(d) for d in collection.documents]  # insert_many adds _id
            for start in range(0, len(documents), INSERT_BATCH):
                target.insert_many(documents[start:start + INSERT_BATCH], ordered=True)
            target.create_indexes([
                pymongo.IndexModel([(key, pymongo.ASCENDING) for key in keys], name=name, unique=unique)
                for name, keys, unique in collection.indexes
            ])
            count = target.count_documents({})
            if count != len(documents):
                errors.append(f"{database}.{collection.name} holds {count} documents, want {len(documents)}")
            print(f"seed: {database}.{collection.name}: {count} documents in "
                  f"{time.monotonic() - started:.1f}s", flush=True)
        client.close()

    def guarded(database: str) -> None:
        try:
            write(database)
        except Exception as error:  # reported below, as the Job's failure
            errors.append(f"{database}: {type(error).__name__}: {error}")

    threads = [threading.Thread(target=guarded, args=(database,)) for database in MONGO]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise RuntimeError("; ".join(sorted(errors)))


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
    # MongoDB, memcached, consul and jaeger: applied and available before the seed; the
    # candidate's application Deployments start after it.
    data_tier="tier=data",
    seed=seed,
    gateway="http://frontend:5000",
    ready=[
        get("/user", query={"username": "Cornell_30", "password": "0000000000"}),
        get("/recommendations", query={"require": "rate", **PROBE_LOCATION}),
        get("/hotels", query={"inDate": "2300-01-01", "outDate": "2300-01-02", **PROBE_LOCATION}),
    ],
)


# ------------------------------------------------------------ (3) workload
# The operations, their weights, how each request is drawn, the model the writes change and
# the checks are Go: evaluator/internal/hotelworkload on the vibesys-evaluator Go SDK, over
# the accuracy oracle's own model (the dataset file, hotel.Reference, hotel.State). The task
# image builds it as hotel-generator (evaluator/cmd/generator); each phase pod runs it under
# loadgen to generate the request stream, then to check the phase's responses.

WORKLOAD = Generator(["hotel-generator"])  # on the task image's PATH


# ------------------------------------------------------------ (4) benchmark

CLIENTS = 32
CLOSED_TRIALS, CLOSED_SECS, CLOSED_WARMUP = 2, 60, 15
OPEN_TRIALS, OPEN_SECS, OPEN_WARMUP = 3, 60, 10
OPEN_LOOP_LOAD_FRACTION = 0.7


def benchmark(run: vse.Run) -> None:
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

_ENVELOPE_ERROR: list[envelope.EnvelopeError] = []


def prepare(argv: list[str], target: Path) -> envelope.App | None:
    """For `benchmark`: validate the candidate's application tier and write the deployed manifest.

    A violation is kept and raised by `dataset()`, the first thing a sample calls inside
    its error handling and before it builds or deploys anything, so that the sample fails
    with the validator's message in its records.
    """
    if argv[:1] != ["benchmark"]:
        return None
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--project-root", type=Path, required=True)
    args, _ = parser.parse_known_args(argv[1:])
    target.unlink(missing_ok=True)
    try:
        app = envelope.load(args.project_root.resolve(), log=lambda line: print(line, file=sys.stderr))
    except envelope.EnvelopeError as error:
        _ENVELOPE_ERROR.append(error)
        return None
    temporary = target.with_suffix(".tmp")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_text(envelope.compose(app))
        os.replace(temporary, target)
    except OSError as error:
        raise SystemExit(
            f"bench.py: cannot write the deployed manifest {target}: {error} "
            "(the task image makes k8s/ writable for the benchmark's user; see the Dockerfile)"
        ) from None
    return app


def dataset() -> ds.Dataset:
    if _ENVELOPE_ERROR:
        raise _ENVELOPE_ERROR[0]
    return ds.generate()


if __name__ == "__main__":
    prepare(sys.argv[1:], Path(__file__).resolve().parent / DEPLOYED)
    vse.main(
        dataset=dataset,
        system=SYSTEM,
        workload=WORKLOAD,
        benchmark=benchmark,
        metrics={
            "operations_per_second": vse.Metric(unit="ops/s", direction="max"),
            "latency_ms.p99": vse.Metric(unit="ms", direction="min"),
        },
        # No error and no failed check in any window, every operation of the mix
        # completed, and the open loop offers at least 95% of its rate.
        validity=vse.Validity(max_error_rate=0.0, min_sustained_ratio=0.95, min_ops_per_operation=1),
    )
