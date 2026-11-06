"""hotel-k8s accuracy check: DeathStarBench Hotel Reservation on Kubernetes, judged by its oracle.

    python /task/accuracy.py accuracy --project-root ROOT     exit 0 PASS, 1 FAIL, 2 UNKNOWN

vibesys-evaluator's accuracy run (its docs/spec.md, "Accuracy run") over the system bench.py
deploys: the candidate's application tier within the capacity envelope, the task's data tier
restored from the node's data snapshot, the seed's check of what MongoDB holds. It runs PHASES
open-loop phases of the accuracy workload (evaluator/internal/hotelaccuracy, built as
hotel-accuracy-generator) at a fixed RATE with every answer captured, applies the faults below
between them, tears the system down and runs the oracle (evaluator/cmd/oracle, built as
hotel-oracle: internal/hotelspec on vibesys-evaluator's checker) on the history. The report, the
failures' timelines and the history go to /vibesys/artifacts/accuracy (accuracy/ in
$VIBESYS_ARTIFACTS_DIR).

The faults are what the oracle's model says changes nothing (hotelspec's Spec.Fault is nil: an
acknowledged reservation, capacity, profile or price lives in MongoDB, and the caches are rebuilt
from it), so a candidate that keeps an acknowledged write only in a pod or in memcached fails:

- after phase 0, one pod of each of the candidate's Deployments, one after the other, each
  waited for until the application serves again (the others serve meanwhile);
- after phase 1, the three memcached tiers emptied in place (`flush_all`);
- after phase 2, every pod of each of the candidate's Deployments at once, one Deployment after
  the other.

MongoDB is never restarted: its pods restore the snapshot into an emptyDir, so a new pod would
lose the sample's writes by the task's design, not the candidate's.

A candidate outside the capacity envelope fails (exit 1); a data snapshot that cannot be made
ready is UNKNOWN (exit 2), as is any run that does not complete.
"""

from __future__ import annotations

import dataclasses
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import bench  # noqa: E402
import envelope  # noqa: E402
from vibesys_evaluator import microservices as ms  # noqa: E402
from vibesys_evaluator.microservices import Generator, k8s  # noqa: E402

# The load: a fixed, modest rate (the base serves about 450 operations/s of the benchmark's
# mix in a closed loop; at 200/s its p99 is about 4 ms), so that the answers are judged, not the
# capacity: the base lost no answer and had at most 3 operations of one hotel in flight at once
# (the report's MaxInFlight). Four phases of 30 s: about 26,000 operations, most of them on the
# ten hot hotels.
PHASES, RATE, SECS, WARMUP = 4, 200, 30, 2

WORKLOAD = Generator(["hotel-accuracy-generator"])  # on the task image's PATH
ORACLE = ["hotel-oracle"]

# The cache tiers (task-owned, k8s/manifest.yaml) and the command that empties one in place: the
# memcached image has bash, whose /dev/tcp needs no client; a reply other than OK fails the run.
MEMCACHED = ("memcached-profile", "memcached-rate", "memcached-reserve")
FLUSH = [
    "bash", "-c",
    "exec 3<>/dev/tcp/127.0.0.1/11211; printf 'flush_all\\r\\n' >&3; read -r reply <&3; "
    'echo "flush_all: $reply"; case "$reply" in OK*) ;; *) exit 1 ;; esac',
]


def faults(app: envelope.App) -> list[ms.Fault]:
    """The fault schedule over the candidate's Deployments (see the module docstring)."""
    return (
        [ms.Fault.restart(name, after=0) for name in app.names]
        + [ms.Fault.exec(cache, FLUSH, after=1) for cache in MEMCACHED]
        + [ms.Fault.restart_all(name, after=2) for name in app.names]
    )


def check(app: envelope.App | None) -> ms.Accuracy:
    """The accuracy check of the candidate's tier (no faults for the other commands, which never run it)."""
    return ms.Accuracy(
        phases=PHASES, rate=RATE, secs=SECS, warmup=WARMUP,
        faults=faults(app) if app is not None else [],
        oracle=ORACLE,
    )


def system(app: envelope.App | None) -> k8s.App:
    """bench.py's system, with the candidate's Deployments and the caches open to the faults."""
    if app is None:
        return bench.SYSTEM
    return dataclasses.replace(bench.SYSTEM, restartable=tuple(app.names),
                               caches={cache: "memcached" for cache in MEMCACHED})


def not_a_benchmark(run: ms.Run) -> None:
    raise SystemExit("accuracy.py is the accuracy check; the benchmark is bench.py")


def main(argv: list[str]) -> None:
    app = bench.prepare(argv, Path(__file__).resolve().parent / bench.DEPLOYED)
    error = bench.prepare_error()
    if argv[:1] == ["accuracy"] and error is not None:
        if isinstance(error, envelope.EnvelopeError):
            sys.stderr.write(f"FAIL: the candidate's application tier is outside the capacity envelope: {error}\n")
            sys.exit(1)
        sys.stderr.write(f"UNKNOWN: the accuracy run did not complete: {error}\n")
        sys.exit(2)
    ms.main(
        dataset=bench.dataset,
        system=system(app),
        workload=WORKLOAD,
        benchmark=not_a_benchmark,
        metrics=bench.METRICS,
        validity=bench.VALIDITY,
        accuracy=check(app),
    )


if __name__ == "__main__":
    main(sys.argv[1:])
