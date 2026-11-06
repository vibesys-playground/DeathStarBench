"""social-network-k8s accuracy check: DeathStarBench Social Network on Kubernetes, judged by its oracle.

    python /task/accuracy.py accuracy --project-root ROOT     exit 0 PASS, 1 FAIL, 2 UNKNOWN

vibesys-evaluator's accuracy run (its docs/spec.md, "Accuracy run") over the system bench.py
deploys (the candidate's image, the task's manifest, the seeded data tier). It runs PHASES
open-loop phases of the accuracy workload (evaluator/internal/snaccuracy, built as
sn-accuracy-generator) at a fixed RATE with every answer captured, applies the faults below
between them, tears the system down and runs the oracle (evaluator/cmd/oracle, built as
sn-oracle: internal/snspec's author and reader specs on vibesys-evaluator's checker, ANDed) on
the history. The report, the failures' timelines and the history go to
/vibesys/artifacts/accuracy (accuracy/ in $VIBESYS_ARTIFACTS_DIR).

The faults are what OBJECTIVE.md's durability rule names and the oracle's model says change
nothing (snspec's Spec.Fault is nil: an acknowledged post, follow or registration survives a
restart of any stateless pod and a flush of any cache tier), so a candidate that keeps an
acknowledged write only in a pod or in a cache fails:

- after phase 0, one pod of each of the twelve application Deployments, one after the other,
  each waited for until the application serves again (the other replica serves meanwhile);
- after phase 1, the cache tiers emptied in place: the three memcached (`flush_all`),
  social-graph-redis and user-timeline-redis (`FLUSHALL`);
- after phase 2, every pod of each application Deployment at once, one Deployment after the
  other.

home-timeline-redis is a store, not a cache (the pinned application keeps home timelines only
there, without persistence), and MongoDB holds the data: neither is ever restarted or flushed.
"""

from __future__ import annotations

import dataclasses
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import bench  # noqa: E402
import dataset as ds  # noqa: E402
from vibesys_evaluator import microservices as ms  # noqa: E402
from vibesys_evaluator.microservices import Generator, k8s  # noqa: E402

# The load: a fixed, modest rate (the base serves well over 1,500 operations/s), so that the
# answers are judged, not the capacity; lost answers stay near zero and few operations of one
# author or reader are in flight at once (the report's MaxInFlight). Four phases of 30 s: about
# 36,000 operations, most of them on the 40 hot users.
PHASES, RATE, SECS, WARMUP = 4, 300, 30, 2

WORKLOAD = Generator(["sn-accuracy-generator"])  # on the task image's PATH
ORACLE = ["sn-oracle"]

# The application tier (tier=app in k8s/manifest.yaml): 11 C++ services and nginx-thrift.
APPLICATION = (
    "nginx-thrift",
    "compose-post-service",
    "home-timeline-service",
    "media-service",
    "post-storage-service",
    "social-graph-service",
    "text-service",
    "unique-id-service",
    "url-shorten-service",
    "user-mention-service",
    "user-service",
    "user-timeline-service",
)

# The cache tiers and the commands that empty one in place: the memcached image has bash, whose
# /dev/tcp needs no client, the Redis image redis-cli. A reply other than OK fails the run.
MEMCACHED = ("post-storage-memcached", "url-shorten-memcached", "user-memcached")
REDIS_CACHES = ("social-graph-redis", "user-timeline-redis")
FLUSH_MEMCACHED = [
    "bash", "-c",
    "exec 3<>/dev/tcp/127.0.0.1/11211; printf 'flush_all\\r\\n' >&3; read -r reply <&3; "
    'echo "flush_all: $reply"; case "$reply" in OK*) ;; *) exit 1 ;; esac',
]
FLUSH_REDIS = [
    "sh", "-c",
    'reply="$(redis-cli FLUSHALL)"; echo "FLUSHALL: $reply"; [ "$reply" = OK ]',
]

FAULTS = (
    [ms.Fault.restart(name, after=0) for name in APPLICATION]
    + [ms.Fault.exec(cache, FLUSH_MEMCACHED, after=1) for cache in MEMCACHED]
    + [ms.Fault.exec(cache, FLUSH_REDIS, after=1) for cache in REDIS_CACHES]
    + [ms.Fault.restart_all(name, after=2) for name in APPLICATION]
)

CHECK = ms.Accuracy(phases=PHASES, rate=RATE, secs=SECS, warmup=WARMUP, faults=FAULTS, oracle=ORACLE)

# bench.py's system, with the application and the caches open to the faults.
SYSTEM = dataclasses.replace(
    bench.SYSTEM,
    restartable=APPLICATION,
    caches={**{c: "memcached" for c in MEMCACHED}, **{c: "redis" for c in REDIS_CACHES}},
)


def not_a_benchmark(run: ms.Run) -> None:
    raise SystemExit("accuracy.py is the accuracy check; the benchmark is bench.py")


if __name__ == "__main__":
    ms.main(
        dataset=ds.generate,
        system=SYSTEM,
        workload=WORKLOAD,
        benchmark=not_a_benchmark,
        metrics=bench.METRICS,
        validity=bench.VALIDITY,
        accuracy=CHECK,
    )
