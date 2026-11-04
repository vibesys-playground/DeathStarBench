# hotel-k8s task files

Task-owned inputs for the Hotel Reservation Kubernetes task. `OBJECTIVE.md` is for
the optimizing agents; this file is for whoever maintains the task. Nothing here
is a candidate input: the CLI versions these files as the task.

| Path | What |
| --- | --- |
| `vibesys.input.toml`, `objectives.toml` | Manifest and the two Pareto objectives. |
| `Dockerfile` | Job image: kubectl 1.34, buildctl, Go, Python 3, and `servicebench` and `hotel-correctness` built from a clone of the vibesys repository at commit `de037943cb6a8e6e73d00e8bc19fa53570558583`. Base images are pinned by digest. |
| `run.py` | Trusted wrapper: `check`, `benchmark`, and `ctl` (the oracle's crash/start commands). |
| `k8s/manifest.yaml` | The application: 19 Deployments, 19 Services, 8 ConfigMaps. |
| `oracle/` | The Go semantic oracle (`hotel-correctness`), copied from `examples/microservices/hotel-correctness/.vibesys/tasks/compose/evaluator` at the vibesys commit above, tests dropped; its `go.mod` replaces the evaluator modules by the Dockerfile's clone at `/src`. |
| `workload/closed.toml` | Closed loop, concurrency 32, the canonical mixed workload; also the oracle's workload. Objective `operations_per_second`. |
| `workload/open.toml` | The same mix, open loop at a fixed offered rate; objective `latency_ms.p99`. |

## What a job does

1. Builds `hotelReservation/` with `buildctl` against `$VIBESYS_K8S_BUILDER` and pushes
   `$VIBESYS_K8S_REGISTRY/hotel-k8s/hotel:<tree hash>`.
2. Creates namespace `hk8s-<random>`, applies `k8s/manifest.yaml` with `${NAMESPACE}` and
   `${HOTEL_IMAGE}` substituted, and waits for all Deployments to be Available.
3. Starts a `kubectl port-forward` to `service/frontend` on a free local port (supervised, so
   it comes back after the oracle restarts pods), and waits until `/user`, `/recommendations`
   and `/hotels` answer 200.
4. `check`: runs `hotel-correctness --mode accuracy` against the forwarded port; its crash and
   start events scale the stateless services and `memcached-reserve` to 0 and back through
   `run.py ctl`. Exit 0 is a pass.
   `benchmark`: builds a load-generator image (busybox + `servicebench` + `workload/`) and runs two
   Kubernetes Jobs inside the namespace against `http://frontend:5000`: `closed` (operations per
   second) then `open` (p99 latency at the declared rate). One protocol-2 result carries both.
5. Deletes the namespace on every exit path the process can see: normal end, error, SIGTERM,
   SIGINT and SIGHUP. The CLI ends a cancelled or timed-out job with SIGKILL (host: the process
   group; docker: `docker rm --force`), which no handler can see, so the namespace leaks. Each
   namespace therefore carries labels `vibesys.dev/hotel-k8s=1`, `vibesys.dev/hotel-k8s-created=<epoch>`
   and `vibesys.dev/hotel-k8s-job=<job id>`, and every job first deletes the task's `hk8s-*`
   namespaces older than the larger of the manifest's `accuracy` and `benchmark` `timeout_seconds`
   plus 10 minutes: older than any live job can be, so concurrent jobs are never touched.
   `run.py sweep` runs only that sweep.

## Environment contract

| Variable | Meaning |
| --- | --- |
| `KUBECONFIG` | Kubeconfig of the cluster; its server address must be reachable from the job's network namespace (host networking). |
| `VIBESYS_K8S_REGISTRY` | Registry as both BuildKit and the cluster's nodes pull it, for example `localhost:5001` (plain http). |
| `VIBESYS_K8S_BUILDER` | BuildKit address, for example `tcp://127.0.0.1:1234`. |
| `VIBESYS_ARTIFACTS_DIR` | Optional. Phase logs, summaries and streams of a benchmark are copied there. |

## Resource limits

Requests equal limits on every container (Guaranteed QoS). Application pods: 1 CPU, 2 GiB.
MongoDB: 2 CPU, 2 GiB each. memcached: 1 CPU, 1 GiB each. consul: 1 CPU, 512 MiB. jaeger:
2 CPU, 1 GiB. The load generator pod: 6 CPU, 4 GiB. The total demand is about 38 CPUs of
requests, so the cluster's node must have that much free, and a concurrent job needs that again.

## Manifest provenance

`k8s/manifest.yaml` is the Helm-rendered manifest of
`hotelReservation/helm-chart/hotelreservation` at DeathStarBench
`867806e575e1f7fb24437ae969910ddb17a76121`, as hand-edited in vibesys's
`examples/microservices/hotel-correctness/.vibesys/tasks/kubernetes/manifest.yaml`, with
these further edits: images pinned by tag and digest (consul 1.15.4, jaeger 1.57,
memcached 1.6.7, mongo 5.0), the application image is `${HOTEL_IMAGE}`, resources as above,
`terminationGracePeriodSeconds: 5`, and `GOMAXPROCS` of each application container from the downward API (`limits.cpu`). Without it the base measured about 345 operations/s with 100 ms latency steps (CFS throttling: Go 1.21 sees every core of the node), with it about 925. Service configs keep stock values
(`GC=100`, `JAEGER_SAMPLE_RATIO=0.01`).

## Not in the manifest yet

`vibesys.input.toml` should declare that the jobs need the `kubernetes` resource
(`[environment] needs = ["kubernetes"]`) once the CLI accepts it; today's CLI refuses the key.
