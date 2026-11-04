# hotel-k8s task files

Task-owned inputs for the Hotel Reservation Kubernetes task. `OBJECTIVE.md` is for
the optimizing agents; this file is for whoever maintains the task. Nothing here
is a candidate input: the CLI versions these files as the task.

| Path | What |
| --- | --- |
| `vibesys.input.toml`, `objectives.toml` | Manifest and the two Pareto objectives. |
| `Dockerfile` | Job image: kubectl 1.34, buildctl, Go, Python 3, and `loadgen`, `seed` and `hotel-correctness` built from `evaluator/` against a clone of the vibesys repository at commit `de037943cb6a8e6e73d00e8bc19fa53570558583` (the generic servicebench engine). Base images are pinned by digest; the build downloads the module's dependencies (mongo-driver) from the Go proxy. |
| `run.py` | Trusted wrapper: `check`, `benchmark`, and `ctl` (the oracle's crash/start commands). |
| `k8s/manifest.yaml` | The application: 19 Deployments, 19 Services, 8 ConfigMaps. |
| `evaluator/` | One Go module holding all Hotel knowledge; it imports only vibesys's generic engine packages, and its `go.mod` replaces them by the Dockerfile's clone at `/src`. `internal/dataset` generates the task's data from `dataset.Seed` (see Dataset below). `cmd/seed` (`internal/seed`) writes it into MongoDB. `internal/appsupport/hotel` holds `Reference`, the pinned application's read answers over the dataset (including an operation-for-operation copy of go-geoindex's approximate nearest search, pinned against the real library by `testdata/geoindex_golden.json`), plus topology and preflight probes. `cmd/loadgen` is servicebench (same flags) with the task's Hotel adapter `internal/apps/hotel` (request distributions, expected answers). `cmd/hotel-correctness` and `internal/accuracyapps/hotel` are the semantic oracle, first copied from `examples/microservices/hotel-correctness/.vibesys/tasks/compose/evaluator` at the vibesys commit above. |
| `workload/closed.toml` | Closed loop, concurrency 32, the canonical mixed workload; also the oracle's workload. Objective `operations_per_second`. |
| `workload/open.toml` | The same mix, open loop at a fixed offered rate; objective `latency_ms.p99`. |

## What a job does

1. Builds `hotelReservation/` with `buildctl` against `$VIBESYS_K8S_BUILDER` and pushes
   `$VIBESYS_K8S_REGISTRY/hotel-k8s/hotel:<tree hash>`; builds and pushes the evaluator image
   (busybox + `loadgen` + `seed` + `workload/`).
2. Creates namespace `hk8s-<random>` and applies `k8s/manifest.yaml` with `${NAMESPACE}` and
   `${HOTEL_IMAGE}` substituted, in two parts: first the data tier and configuration (ConfigMaps,
   Services, MongoDB, memcached, consul, jaeger). Once the MongoDB Deployments are Available it
   runs the Job `seed` (2 CPU, 1 GiB, at most 10 minutes), which writes the task's dataset into
   the six databases (drop, insert, indexes, count check) and prints the dataset and documents
   digests (`seed.log`, `seed.summary.json` in the artifacts). Only then it applies the eight
   application Deployments and waits for all Deployments to be Available. The services find
   their collections non-empty and keep them (see Base-application changes).
3. Starts a `kubectl port-forward` to `service/frontend` on a free local port (supervised, so
   it comes back after the oracle restarts pods), and waits until `/user`, `/recommendations`
   and `/hotels` answer 200.
4. `check`: runs `hotel-correctness --mode accuracy` against the forwarded port; its crash and
   start events scale the stateless services and `memcached-reserve` to 0 and back through
   `run.py ctl`. Exit 0 is a pass.
   `benchmark`: runs two load-generator Jobs inside the namespace against `http://frontend:5000`:
   `closed` (operations per second) then `open` (p99 latency at the declared rate). One
   protocol-2 result carries both.
5. Deletes the namespace on every exit path the process can see: normal end, error, SIGTERM,
   SIGINT and SIGHUP. The CLI ends a cancelled or timed-out job with SIGKILL (host: the process
   group; docker: `docker rm --force`), which no handler can see, so the namespace leaks. Each
   namespace therefore carries labels `vibesys.dev/hotel-k8s=1`, `vibesys.dev/hotel-k8s-created=<epoch>`
   and `vibesys.dev/hotel-k8s-job=<job id>`, and every job first deletes the task's `hk8s-*`
   namespaces older than the larger of the manifest's `accuracy` and `benchmark` `timeout_seconds`
   plus 10 minutes: older than any live job can be, so concurrent jobs are never touched.
   `run.py sweep` runs only that sweep.

## Dataset

`evaluator/internal/dataset` generates everything the databases hold, as a pure function of
`dataset.Seed` (fixed per task version; changing it changes the task). The package doc records
the choices; in short:

| Collection | Documents | BSON |
| --- | --- | --- |
| `geo-db.geo` | 25,000 | 1.2 MB |
| `profile-db.hotels` | 25,000 | 10.7 MB |
| `rate-db.inventory` | 66,408 | 14.5 MB |
| `recommendation-db.recommendation` | 25,000 | 2.0 MB |
| `reservation-db.number` | 25,000 | 1.0 MB |
| `reservation-db.reservation` | 0 (index only) | 0 |
| `user-db.user` | 10,000 | 1.1 MB |

- 20 real cities; hotels distributed over them by Zipf rank (London 6,949 ... Rio de Janeiro
  347), around each centre with normal offsets (sigma 1.5-4 km, 15% at 2.5 sigma); unique
  coordinates with 6 decimals; IDs `1`..`25000` in a random order of cities.
- Profiles: name, phone, address and description from per-city templates; float32 coordinates
  (the profile service refuses to truncate float64).
- 1-5 rate plans per hotel (mean 2.66): `RACK` over the whole 2027 rate window, other plans over
  14-180 days of it; prices by city, star class and plan; tax-inclusive rates.
- Recommendation attributes: rating 1 + 4 Beta(5, 2) at 0.01 (one hotel has the top rating),
  price = the cheapest plan (one hotel is the cheapest); capacities 10-800 rooms by star class;
  a popularity rank per hotel within its city.
- 10,000 users with unique names and random 10-16 character passwords (stored as SHA-256).
- Indexes on the services' query keys (`id`, `hotelId`, hotel+night, `username`).
- Digests: dataset `7fcac12b...` (`internal/dataset`), documents `7670033f...`
  (`internal/seed`); both are pinned by tests and printed by the seed Job.

Canonical requests (`internal/apps/hotel/operations.go`) are deterministic functions of each
sample's random word: locations by city popularity (Zipf, s = 1) with the hotels' spread around
the centre (5 decimals), stays of 1-7 nights skewed short over the rate window, logins and
reservation users by Zipf (s = 0.75) over the users, reservations by hotel popularity on
trial-owned far-future nights. Over 100,000 samples an exact-request cache hits 0.45% of the mix.

## Base-application changes

Two changes to `hotelReservation/` make the base work with a task-owned dataset; they are part
of the base every candidate starts from:

- `cmd/*/db.go`: a service inserts its built-in test data only into an empty collection
  (reservation-db is keyed on its capacity collection).
- `services/rate`: on a cache miss, GetRates reads the missed hotel's plans instead of the
  whole inventory, which made every search return every hotel that has a plan.

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
