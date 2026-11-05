# hotel-k8s task files

Task-owned inputs for the Hotel Reservation Kubernetes task. `OBJECTIVE.md` is for
the optimizing agents; this file is for whoever maintains the task. Nothing here
is a candidate input: the CLI versions these files as the task.

| Path | What |
| --- | --- |
| `vibesys.input.toml`, `objectives.toml` | Manifest and the two Pareto objectives. |
| `Dockerfile` | Task image (every job and every in-cluster pod of the task runs it): vibesys-evaluator (Python package, `loadgen`, kubectl 1.34, buildctl) built from vibesys commit `e7da9610dfce08c7261895df9b01bfadb3f46314` until the image is published, pymongo, and `hotel-correctness` and `podctl` built from `evaluator/` against a clone of vibesys at `de037943cb6a8e6e73d00e8bc19fa53570558583` (the servicebench accuracy framework). Base images are pinned by digest. |
| `dataset.py` | The single owner of the data: generates it from `SEED` (see Dataset), the MongoDB documents, the values writes put in, and the reference answers of the read paths (an operation-for-operation copy of go-geoindex's approximate nearest search). |
| `bench.py` | The benchmark, on vibesys-evaluator: system under test, seed, workload (operations, distributions, checks), `benchmark(run)` and the validity rules. |
| `run.py` | Trusted wrapper of the accuracy check: `check`, `sweep`. |
| `tests/` | `python -m unittest discover -s tests` (needs vibesys-evaluator and pymongo): `test_dataset.py` pins the documents' digest to the earlier Go seed's and the reference to the real go-geoindex's answers; `test_run.py` tests run.py's Jobs and the manifest. |
| `k8s/manifest.yaml` | The application: 19 Deployments (the data tier labelled `tier: data`), 19 Services, 8 ConfigMaps. |
| `evaluator/` | Go module of the accuracy check. `internal/dataset` reads dataset.py's dataset file (`--dataset` or `$HOTEL_DATASET`) and draws the oracle's own write values (`additions.go`, `vocabulary.go`). `internal/appsupport/hotel` holds `Reference`, the pinned application's read answers over the dataset (pinned against the real library by `testdata/geoindex_golden.json`, which `tests/test_dataset.py` also reads), plus topology and preflight probes. `cmd/hotel-correctness` and `internal/accuracyapps/hotel` are the semantic oracle. `cmd/podctl` (`internal/podctl`) restarts and waits for pods through the Kubernetes API from inside the namespace. Its tests need the dataset file: `python dataset.py /tmp/dataset.json; HOTEL_DATASET=/tmp/dataset.json go test ./...` (without it they skip). |
| `workload/oracle.toml` | The oracle's workload: targets, timeout, `strict_linearizable_capacity`. |

## What the benchmark does (bench.py, one sample)

vibesys-evaluator deploys `SYSTEM` (builds `hotelReservation/` and the task image in-cluster,
fresh namespace, the `tier=data` Deployments, the seed Job running `seed`, the eight application
Deployments with two replicas each, readiness probes through `frontend:5000`), then runs
`benchmark(run)`: two closed-loop phases (32 clients, 15 s warmup, 60 s; throughput = their
median) and three open-loop phases at 0.7 x that throughput (uniform arrivals, 10 s warmup,
60 s; p99 = their median). Each phase is a pod of the task image: it generates the phase's
requests from the dataset and the sample's seed into a file, runs loadgen over it, then checks
every write and 10% of the reads (the validation sample) after the window. A sample fails on any
loadgen error or failed check in a window, an operation of the mix that never completed, or an
open loop below 95% of its rate. The sample seed is drawn per sample and recorded
(`seeds.json`; `bench.py benchmark --seed N` replays one). Writes carry from phase to phase
(each phase replays the earlier phases' writes into its model), so nothing is reset.

## What the check does (run.py)

1. Builds `hotelReservation/` and the task image with `buildctl` against `$VIBESYS_K8S_BUILDER`
   and pushes them to `$VIBESYS_K8S_REGISTRY/hotel-k8s/{hotel,task}:<tree hash>`.
2. Creates namespace `hk8s-<random>` and applies `k8s/manifest.yaml` with `${NAMESPACE}` and
   `${HOTEL_IMAGE}` substituted, in two parts: first the data tier and configuration (ConfigMaps,
   Services, MongoDB, memcached, consul, jaeger), and the service account `evaluator` (a Role
   that may get/list/watch/delete pods and get/list Deployments of the namespace, nothing else;
   the application runs as the namespace's default account). Once the MongoDB Deployments are
   Available it runs the Job `seed` (`bench.py seed`, as a benchmark sample seeds: drop, insert,
   indexes, count check; `seed.log`). Only then it applies the eight application Deployments
   (two replicas each) and waits for all Deployments to be Available. The services find their
   collections non-empty and keep them (see Base-application changes).
3. Runs the Job `ready` (`podctl ready`, `ready.log`): every replica of every application
   Deployment Ready, every pod of the seven gRPC services registered in consul at its pod IP
   (`/v1/agent/services`), and every frontend pod answering `/user`, `/recommendations` and
   `/hotels` with 200 at its own IP.
4. Draws the oracle's seed with `secrets` (`seeds.json`) and runs the Job `oracle` (2 CPU,
   2 GiB, `oracle.log`): `python dataset.py /tmp/dataset.json`, then
   `hotel-correctness --mode accuracy --dataset /tmp/dataset.json` against
   `http://frontend:5000`. Every oracle request asks for its own connection (`Connection: close`),
   so kube-proxy spreads the reads over both frontend pods; internal gRPC round-robins over
   consul. Its restart hook (`--restart-command-json`, see below) is `podctl ... restart`. Exit 0
   is a pass.
5. Deletes the namespace on every exit path the process can see: normal end, error, SIGTERM,
   SIGINT and SIGHUP. The CLI ends a cancelled or timed-out job with SIGKILL, which no handler
   can see, so the namespace leaks. Each namespace therefore carries labels
   `vibesys.dev/hotel-k8s=1`, `vibesys.dev/hotel-k8s-created=<epoch>` and
   `vibesys.dev/hotel-k8s-job=<job id>`, and every check first deletes the task's `hk8s-*`
   namespaces older than the larger of the manifest's `timeout_seconds` plus 10 minutes.
   `run.py sweep` runs only that sweep. (The benchmark's namespaces are vibesys-evaluator's,
   which sweeps its own the same way.)

## Restart hooks

The engine's managed lifecycle stops a whole candidate and waits until it is unreachable, which
a restart of one replica behind a Service never is, so the oracle takes its own hook:
`hotel-correctness --restart-command-json '[...]'` (the flag is removed before servicebench
parses the rest). The oracle runs the command with `one` or `all` appended; exit 0 means every
replacement serves (as in step 3 above). `podctl restart one` deletes, with a 1 s grace period,
the first pod by name of each of the eight application Deployments and `memcached-reserve` (its
only pod), all at once; `podctl restart all` deletes every pod of them. Both wait until no
deleted pod exists, each Deployment has exactly its replica count of Ready pods, the gRPC pods
are in consul and each frontend pod passes the probes; `podctl:` lines in `oracle.log` name
the deleted and the serving pods.

| Check | Restart |
| --- | --- |
| `crash_recovery` | `all`: a filled night stays full after every instance and the cache restart. |
| `durable_capacity` | `one`: reservations survive while a peer of each restarted instance runs. |
| `durable_writes` | `one`, every write read back 8 times, then `all`, read back again: a restarted instance must not serve older data than its peer, and no write may live only in running instances' memory. |

Without the flag the checks fall back to the engine's `--run-command-json`/`--stop-command-json`
crash and start (`check_test.go` runs both ways).

## Dataset

`dataset.py` generates everything the databases hold, as a pure function of `SEED` (fixed per
task version; changing it changes the task). It equals the task's earlier Go generator draw for
draw: the MongoDB documents' digest is `7670033f...` for both (`tests/test_dataset.py`).

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
- Recommendation attributes: rating 1 + 4 Beta(5, 2) at 0.01, price = the cheapest plan;
  capacities 10-800 rooms by star class; a popularity rank per hotel within its city.
- 10,000 users with unique names and random 10-16 character passwords (stored as SHA-256).
- Indexes on the services' query keys (`id`, `hotelId`, hotel+night, `username`).

Requests (`bench.py`) draw from the operation's own stream: locations by city popularity
(Zipf, s = 1) with the hotels' spread around the centre (5 decimals; a search redraws where
geo's own ranking ties), stays of 1-7 nights skewed short over the rate window, logins and
reservation users by Zipf (s = 0.75) over the users, rates, reservations and writes by hotel
popularity; reservations on far-future nights of a block the sample draws, at most 128 distinct
hotels a night, serialized (`exclusive`: the service's capacity check is not transactional).

## Writes in the workload

About 1% of the mix (104 of 10,504 weight) are owner writes, plus `hotel_rates` reads of
`GET /rates` (4%) that make rate writes visible:

| Operation | Weight | Target and value |
| --- | --- | --- |
| `update_rate` | 42 | a hotel by popularity, one of its plans; prices x0.8-1.25 of the seeded ones |
| `update_profile` | 42 | a hotel by popularity; a new phone, and in 10% of the updates a new name |
| `set_capacity` | 12 | a capacity-managed hotel (one in four, never booked by `reserve_capacity`); rooms between C/2 and 2C, then a reservation of rooms+1 that must be refused |
| `add_hotel` | 8 | a hotel in the sample's sandbox cell (below) |

Checks stay exact with `Versions` (the 1 s bound of OBJECTIVE.md): a read over [start, end]
may show, per item, the latest value acknowledged more than 1 s before it started (the seeded
value if none) or any value whose write started before it ended; a failed write counts as
possibly applied. Writes of one item never overlap (`exclusive`), so its versions are in
application order. Sandboxes keep additions from changing seeded answers: about 31,000 cells
of 25 km in the South Pacific, more than 1,000 km from every seeded hotel; a sample adds its
hotels within 2 km of one cell's centre (IDs `a<hex>-<n>`), and 3% of the searches and
distance recommendations of later phases query near an addition acknowledged earlier; the
answer must equal the reference answer for the visible additions plus any subset of the ones
still in their window.

## Write checks in the oracle

`internal/accuracyapps/hotel/writes.go` runs after every other check (it changes data they
expect): `write_validation` (405/400/404 answers change nothing), `write_rate_visibility`,
`write_profile_visibility`, `write_capacity` and `write_hotel_addition`, then, with the
restart hooks, `durable_writes`. Each visibility check reads right after the
acknowledgement (old or new value allowed), then at 1 s + 50 ms eight or more times, each
on a new connection, so kube-proxy and round robin reach every instance of a two-replica
service. Capacity: a decrease below a
night's reserved rooms is 409 and changes nothing, a decrease to them or an increase applies
to the next reservations (probed eight times), capacity 0 hides a hotel from search. An added
hotel (rated above and priced below every seeded hotel) must appear in search, all three
recommendations, `/rates` and reservations. `durable_writes` restarts one instance of the
stateless pods and the reservation cache, reads every write again, then restarts all and
reads again. The tests' fake application (`check_test.go`) runs two replicas; a replica with
a never-invalidated profile, rate or capacity cache, a geo index that never loads additions,
a capacity write that oversells, a restart that loses writes, or writes kept only in the
instances' memory (lost only when all restart) each fails the check.

## Base-application changes

These changes to `hotelReservation/` are part of the base every candidate starts from:

- `cmd/*/db.go`: a service inserts its built-in test data only into an empty collection
  (reservation-db is keyed on its capacity collection).
- `services/rate`: on a cache miss, GetRates reads the missed hotel's plans instead of the
  whole inventory, which made every search return every hotel that has a plan.
- `services/reservation`: CheckAvailability read cached capacities under the wrong key and
  never ran its MongoDB fallbacks, so search hid hotels with rooms free (fix with a test).
- The write path: `services/frontend/admin.go` (routes above), new RPCs `UpdateRate`/`AddRates`,
  `UpdateProfile`/`AddProfile`, `SetCapacity`/`AddCapacity`, `geo.AddHotel`,
  `recommendation.AddHotel` (protos regenerated with protoc 25.0 and the
  protoc-gen-go/-grpc versions named in the generated headers). Each owner writes MongoDB, then deletes the memcached key.
- `lease/`: cache fills take a memcached lease (`add` a placeholder, then `cas` the value),
  so a fill that read MongoDB before a write cannot overwrite the write's invalidation.
- `changes/`: geo and recommendation keep their in-memory catalog, now under a RWMutex; an
  addition inserts the document (`added: true`) and bumps a version counter in
  `<db>.changes`, and every replica polls the counter every 200 ms and reloads the added
  documents when it moved. Without writes this costs five point reads a second per replica.
- `internal/memcfake` (an in-process memcached for tests), `internal/writetest` (end-to-end
  write tests of the services against a MongoDB given by `HOTEL_TEST_MONGO`).
- Not-found profiles are skipped instead of returned as null and cached.
- `services/reservation`: reserved rooms are counted per hotel and night in
  `reservation-db.nights` (unique index on hotelId + night, created at startup), and a night
  is taken by a conditional increment (`rooms <= capacity - n`), so MongoDB serializes
  concurrent reservations across instances; a stay that does not fit gives back the nights it
  took, and the cached counts are invalidated. Before, every request read the count
  (memcached, or a sum of reservation documents), compared and wrote: two replicas
  acknowledged 40 one-room reservations of a 10-room night
  (`internal/writetest.TestConcurrentReservationsAcrossReplicas`). A capacity change is still
  not ordered with reservations of the same hotel that run concurrently with it (the workload
  never does that: capacity writes go to hotels it never books).

## State per instance

With two instances of every service, per-process state was reviewed: `geo` and
`recommendation` keep their catalog in memory and poll `<db>.changes` every 200 ms for
additions (step 2); `user` loads the users once at startup, which stays correct because no
route changes users; `search` and `frontend` keep no state between requests; `profile`,
`rate` and `reservation` cache in the shared memcached (lease-guarded fills), with MongoDB as
the source of truth (reservation counts now included, above).

## Environment contract

| Variable | Meaning |
| --- | --- |
| `KUBECONFIG` | Kubeconfig of the cluster; its server address must be reachable from the job's network namespace (host networking). |
| `VIBESYS_K8S_REGISTRY` | Registry as both BuildKit and the cluster's nodes pull it, for example `localhost:5001` (plain http). |
| `VIBESYS_K8S_BUILDER` | BuildKit address, for example `tcp://127.0.0.1:1234`. |
| `VIBESYS_ARTIFACTS_DIR` | Optional. Logs, phase results and the dataset file are kept there. |

## Resource limits

Requests equal limits on every container (Guaranteed QoS). Application pods: 16 (8 services x 2),
1 CPU, 2 GiB each. MongoDB: 2 CPU, 2 GiB each. memcached: 1 CPU, 1 GiB each. consul: 1 CPU,
512 MiB. jaeger: 2 CPU, 1 GiB. The namespace's deployments request 34 CPUs and 48.5 GiB. Pods
of the jobs: seed 4 CPU (before the application starts), ready/probe 0.5 CPU, oracle 2 CPU,
phase (loadgen) 6 CPU, 4 GiB. Peak demand is 40 CPUs (benchmark) or 36 (check), on a 64-CPU node shared with other
tasks' jobs, so two of these jobs cannot run at once.

## Manifest provenance

`k8s/manifest.yaml` is the Helm-rendered manifest of
`hotelReservation/helm-chart/hotelreservation` at DeathStarBench
`867806e575e1f7fb24437ae969910ddb17a76121`, as hand-edited in vibesys's
`examples/microservices/hotel-correctness/.vibesys/tasks/kubernetes/manifest.yaml`, with
these further edits: images pinned by tag and digest (consul 1.15.4, jaeger 1.57,
memcached 1.6.7, mongo 5.0), the application image is `${HOTEL_IMAGE}`, two replicas of each application Deployment, resources as above,
`terminationGracePeriodSeconds: 5`, and `GOMAXPROCS` of each application container from the downward API (`limits.cpu`). Without it the base measured about 345 operations/s with 100 ms latency steps (CFS throttling: Go 1.21 sees every core of the node), with it about 925. Service configs keep stock values
(`GC=100`, `JAEGER_SAMPLE_RATIO=0.01`).
