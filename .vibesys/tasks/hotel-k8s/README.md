# hotel-k8s task files

Task-owned inputs for the Hotel Reservation Kubernetes task. `OBJECTIVE.md` is for
the optimizing agents; this file is for whoever maintains the task. Nothing here
is a candidate input: the CLI versions these files as the task.

| Path | What |
| --- | --- |
| `vibesys.input.toml`, `objectives.toml` | Manifest and the two Pareto objectives. |
| `Dockerfile` | Job image: kubectl 1.34, buildctl, Go, Python 3, and `loadgen`, `seed`, `hotel-correctness` and `podctl` built from `evaluator/` against a clone of the vibesys repository at commit `de037943cb6a8e6e73d00e8bc19fa53570558583` (the generic servicebench engine). Base images are pinned by digest; the build downloads the module's dependencies (mongo-driver) from the Go proxy. |
| `run.py` | Trusted wrapper: `check`, `benchmark`, `sweep`. `tests/test_run.py` tests its manifests and seeds (`python3 -m unittest discover -s tests`). |
| `k8s/manifest.yaml` | The application: 19 Deployments, 19 Services, 8 ConfigMaps. |
| `evaluator/` | One Go module holding all Hotel knowledge; it imports only vibesys's generic engine packages, and its `go.mod` replaces them by the Dockerfile's clone at `/src`. `internal/dataset` generates the task's data from `dataset.Seed` (see Dataset below). `cmd/seed` (`internal/seed`) writes it into MongoDB. `internal/appsupport/hotel` holds `Reference`, the pinned application's read answers over the dataset (including an operation-for-operation copy of go-geoindex's approximate nearest search, pinned against the real library by `testdata/geoindex_golden.json`), plus topology and preflight probes. `cmd/loadgen` is servicebench (same flags) with the task's Hotel adapter `internal/apps/hotel` (request distributions, expected answers). `cmd/hotel-correctness` and `internal/accuracyapps/hotel` are the semantic oracle, first copied from `examples/microservices/hotel-correctness/.vibesys/tasks/compose/evaluator` at the vibesys commit above. `cmd/podctl` (`internal/podctl`) restarts and waits for pods through the Kubernetes API from inside the namespace. |
| `workload/closed.toml` | Closed loop, concurrency 32, the canonical mixed workload; also the oracle's workload (with `strict_linearizable_capacity`). Objective `operations_per_second`. |
| `workload/open.toml` | The same mix, open loop at a fixed offered rate; objective `latency_ms.p99`. |

## What a job does

1. Builds `hotelReservation/` with `buildctl` against `$VIBESYS_K8S_BUILDER` and pushes
   `$VIBESYS_K8S_REGISTRY/hotel-k8s/hotel:<tree hash>`; builds and pushes the evaluator image
   (busybox + `loadgen` + `seed` + `hotel-correctness` + `podctl` + `workload/`).
2. Creates namespace `hk8s-<random>` and applies `k8s/manifest.yaml` with `${NAMESPACE}` and
   `${HOTEL_IMAGE}` substituted, in two parts: first the data tier and configuration (ConfigMaps,
   Services, MongoDB, memcached, consul, jaeger), and the service account `evaluator` (a Role
   that may get/list/watch/delete pods and get/list Deployments of the namespace, nothing else;
   the application runs as the namespace's default account). Once the MongoDB Deployments are
   Available it runs the Job `seed` (2 CPU, 1 GiB, at most 10 minutes), which writes the task's
   dataset into the six databases (drop, insert, indexes, count check) and prints the dataset and
   documents digests (`seed.log`, `seed.summary.json` in the artifacts). Only then it applies the
   eight application Deployments (two replicas each) and waits for all Deployments to be Available
   (with two replicas that needs both). The services find their collections non-empty and keep
   them (see Base-application changes).
3. Runs the Job `ready` (`podctl ready`, `ready.log`): every replica of every application
   Deployment Ready, every pod of the seven gRPC services registered in consul at its pod IP
   (`/v1/agent/services`), and every frontend pod answering `/user`, `/recommendations` and
   `/hotels` with 200 at its own IP.
4. `check`: draws the oracle's seed with `secrets` (`seeds.json`) and runs the Job `oracle`
   (`hotel-correctness --mode accuracy`, 2 CPU, 2 GiB, `oracle.log`) against
   `http://frontend:5000`. Every oracle request asks for its own connection (`Connection: close`),
   so kube-proxy spreads the reads, the repeated reads of the visibility checks included, over
   both frontend pods; internal gRPC round-robins over consul. Its restart hook
   (`--restart-command-json`, see below) is `podctl ... restart`. Exit 0 is a pass. Afterwards
   the job logs every application pod's CPU seconds and network bytes from the kubelet summary
   (`pod-usage-check.json`): pods replaced by the last restart show what they served since.
   `benchmark`: draws fresh seeds (below), runs two load-generator Jobs inside the namespace
   against `http://frontend:5000`: `closed` (operations per second) then `open` (p99 latency at
   70% of the closed loop's throughput), logging per-pod usage after each. One protocol-2 result
   carries both.
5. Deletes the namespace on every exit path the process can see: normal end, error, SIGTERM,
   SIGINT and SIGHUP. The CLI ends a cancelled or timed-out job with SIGKILL (host: the process
   group; docker: `docker rm --force`), which no handler can see, so the namespace leaks. Each
   namespace therefore carries labels `vibesys.dev/hotel-k8s=1`, `vibesys.dev/hotel-k8s-created=<epoch>`
   and `vibesys.dev/hotel-k8s-job=<job id>`, and every job first deletes the task's `hk8s-*`
   namespaces older than the larger of the manifest's `accuracy` and `benchmark` `timeout_seconds`
   plus 10 minutes: older than any live job can be, so concurrent jobs are never touched.
   `run.py sweep` runs only that sweep.

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

## Seeds

Every benchmark sample draws, with `secrets`, a workload seed per phase and the closed phase's
fixture seed F, below 2^62; the open phase's fixture seed is F + 1. They go to the loadgen Jobs as
`--seed`/`--fixture-seed` and into `seeds.json`; `loadgen --seed S --fixture-seed F` with the
phase's workload replays a phase. The `seed`/`fixture_seed` in `workload/*.toml` are for local
development only. The check records the oracle's `--seed` in its `seeds.json`.

Both phases run in one namespace and reservations cannot be deleted, so their fixtures must be
disjoint. `TrialSlot` (`internal/apps/hotel/application.go`) places them without a hash: the run's
fixture seed (the engine passes trial fixture seed = F + trial index) selects run slot F mod 303,
the trial index (at most 16 trials per run) the slot within it; 303 x 16 slots fit the 4850
blocks of 512 nights in years 3000-9799. The slot gives the trial's reservation block, its
additions' sandbox cell (slot x 6, of about 31,000 cells) and the high bits of its added hotels'
ID tag. Fixture seeds F and F + 1 therefore never share a night, a cell or an ID, by construction;
`fixtures_test.go` checks it for random F and every trial of both phases.

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

## Writes in the workload

About 1% of the mix (104 of 10,504 weight) are owner writes (`internal/apps/hotel/writes.go`),
plus `hotel_rates` reads of `GET /rates` (4%) that make rate writes visible:

| Operation | Weight | Target and value |
| --- | --- | --- |
| `update_rate` | 42 | a hotel by popularity, one of its plans; prices x0.8-1.25 of the seeded ones |
| `update_profile` | 42 | a hotel by popularity; a new phone, and in 10% of the updates a new name |
| `set_capacity` | 12 | a capacity-managed hotel (one in four, never booked by `reserve_capacity`); rooms between C/2 and 2C, then a reservation of rooms+1 that must be refused |
| `add_hotel` | 8 | a hotel in the trial's sandbox (below) |

Validation stays exact with an acceptance window (`history.go`): for each item the adapter
keeps every write with the time its plan was built and the time its response was validated.
A read whose plan was built at `start` and validated at `end` may show the latest value
acknowledged more than 1 s before `start` (the seeded value if none), or any later write
whose plan was built before `end`. A write that failed counts as possibly applied. Writes of
one item are serialized (a plan that draws a busy item draws again), so "the latest" is well
defined.

Sandboxes keep additions from changing seeded answers: about 31,000 cells of 25 km in the
South Pacific (lat -58..-20, lon -165..-85), more than 1,000 km from every seeded hotel. Each
trial owns a cell and an ID tag (both from its trial slot, see Seeds; IDs `a<hex>-<n>`, never
numeric, so never a seeded ID) and adds hotels within 2 km of the cell's centre. Once it holds a visible addition, 3% of the trial's searches and distance
recommendations query the sandbox; the answer must equal the reference answer for the
acknowledged additions plus any subset of the ones still in their window.

The closed and open phases are separate load-generator processes in one namespace, so each
trial's `Reset` restores what it changed (prices, profiles and capacities back to the seeded
values, through the same routes); additions stay in their sandbox, which the next process's
trials do not reuse.

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
| `VIBESYS_ARTIFACTS_DIR` | Optional. Phase logs, summaries and streams of a benchmark are copied there. |

## Resource limits

Requests equal limits on every container (Guaranteed QoS). Application pods: 16 (8 services x 2),
1 CPU, 2 GiB each. MongoDB: 2 CPU, 2 GiB each. memcached: 1 CPU, 1 GiB each. consul: 1 CPU,
512 MiB. jaeger: 2 CPU, 1 GiB. The namespace's deployments request 34 CPUs and 48.5 GiB. Jobs:
seed 2 CPU (before the application starts), ready 0.5 CPU, oracle 2 CPU, load generator 6 CPU,
4 GiB. Peak demand is 40 CPUs (benchmark) or 36 (check), on a 64-CPU node shared with other
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

## Not in the manifest yet

`vibesys.input.toml` should declare that the jobs need the `kubernetes` resource
(`[environment] needs = ["kubernetes"]`) once the CLI accepts it; today's CLI refuses the key.
