# hotel-k8s task files

Task-owned inputs for the Hotel Reservation Kubernetes task. `OBJECTIVE.md` is for
the optimizing agents; this file is for whoever maintains the task. Nothing here
is a candidate input: the CLI versions these files as the task.

| Path | What |
| --- | --- |
| `vibesys.input.toml`, `objectives.toml` | Manifest and the two Pareto objectives. |
| `Dockerfile` | Task image (every job and every in-cluster pod of the task runs it): vibesys-evaluator (Python package, `loadgen`, kubectl 1.34, buildctl) built from the vibesys commit `VIBESYS_COMMIT` until the image is published, pymongo, and from `evaluator/`: `hotel-generator` (against the Go SDK of that same commit) and `hotel-correctness` and `podctl` (against a clone of vibesys at `ORACLE_VIBESYS_COMMIT`, the servicebench accuracy framework). Base images are pinned by digest. |
| `dataset.py` | The single owner of the data: generates it from `SEED` (see Dataset) and the MongoDB documents. The dataset file it encodes is what the Go programs read. |
| `bench.py` | The benchmark's orchestration, on vibesys-evaluator: system under test, seed, `prepare` (the capacity envelope), `benchmark(run)` and the validity rules. Its workload is `Generator(["hotel-generator"])`: the Go generator of `evaluator/cmd/generator`. |
| `envelope.py` | The application tier's capacity envelope: validates the candidate's `hotelReservation/deploy/app.yaml` (see Capacity envelope) and composes the deployed manifest. Both run.py and bench.py call it before they build anything. |
| `run.py` | Trusted wrapper of the accuracy check: `check`, `sweep`. |
| `tests/` | `python -m unittest discover -s tests` (needs vibesys-evaluator and pymongo): `test_dataset.py` pins the documents' digest to the earlier Go seed's; `test_run.py` tests run.py's Jobs; `test_envelope.py` the validator (every rule, sums at and over the envelope, random splits) and bench.py's `prepare`; `testdata/regrouped.yaml` is a regrouped tier within the envelope. |
| `k8s/manifest.yaml` | The task's part: the data tier's 11 Deployments (labelled `tier: data`) and 12 Services, the `frontend` Service and the services' 8 ConfigMaps. |
| `k8s/app.yaml` | The reference application tier: 8 Deployments (2 pods, 1 CPU / 2 GiB each) and the 7 gRPC Services, as the base's `hotelReservation/deploy/app.yaml` has them; used for a candidate without that file (one made before the envelope). |
| `k8s/deployed.yaml` | Not committed: the composed manifest (`k8s/manifest.yaml` and the candidate's tier) that bench.py writes before vibesys-evaluator reads it (`k8s.App(manifest=)` names one file of the task directory). `benchmark` first re-runs bench.py from a writable copy of the task directory, where it writes this file. |
| `evaluator/` | Go module of the benchmark's workload and the accuracy check. `cmd/generator` (`internal/hotelworkload`) is the benchmark's request generator and its checks, on the vibesys-evaluator Go SDK (`loadgen/workload`) over the oracle's model; its `TestTheStreamIsThePythonWorkloads` pins its request stream to the one the earlier Python workload wrote (`testdata/`). `internal/dataset` reads dataset.py's dataset file (`--dataset` or `$HOTEL_DATASET`) and draws the oracle's own write values (`additions.go`, `vocabulary.go`). `internal/appsupport/hotel` holds `Reference`, the pinned application's read answers over the dataset (pinned against the real library by `testdata/geoindex_golden.json`), plus topology and preflight probes. `cmd/hotel-correctness` and `internal/accuracyapps/hotel` are the semantic oracle. `cmd/podctl` (`internal/podctl`) restarts and waits for pods through the Kubernetes API from inside the namespace. Its tests need the dataset file: `python dataset.py /tmp/dataset.json; HOTEL_DATASET=/tmp/dataset.json go test ./...` (without it they skip). `go.mod` replaces the vibesys modules by the image's clones (`/src`, `/vse`); a local build points them at checkouts of the two commits with a `go.work`. |
| `workload/oracle.toml` | The oracle's workload: targets, timeout, `strict_linearizable_capacity`. |

## What the benchmark does (bench.py, one sample)

bench.py first validates the candidate's application tier (`envelope.load`) and writes the
composed manifest; a violation fails the sample before anything is built, with the validator's
message. vibesys-evaluator then deploys `SYSTEM` (builds `hotelReservation/` and the task image
in-cluster, fresh namespace, the `tier=data` Deployments and every Service and ConfigMap, the
seed Job running `seed`, the candidate's Deployments, readiness probes through `frontend:5000`),
then runs
`benchmark(run)`: two closed-loop phases (32 clients, 15 s warmup, 60 s; throughput = their
median) and three open-loop phases at 0.7 x that throughput (uniform arrivals, 10 s warmup,
60 s; p99 = their median). Each phase is a pod of the task image: loadgen runs
`hotel-generator generate`, which writes the phase's request stream from the dataset, the
sample's seed and the earlier phases' writes for as long as loadgen reads it; after the window
`hotel-generator check` checks every write and 10% of the reads (the validation sample). A sample fails on any
loadgen error or failed check in a window, an operation of the mix that never completed, or an
open loop below 95% of its rate. The sample seed is drawn per sample and recorded
(`seeds.json`; `bench.py benchmark --seed N` replays one). Writes carry from phase to phase
(each phase replays the earlier phases' writes into its model), so nothing is reset.

## What the check does (run.py)

1. Validates the candidate's application tier (`envelope.load`, logged with its CPU and memory
   split); a violation ends the check here, before any build or namespace.
2. Builds `hotelReservation/` and the task image with `buildctl` against `$VIBESYS_K8S_BUILDER`
   and pushes them to `$VIBESYS_K8S_REGISTRY/hotel-k8s/{hotel,task}:<tree hash>`.
3. Creates namespace `hk8s-<random>` and applies `k8s/manifest.yaml` and then the candidate's
   tier with `${NAMESPACE}` and `${HOTEL_IMAGE}` substituted: first the data tier and
   configuration (ConfigMaps, Services, MongoDB, memcached, consul, jaeger), and the service
   account `evaluator` (a Role
   that may get/list/watch/delete pods and get/list Deployments of the namespace, nothing else;
   the application runs as the namespace's default account). Once the MongoDB Deployments are
   Available it runs the Job `seed` (`bench.py seed`, as a benchmark sample seeds: drop, insert,
   indexes, count check; `seed.log`). Only then it applies the candidate's tier (its
   Deployments, Services and ConfigMaps) and waits for all Deployments to be Available. The services find their
   collections non-empty and keep them (see Base-application changes).
4. Runs the Job `ready` (`podctl ready`, `ready.log`): every replica of every candidate
   Deployment Ready, every pod of a Deployment that registers in consul registered at its pod IP
   (`/v1/agent/services`; `--registered-auto`: a Deployment registers when any of its pods is
   listed), and every pod of the Deployments behind the `frontend` Service answering `/user`,
   `/recommendations` and `/hotels` with 200 at its own IP.
5. Draws the oracle's seed with `secrets` (`seeds.json`) and runs the Job `oracle` (2 CPU,
   2 GiB, `oracle.log`): `python dataset.py /tmp/dataset.json`, then
   `hotel-correctness --mode accuracy --dataset /tmp/dataset.json` against
   `http://frontend:5000`. Every oracle request asks for its own connection (`Connection: close`),
   so kube-proxy spreads the reads over both frontend pods; internal gRPC round-robins over
   consul. Its restart hook (`--restart-command-json`, see below) is `podctl ... restart`. Exit 0
   is a pass.
6. Deletes the namespace on every exit path the process can see: normal end, error, SIGTERM,
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
replacement serves (as in step 4 above). `podctl restart one` deletes, with a 1 s grace period,
the first pod by name of each of the candidate's Deployments (run.py passes their names) and of
`memcached-reserve` (its only pod), all at once, while the other replicas serve; `podctl restart
all` deletes every pod of them. Both wait until no deleted pod exists, each Deployment has
exactly its replica count of Ready pods, the replacements of each Deployment whose pods were in
consul before the restart are in consul, and each frontend pod passes the probes; `podctl:`
lines in `oracle.log` name the deleted and the serving pods.

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

Requests (`evaluator/internal/hotelworkload`) draw from the operation's own stream: locations by city popularity
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

- `deploy/app.yaml`: the application tier (moved from `k8s/manifest.yaml`, the reference
  topology unchanged); the Dockerfile copies named directories only, so the file does not
  change the image's build layers.
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

## Capacity envelope

The candidate owns the application tier, `hotelReservation/deploy/app.yaml` (OBJECTIVE.md,
Resources). `envelope.py` checks it before any build or deploy, collecting every violation
into one error that names the object and key (`Deployment geo: spec.replicas: 1: at least 2`):

| Rule | Where |
| --- | --- |
| CPU limits over every container of every replica at most 16, memory at most 32 GiB | `validate`, the `envelope` problems |
| `requests` == `limits`, both of `cpu` and `memory` only, at least 100m and 64 MiB, whole millicores | `_Checker.resources` |
| `replicas` an integer >= 2 | `_Checker.deployment` |
| `GOMAXPROCS` set once: ceil(CPU limit) as a literal, or `resourceFieldRef` `limits.cpu` divisor 1 of the container itself | `_Checker.env` |
| `image: ${HOTEL_IMAGE}` only; no variables but `${NAMESPACE}` and `${HOTEL_IMAGE}` | `_Checker.container`, `validate` |
| kinds `Deployment` (`apps/v1`), `Service`, `ConfigMap` (`v1`); namespace `${NAMESPACE}` or absent; no `tier`/`backend` labels | `validate`, `_Checker.metadata` |
| allowlists of Deployment, pod, container, securityContext, port, env, volume (`configMap`, `emptyDir`) and Service (`ClusterIP`, headless) fields; `affinity`, `tolerations`, `nodeSelector` empty; default service account | `*_KEYS`, `_Checker.pod`, `_Checker.service` |
| no Deployment, Service or ConfigMap name of the task's; mounted ConfigMaps exist | `_Checker.deployment`, `_Checker.service`, `validate` |
| selectors select their own pods only; no pod matches a task Service's selector except `frontend`'s; at least one Deployment behind `frontend` | `_Checker.deployment`, `validate` |

Decisions: GOMAXPROCS is the CPU limit rounded up, because that is what the downward API
(`limits.cpu`, divisor 1) gives and what the base uses; a candidate may also write the literal.
Candidate ConfigMaps are allowed (a merged service may want its own configuration); Secrets,
PVCs, PodDisruptionBudgets and StatefulSets are not. The `frontend` Service stays the task's,
so its name, port and selector cannot change; any Deployment whose pods carry its labels serves
it. A candidate without `app.yaml` (from before the envelope) gets `k8s/app.yaml`, the base
topology, so its numbers are those it had.

## Resource limits

Requests equal limits on every container (Guaranteed QoS). Application tier: at most 16 CPUs
and 32 GiB in total, split by the candidate (the base: 16 pods, 8 services x 2, at 1 CPU and
2 GiB each). MongoDB: 2 CPU, 2 GiB each. memcached: 1 CPU, 1 GiB each. consul: 1 CPU,
512 MiB. jaeger: 2 CPU, 1 GiB. The namespace's deployments request at most 34 CPUs and
48.5 GiB. Pods of the jobs: seed 4 CPU (before the application starts), ready/probe 0.5 CPU,
oracle 2 CPU, phase (loadgen) 6 CPU, 4 GiB. Peak demand is 40 CPUs (benchmark) or 36 (check),
on a 64-CPU node shared with other tasks' jobs, so two of these jobs cannot run at once.

## Manifest provenance

`k8s/manifest.yaml` is the Helm-rendered manifest of
`hotelReservation/helm-chart/hotelreservation` at DeathStarBench
`867806e575e1f7fb24437ae969910ddb17a76121`, as hand-edited in vibesys's
`examples/microservices/hotel-correctness/.vibesys/tasks/kubernetes/manifest.yaml`, with
these further edits: images pinned by tag and digest (consul 1.15.4, jaeger 1.57,
memcached 1.6.7, mongo 5.0), the application image is `${HOTEL_IMAGE}`, two replicas of each application Deployment, resources as above, the application Deployments and the gRPC Services moved to `k8s/app.yaml` (and the base's `hotelReservation/deploy/app.yaml`),
`terminationGracePeriodSeconds: 5`, and `GOMAXPROCS` of each application container from the downward API (`limits.cpu`). Without it the base measured about 345 operations/s with 100 ms latency steps (CFS throttling: Go 1.21 sees every core of the node), with it about 925. Service configs keep stock values
(`GC=100`, `JAEGER_SAMPLE_RATIO=0.01`).
