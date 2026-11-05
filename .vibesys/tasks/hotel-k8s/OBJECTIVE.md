# Hotel Reservation on Kubernetes: throughput and p99 latency

## Goal

Improve the DeathStarBench Hotel Reservation application on two objectives at once.
Both are measured on every sample, against a Kubernetes cluster, in two phases:

| Objective | Direction | Phase |
| --- | --- | --- |
| `operations_per_second` | max | Closed loop, 32 concurrent clients, the canonical mixed workload, 2 trials of 60 s after a 15 s warmup. |
| `latency_ms.p99` | min | Open loop, the same mix, offered at 70% of the throughput the same sample's closed loop just measured. |

The two together form a Pareto frontier: a change that raises throughput and lowers tail
latency is a win; a change that trades one for the other is a tradeoff, not a promotion.

## Scope

You may change only code under `hotelReservation/`: the Go services in `services/`,
`cmd/`, `dialer/`, `registry/`, `tracing/`, `tune/`, `tls/`, `lease/`, `changes/`,
`internal/`, the module and its `vendor/` tree, `hotelReservation/Dockerfile` (it builds
the one image that every application container runs), and the application tier's
manifest `hotelReservation/deploy/app.yaml` (see Resources). Do not touch `.vibesys/`.

The task owns everything else, and a candidate cannot change it:

- the data: the task generates its dataset and writes it into the six MongoDB
  databases before your services start (see Data below);
- the data tier and its resources (`k8s/manifest.yaml`): 6 MongoDB at 2 CPU / 2 GiB
  each, 3 memcached at 1 CPU / 1 GiB each, consul at 1 CPU / 512 MiB and jaeger at
  2 CPU / 1 GiB, one replica each, so that the stores are not the bottleneck and a
  candidate cannot buy speed by shrinking them; the services' ConfigMaps (`frontend`,
  `geo`, `profile`, `rate`, `recommendation`, `reservation`, `search`, `user`, each with
  the `service-config.json` of the data tier's addresses); and the `frontend` Service
  (port 5000, selecting the pods labelled `app.kubernetes.io/name: frontend`,
  `app.kubernetes.io/instance: hotel`, `service: frontend`), through which the load
  generator and the oracle reach the application;
- the load generator, the workloads, the open loop's offered rate (70% of the sample's own
  closed-loop throughput, so a faster candidate is held to proportionally more load), and the
  semantic oracle.

## Resources

The application tier has a fixed capacity: **16 CPUs and 32 GiB in total** across all
application pods (each container's requests equal its limits). You choose how the eight
services' code is grouped into Deployments, how many replicas each Deployment runs (at
least 2, so that any one pod can restart without losing service), and how the 16 CPUs and
32 GiB are split among them. The data tier (6 MongoDB, 3 memcached, consul, jaeger) and its
resources stay as given. GOMAXPROCS of each application container must equal its CPU limit.

`hotelReservation/deploy/app.yaml` is that tier: Kubernetes Deployments, Services and
ConfigMaps, applied after the task's data tier has been seeded. The base gives the
reference topology: eight Deployments (one per service) of 2 pods at 1 CPU / 2 GiB each,
and a Service per gRPC service. Before it builds or deploys anything, the check and the
benchmark validate the file, and a candidate that breaks a rule fails with a message that
names the object and key:

- the CPU limits, summed over every container of every replica, are at most 16, and the
  memory limits at most 32 GiB; every container sets `requests` equal to `limits` for
  `cpu` and `memory` (nothing else), at least 100m CPU and 64 MiB, CPU in whole millicores;
- every Deployment sets `replicas` to at least 2;
- every container sets `GOMAXPROCS` once, to its CPU limit rounded up: the literal value
  (`"1"` for a limit of 500m or 1, `"2"` for 1500m or 2), or, as the base does, the downward
  API (`valueFrom.resourceFieldRef: {resource: limits.cpu, divisor: "1"}`, which Kubernetes
  rounds up the same way). The Go 1.21 runtime in the image does not read the cgroup quota:
  with the node's full core count it burns the quota in a few milliseconds and is throttled
  for the rest of each 100 ms period;
- every container runs the candidate image, `image: ${HOTEL_IMAGE}` (the one image built
  from `hotelReservation/Dockerfile`); `${NAMESPACE}` and `${HOTEL_IMAGE}` are the only
  variables;
- only Deployments (`apps/v1`), Services and ConfigMaps (`v1`), in the job's namespace
  (`metadata.namespace` absent or `${NAMESPACE}`), named differently from the task's
  objects above, without the labels `tier` and `backend` (the task's own);
- no init containers, host namespaces (`hostNetwork`, `hostPID`, `hostIPC`), node
  selection (`nodeName`, `nodeSelector`, `affinity`, `tolerations` other than empty),
  priority or runtime classes, privileges (`privileged`, added capabilities, privilege
  escalation), `hostPort`, `envFrom`, or volumes other than `configMap` and `emptyDir`
  (so no `hostPath` and no persistent volume claims); pods run as the namespace's default
  service account, which may do nothing; Services are `ClusterIP` (or headless);
- a Deployment's `selector.matchLabels` selects its own pods and no other Deployment's,
  and no pod carries the selector of a task Service other than `frontend`;
- at least one Deployment's pods carry the `frontend` Service's labels and serve HTTP on
  port 5000: the load generator and the oracle send every request there.

So a gain may come from the code using its CPU better (fewer allocations and syscalls,
cheaper serialization, smarter caching and batching, less work on the request path,
connection and goroutine use, fewer or merged internal RPCs, Go runtime settings that the
code itself can set), or from placing the capacity where the load is: more CPU for the
services at their limit and less for those that idle, other replica counts, or services
merged into one process or one pod so that their calls stay local.

## Replicas

Each application Deployment runs at least two pods, behind the `frontend` Service (the
pods that carry its labels) or behind consul names (the gRPC services, which clients
round-robin over). Any instance may serve any request: the load generator and the oracle
reach every frontend pod, and every internal call may land on any instance of the callee.
State that must be shared lives in MongoDB or memcached, or is kept coherent within the
staleness bound below. The oracle restarts single instances (one pod of each application
Deployment while the others keep serving) and every instance at once; neither may lose an
acknowledged write or bring back an older value.

## Data

The databases hold a realistic dataset that the task generates from a fixed seed and
writes before the application services start: 25,000 hotels in 20 cities worldwide
(distributed by city popularity, spread a few km around each centre), each with a
profile, a location, 1 to 5 rate plans over the year 2027, a rating, a price and a room
capacity, and 10,000 user accounts; about 30 MB of documents, with indexes on the
services' query keys. A service finds its collection non-empty and must not replace or
rewrite it; it may read it however it likes (and add indexes of its own). The data
changes only through the owner routes below. Requests are spread realistically over
this data: search and recommendation locations by city popularity with a continuous
spread, stays of 1 to 7 nights over 2027, logins and reservations by user and hotel
popularity, so exact-request caching hits rarely and caches have to be sized for the
data. About 1% of the requests are owner writes, drawn by the same hotel popularity, so
hot data changes during the run.

## Writes and their visibility

The frontend serves owner-side writes, each stored by the service that owns the data
in its MongoDB database. Parameters come from the query string or a form body.

| Route | Effect | Errors |
| --- | --- | --- |
| `POST /admin/rate?hotelId=&code=&bookableRate=&totalRate=&totalRateInclusive=` | Sets the three prices of one rate plan. | 400 missing or negative amount; 404 unknown hotel or plan code |
| `POST /admin/profile?hotelId=&name=&phone=` | Sets the hotel's name and/or phone (at least one; an empty one is unchanged). | 400 neither given; 404 unknown hotel |
| `POST /admin/capacity?hotelId=&rooms=` | Sets the hotel's room count. | 400 missing or negative; 404 unknown hotel; 409 when some night already has more rooms reserved |
| `POST /admin/hotel` (JSON body) | Adds a hotel: profile, rate plans, capacity, rating and price, location. | 400 malformed body or invalid field; 409 ID taken |
| `GET /rates?hotelId=` | Reads the hotel's rate plans with their current prices. | 400 missing hotelId |

A write is acknowledged with HTTP 200 and `{"message": ...}` (`Rate updated.`,
`Profile updated.`, `Capacity updated.`, `Hotel added.`); any other answer is not an
acknowledgement. The `/admin/hotel` body is
`{"hotelId", "name", "phoneNumber", "description", "address": {"streetNumber",
"streetName", "city", "state", "country", "postalCode"}, "lat", "lon", "rating",
"price", "rooms", "ratePlans": [{"code", "inDate", "outDate", "roomType": {"code",
"roomDescription", "bookableRate", "totalRate", "totalRateInclusive"}}]}`; unknown
fields are rejected, a hotel ID is 1 to 64 letters, digits or `-`, and at least one
rate plan is required. `GET /rates` answers `{"hotelId", "ratePlans": [...]}` with the
plans in that shape.

The contract:
- Bounded staleness: once a write is acknowledged, every read that starts more than
  1 second later reflects it, on every path (search, the three recommendations, the
  GeoJSON profile fields, `/rates`, reservations) and through every instance of every
  service. An added hotel is found by search and recommendations, and has its
  profile, rate plans and capacity, within the same bound.
- Within that second a read may show the new value or the one before it. Reads are
  not required to be monotonic: two reads in that window may disagree in either order.
- A write that was not acknowledged (an error or a timeout) may or may not take effect.
- Capacity is strict and immediate: a reservation that starts after a capacity write is
  acknowledged is decided against the new capacity, never against an old one, and no
  write may make a night overbooked (that is the 409). Reservations are never
  oversold, also when concurrent reservations of one night reach different instances:
  a burst of them accepts exactly the rooms left.
- Acknowledged writes are durable: they survive a restart of any stateless service or
  cache, and a restarted instance never serves an older value.

## Correctness bar

The check deploys the candidate and runs the Go semantic oracle in accuracy mode,
inside the cluster against the frontend Service, opening a new connection for every
request so that its reads reach every frontend pod. It passes only if every observation matches the reference behavior of the pinned
application: the four frontend routes `/hotels`, `/recommendations`, `/user`,
`/reservation` with exact response schemas and values over the task's dataset and the
writes made so far; the owner routes and `/rates` with the contract above (the oracle
reads every write at 1 s plus a margin, at least eight times so the reads reach every
instance, and again after restarting one pod of every application Deployment and then
all of them); reservation capacity by hotel and night (atomic, over-capacity
rejection, no overselling of sequential requests, and a concurrent burst on one night
accepts exactly the rooms left); degenerate date ranges; concurrent isolation; and
durability: acknowledged reservations survive a restart of every application pod and of
the reservation cache tier, and a restart of one pod of each application Deployment and
of the reservation cache. A restart deletes the pods (1 s grace) and waits until their
replacements are ready (below) before the oracle reads again.
`/reservation` still reaches the reservation service after an invalid login, as in
the pinned reference. The load generator checks every response of the benchmark the same
way, accepting for each read exactly the values the contract allows given the writes it
has made, and a single wrong answer fails the measurement.

The reference semantics include these details, which an optimization must keep:
- search answers the hotels that the pinned geo service returns (go-geoindex's
  `KNearest`: the 5 nearest within 10 km on its 0.5 km grid, an approximation that
  differs from an exact nearest-five search for about 3% of locations), restricted to
  hotels with rate plans and with a room free on every night; coordinates are parsed as
  float32 as the frontend does;
- recommendations by `dis` return every hotel at the minimum haversine distance over all
  hotels, by `rate` the hotels with the highest rating and by `price` those with the
  lowest price, independent of the location;
- GeoJSON coordinates are the profile's float32 values.

## Start and readiness contract

The wrapper builds the image from `hotelReservation/Dockerfile` (build context:
`hotelReservation/`) and applies `hotelReservation/deploy/app.yaml` with `${HOTEL_IMAGE}`
set to it. The base starts each service with `command: /go/bin/<service>` and mounts the
task's ConfigMap of that name at `/workspace/config.json`, which holds the data tier's
addresses; a candidate may mount any of the task's ConfigMaps (they are identical) or one
of its own. Service-to-service discovery stays on consul (`consulAddress`), tracing stays
on jaeger (`jaegerAddress`), and the frontend listens on port 5000. The deployment is
ready when every replica of every Deployment is Ready, every pod of a Deployment that
registers in consul is registered at its pod IP (a Deployment counts as registering when
any of its pods is; one that serves no gRPC need not register), and `/user`,
`/recommendations` and `/hotels` return HTTP 200 on each pod behind the `frontend`
Service; the same holds after every restart (a Deployment whose pods were registered
before a restart must have its replacements registered). A candidate that does not become
ready in 300 seconds fails.

## Rules

- No caching of benchmark inputs or answers keyed on the workload (fixed seeds,
  fixed dates, the seeded user or hotel ids) beyond what a production system would
  do; every cache and in-memory copy must honour the write contract above, for any
  hotel, under the reservations and owner writes of the benchmark and the oracle.
- The data comes from MongoDB at run time: do not embed the dataset, its generator or
  answers precomputed from them in the candidate (as if the data could change).
- Do not skip work the oracle cannot see: no dropped traces on purpose beyond the
  configured sampling, no weakened persistence (acknowledged writes reach MongoDB),
  no relaxed authentication or validation.
- Do not detect the benchmark or the check (by user agent, request pattern, namespace
  or environment) and behave differently.
- Do not depend on the cluster, host or network beyond the manifests; one candidate
  image runs every application container.
- The candidate may not read or write anything outside its pods.
