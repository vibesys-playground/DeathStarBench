# Hotel Reservation on Kubernetes: throughput and p99 latency

## Goal

Improve the DeathStarBench Hotel Reservation application on two objectives at once.
Both are measured on every sample, against a Kubernetes cluster, in two phases:

| Objective | Direction | Phase |
| --- | --- | --- |
| `operations_per_second` | max | Closed loop, 32 concurrent clients, the canonical mixed workload. |
| `latency_ms.p99` | min | Open loop, the same mix, offered at 70% of the throughput the same sample's closed loop just measured. |

The two together form a Pareto frontier: a change that raises throughput and lowers tail
latency is a win; a change that trades one for the other is a tradeoff, not a promotion.

## Scope

You may change only code under `hotelReservation/`: the Go services in `services/`,
`cmd/`, `dialer/`, `registry/`, `tracing/`, `tune/`, `tls/`, `lease/`, `changes/`,
`internal/`, the module and its
`vendor/` tree, and `hotelReservation/Dockerfile` (it builds the one image that all
eight application services run). Do not touch `.vibesys/`.

The task owns everything else, and a candidate cannot change it:

- the data: the task generates its dataset and writes it into the six MongoDB
  databases before your services start (see Data below);
- the Kubernetes manifests (`k8s/manifest.yaml`): the topology of 8 application
  services, 6 MongoDB, 3 memcached, consul and jaeger, replica counts (2 instances of
  each application service, 1 of everything else), the service config files and
  environment variables of each application pod, and the resources below;
- the load generator, the workloads, the open loop's offered rate (70% of the sample's own
  closed-loop throughput, so a faster candidate is held to proportionally more load), and the
  semantic oracle.

Resources are fixed. Every application pod gets exactly 1 CPU (request = limit) and
2 GiB: 16 application CPUs in 16 pods. The data and cache tiers have fixed limits so they are not the bottleneck:
each MongoDB 2 CPU / 2 GiB, each memcached 1 CPU / 1 GiB, consul 1 CPU / 512 MiB,
jaeger 2 CPU / 1 GiB. The manifest also sets `GOMAXPROCS` of every application container to its CPU limit
(1), as an operator would: the Go 1.21 runtime in the image does not read the cgroup
quota, and with the node's full core count it burns the quota in a few milliseconds
and is throttled for the rest of each 100 ms period. You cannot add or remove replicas or CPU; a gain must come from the code
using its CPU better (fewer allocations and syscalls, cheaper serialization, smarter
caching and batching, less work on the request path, connection and goroutine use,
fewer or merged internal RPCs, Go runtime settings that the code itself can set).

## Replicas

Each application service runs as two instances (pods) behind its Kubernetes Service
(the frontend) or its consul name (the gRPC services, which clients round-robin over).
Any instance may serve any request: the load generator and the oracle reach both
frontend instances, and every internal call may land on either instance of the
callee. State that must be shared lives in MongoDB or memcached, or is kept coherent
within the staleness bound below. The oracle restarts single instances (one pod of
each service while the other keeps serving) and every instance at once; neither may
lose an acknowledged write or bring back an older value.

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
request so that its reads reach both frontend instances. It passes only if every observation matches the reference behavior of the pinned
application: the four frontend routes `/hotels`, `/recommendations`, `/user`,
`/reservation` with exact response schemas and values over the task's dataset and the
writes made so far; the owner routes and `/rates` with the contract above (the oracle
reads every write at 1 s plus a margin, at least eight times so the reads reach every
instance, and again after restarting one instance of every stateless service and then
all of them); reservation capacity by hotel and night (atomic, over-capacity
rejection, no overselling of sequential requests, and a concurrent burst on one night
accepts exactly the rooms left); degenerate date ranges; concurrent isolation; and
durability: acknowledged reservations survive a restart of every instance of the
stateless services and of the reservation cache tier, and a restart of one instance of
each. A restart deletes the pods (1 s grace) and waits until their replacements are
ready (below) before the oracle reads again.
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
`hotelReservation/`) and starts each service with `command: /go/bin/<service>` and
`/workspace/config.json` mounted from the task's config map. So the Dockerfile must
leave the service binaries at `/go/bin/{frontend,geo,profile,rate,recommendation,
reservation,search,user}`, and `/go/bin/<service>` must read `/workspace/config.json`
for its addresses. Service-to-service discovery stays on consul (every instance of a
gRPC service registers at its pod IP), tracing stays on jaeger (`jaegerAddress`), and
the frontend listens on port 5000. The deployment is ready when every replica of
every Deployment is Ready, every instance of the seven gRPC services is registered in
consul at its pod IP, and `/user`, `/recommendations` and `/hotels` return HTTP 200 on
each frontend pod; the same holds after every restart. A candidate that does not
become ready in 300 seconds fails.

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
  image runs all eight services.
- The candidate may not read or write anything outside its pods.
