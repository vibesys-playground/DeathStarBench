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
`cmd/`, `dialer/`, `registry/`, `tracing/`, `tune/`, `tls/`, the module and its
`vendor/` tree, and `hotelReservation/Dockerfile` (it builds the one image that all
eight application services run). Do not touch `.vibesys/`.

The task owns everything else, and a candidate cannot change it:

- the data: the task generates its dataset and writes it into the six MongoDB
  databases before your services start (see Data below);
- the Kubernetes manifests (`k8s/manifest.yaml`): the topology of 8 application
  services, 6 MongoDB, 3 memcached, consul and jaeger, replica counts (1 each), the
  service config files and environment variables of each application pod, and the
  resources below;
- the load generator, the workloads, the open loop's offered rate (70% of the sample's own
  closed-loop throughput, so a faster candidate is held to proportionally more load), and the
  semantic oracle.

Resources are fixed. Every application pod gets exactly 1 CPU (request = limit) and
2 GiB. The data and cache tiers have fixed limits so they are not the bottleneck:
each MongoDB 2 CPU / 2 GiB, each memcached 1 CPU / 1 GiB, consul 1 CPU / 512 MiB,
jaeger 2 CPU / 1 GiB. The manifest also sets `GOMAXPROCS` of every application container to its CPU limit
(1), as an operator would: the Go 1.21 runtime in the image does not read the cgroup
quota, and with the node's full core count it burns the quota in a few milliseconds
and is throttled for the rest of each 100 ms period. You cannot add replicas or CPU; a gain must come from the code
using its CPU better (fewer allocations and syscalls, cheaper serialization, smarter
caching and batching, less work on the request path, connection and goroutine use,
fewer or merged internal RPCs, Go runtime settings that the code itself can set).

## Data

The databases hold a realistic dataset that the task generates from a fixed seed and
writes before the application services start: 25,000 hotels in 20 cities worldwide
(distributed by city popularity, spread a few km around each centre), each with a
profile, a location, 1 to 5 rate plans over the year 2027, a rating, a price and a room
capacity, and 10,000 user accounts; about 30 MB of documents, with indexes on the
services' query keys. A service finds its collection non-empty and must not replace or
rewrite it; it may read it however it likes (and add indexes of its own). Requests are
spread realistically over this data: search and recommendation locations by city
popularity with a continuous spread, stays of 1 to 7 nights over 2027, logins and
reservations by user and hotel popularity, so exact-request caching hits rarely and
caches have to be sized for the data.

## Correctness bar

The check deploys the candidate and runs the Go semantic oracle in accuracy mode. It
passes only if every observation matches the reference behavior of the pinned
application: the four frontend routes `/hotels`, `/recommendations`, `/user`,
`/reservation` with exact response schemas and values over the task's dataset; reservation
capacity by hotel and night (atomic, over-capacity rejection, no overselling of
sequential requests); degenerate date ranges; concurrent isolation; and durability:
acknowledged reservations survive a restart of the stateless services and of the
reservation cache tier. The oracle also restarts those pods during the check.
`/reservation` still reaches the reservation service after an invalid login, as in
the pinned reference. The load generator checks every response of the benchmark the same
way, and a single wrong answer fails the measurement.

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
for its addresses. Service-to-service discovery stays on consul, tracing stays on
jaeger (`jaegerAddress`), and the frontend listens on port 5000. The deployment is
ready when `/user`, `/recommendations` and `/hotels` return HTTP 200 through the
frontend service. A candidate that does not become ready in 300 seconds fails.

## Rules

- No caching of benchmark inputs or answers keyed on the workload (fixed seeds,
  fixed dates, the seeded user or hotel ids) beyond what a production system would
  do; every cache must stay correct under the reservation writes the oracle makes.
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
