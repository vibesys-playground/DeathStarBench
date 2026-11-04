# Hotel Reservation on Kubernetes: throughput and p99 latency

## Goal

Improve the DeathStarBench Hotel Reservation application on two objectives at once.
Both are measured on every sample, against a Kubernetes cluster, in two phases:

| Objective | Direction | Phase |
| --- | --- | --- |
| `operations_per_second` | max | Closed loop, 32 concurrent clients, the canonical mixed workload. |
| `latency_ms.p99` | min | Open loop at a fixed offered rate (declared in `workload/open.toml`), the same mix. |

The two together form a Pareto frontier: a change that raises throughput and lowers tail
latency is a win; a change that trades one for the other is a tradeoff, not a promotion.

## Scope

You may change only code under `hotelReservation/`: the Go services in `services/`,
`cmd/`, `dialer/`, `registry/`, `tracing/`, `tune/`, `tls/`, the module and its
`vendor/` tree, and `hotelReservation/Dockerfile` (it builds the one image that all
eight application services run). Do not touch `.vibesys/`.

The task owns everything else, and a candidate cannot change it:

- the Kubernetes manifests (`k8s/manifest.yaml`): the topology of 8 application
  services, 6 MongoDB, 3 memcached, consul and jaeger, replica counts (1 each), the
  service config files and environment variables of each application pod, and the
  resources below;
- the load generator, the workloads and the offered rate, and the semantic oracle.

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

## Correctness bar

The check deploys the candidate and runs the Go semantic oracle in accuracy mode. It
passes only if every observation matches the reference behavior of the pinned
application: the four frontend routes `/hotels`, `/recommendations`, `/user`,
`/reservation` with exact response schemas and values; seeded data; reservation
capacity by hotel and night (atomic, over-capacity rejection, no overselling of
sequential requests); degenerate date ranges; concurrent isolation; and durability:
acknowledged reservations survive a restart of the stateless services and of the
reservation cache tier. The oracle also restarts those pods during the check.
`/reservation` still reaches the reservation service after an invalid login, as in
the pinned reference.

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
- Do not skip work the oracle cannot see: no dropped traces on purpose beyond the
  configured sampling, no weakened persistence (acknowledged writes reach MongoDB),
  no relaxed authentication or validation.
- Do not detect the benchmark or the check (by user agent, request pattern, namespace
  or environment) and behave differently.
- Do not depend on the cluster, host or network beyond the manifests; one candidate
  image runs all eight services.
- The candidate may not read or write anything outside its pods.
