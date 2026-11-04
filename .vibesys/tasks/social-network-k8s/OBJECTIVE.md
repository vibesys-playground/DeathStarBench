# Social Network on Kubernetes: throughput and p99 latency

## Goal

Improve the DeathStarBench Social Network application on two objectives at once.
Both are measured on every sample, against a Kubernetes cluster, in two phases:

| Objective | Direction | Phase |
| --- | --- | --- |
| `operations_per_second` | max | Closed loop, a fixed number of concurrent clients, the canonical mixed workload. |
| `latency_ms.p99` | min | Open loop, the same mix, offered at 70% of the throughput the same sample's closed loop just measured. |

The two together form a Pareto frontier: a change that raises throughput and lowers tail
latency is a win; a change that trades one for the other is a tradeoff, not a promotion.

## Workload

Requests go to nginx-thrift's public HTTP API (`/wrk2-api/...` and `/api/...`):

- 50% user-timeline reads (a user's own posts, newest first),
- 40% home-timeline reads (posts of the users one follows, newest first),
- 10% compose-post, each followed by reads that check the new post is visible
  (read-your-write),
- about 1% follow / unfollow.

Users are picked by a skewed (Zipf) popularity over a social graph of tens of thousands of
users with heavy-tailed follower counts; timelines start with depth (pre-populated posts
with realistic text, mentions and URLs). Every sample draws fresh load seeds, so request
streams differ between samples.

## Scope

You may change only code under `socialNetwork/`: the C++ services (`src/`, `gen-cpp/`,
`CMakeLists.txt`, `cmake/`, `third_party/`), the Thrift IDL and generated code, the
front end (`nginx-web-server/conf/nginx.conf`, `nginx-web-server/lua-scripts/`,
`nginx-web-server/nginx-thrift.sh`, `gen-lua/`, `docker/openresty-thrift/lua-thrift/`),
the services' `config/service-config.json`, and `socialNetwork/Dockerfile`, which builds
the one image that every application pod runs (build context `socialNetwork/`).
Do not touch `.vibesys/`.

The task owns everything else, and a candidate cannot change it:

- the Kubernetes manifest (`k8s/manifest.yaml`): the topology of 11 C++ services and
  nginx-thrift, each at **2 replicas** behind a ClusterIP Service; 5 MongoDB, 3 Redis,
  3 memcached and jaeger; replica counts, resources, commands, and the tracing
  configuration (`jaeger-config.yml` / `jaeger-config.json`, sampling ratio 0.01);
- the dataset and how it is written (through the public API, before the load starts);
- the load generator, the workloads, the open loop's offered rate, and the checker.

Resources are fixed. Every application pod gets exactly 1 CPU (request = limit) and a
fixed memory (1 GiB, nginx-thrift 2 GiB). The data tier is sized so it is not the
bottleneck: each MongoDB 2 CPU / 4 GiB, each Redis 1 CPU / 4 GiB (no persistence,
no eviction), each memcached 1 CPU / 1 GiB (512 MB of cache, 2 threads), jaeger
2 CPU / 2 GiB. You cannot add replicas or CPU; a gain must come from the code using its
CPU better (cheaper serialization and fewer copies, fewer or batched RPCs and database
round trips, smarter caching, connection and thread use, nginx and Lua configuration).

## Start and readiness contract

The wrapper builds the image from `socialNetwork/Dockerfile` and starts:

- each service with `command: <Name>Service` (one of `ComposePostService`,
  `HomeTimelineService`, `MediaService`, `PostStorageService`, `SocialGraphService`,
  `TextService`, `UniqueIdService`, `UrlShortenService`, `UserMentionService`,
  `UserService`, `UserTimelineService`) on `PATH`, working directory
  `/social-network-microservices`; it reads `config/service-config.json` from the image
  and `config/jaeger-config.yml` mounted by the task, and listens on port 9090;
- nginx-thrift with `command: /usr/local/bin/nginx-thrift`, listening on port 8080, with
  `/usr/local/openresty/nginx/jaeger-config.json` mounted by the task and the environment
  variable `fqdn_suffix` set to `.<namespace>.svc.cluster.local`.

Service addresses are the Kubernetes Service names of the manifest (`<name>:<port>`, for
example `user-timeline-redis:6379`, `post-storage-mongodb:27017`, `jaeger:6831`). The
deployment is ready when every Deployment is Available (services: TCP 9090; nginx:
`GET /` = 200) and user- and home-timeline reads answer 200 through nginx-thrift.
A candidate that does not become ready in 300 seconds fails.

## Correctness bar

The check deploys the candidate, seeds the dataset and runs the task's checker over the
public API only. The contract:

- Every response has the schema and values of the pinned application (timeline order
  newest first, `start`/`stop` windows, post fields, user mentions, shortened URLs, media).
- An acknowledged post is in its author's user timeline immediately (the next read).
- It is in the home timeline of each follower (and of each mentioned user) within
  **1 second**.
- A follow or unfollow is reflected in the home timelines of posts composed after it
  within 1 second.
- This holds on every replica: any instance may serve any request.
- Data survives a restart of any stateless pod and a flush of a cache tier (memcached,
  `social-graph-redis`, `user-timeline-redis`): acknowledged posts, follows and users
  live in MongoDB (home timelines in `home-timeline-redis`, which is a store and is
  never flushed). The checker restarts single pods and flushes caches during the check.

The load generator checks the responses of the benchmark the same way, and a wrong
answer fails the measurement.

## Rules

- No caching of benchmark inputs or answers keyed on the workload (fixed seeds, the
  seeded user ids) beyond what a production system would do; every cache must stay
  correct under the writes the workload and the checker make.
- The data comes from the data tier at run time: do not embed the dataset, its
  generator or answers precomputed from them in the candidate.
- Do not skip work the checker cannot see: keep reporting traces at the task's sampling
  ratio for every hop, no weakened persistence (acknowledged writes reach the store),
  no relaxed validation or authentication.
- Do not detect the benchmark or the check (by user agent, request pattern, namespace
  or environment) and behave differently.
- Do not depend on the cluster, host or network beyond the manifest; one candidate
  image runs every application pod.
- The candidate may not read or write anything outside its pods.
