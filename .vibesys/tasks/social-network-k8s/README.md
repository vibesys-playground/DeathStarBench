# social-network-k8s task files

Task-owned inputs for the Social Network Kubernetes task. `OBJECTIVE.md` is for the
optimizing agents; this file is for whoever maintains the task. Nothing here is a
candidate input: the CLI versions these files as the task.

| Path | What |
| --- | --- |
| `vibesys.input.toml`, `objectives.toml` | Manifest and the two Pareto objectives. |
| `Dockerfile` | The task image every job (and the pods of a benchmark sample or an accuracy check) runs in: vibesys-evaluator (the Python package from PyPI, whose platform wheel bundles the core, `loadgen` and `otelcapture`, the latter two linked onto PATH for the probe pods; release 0.3.0rc1, the Dockerfile's `VIBESYS_EVALUATOR_VERSION` sets it), kubectl 1.34 and buildctl, and `seed`, `sn-generator`, `sn-accuracy-generator` and `sn-oracle` built from `evaluator/` against the vibesys-evaluator Go module `evaluator/go.mod` requires at the same release. Base images are pinned by digest. |
| `bench.py`, `dataset.py` | The benchmark on vibesys-evaluator: the system (`k8s.App`), the seed, the workload (`sn-generator`), the phases and the validity rules; the dataset is the Go dataset's seed and digest. |
| `accuracy.py` | The accuracy check on vibesys-evaluator's accuracy run: bench.py's system, the accuracy workload (`sn-accuracy-generator`), the fault schedule and the oracle (`sn-oracle`). |
| `k8s/manifest.yaml` | The application: 24 Deployments, 24 Services, 1 ConfigMap (tracing). Flat, namespaced kinds only. |
| `evaluator/` | Owned by the evaluator: dataset, seed, the benchmark's workload (`internal/snworkload`), the accuracy workload (`internal/snaccuracy`) and the oracle (`cmd/oracle`, `internal/snspec`); `evaluator/INTERFACE.md` is the contract with `bench.py` and `accuracy.py`. |
| `tests/` | Unit tests of `bench.py`, `dataset.py` and `accuracy.py` (`python -m unittest discover -s tests`, with vibesys-evaluator installed). |

## The benchmark (`bench.py benchmark`)

vibesys-evaluator builds `socialNetwork/` (image variable `SOCIAL_NETWORK_IMAGE`), deploys
`k8s/manifest.yaml` into a fresh namespace, applies the data tier (`tier=data`) first, runs the
seed Job (`bench.py seed`, which runs the Go `seed`: the dataset and its indexes), then the
application, and waits until `/` and a user- and a home-timeline read of user 1999999999 answer
200. Each phase runs in a pod: `loadgen` with `sn-generator generate` under it. A benchmark only
measures: nothing checks the answers (the accuracy check does), and an answer other than 200 is an
error of its window. Every phase builds its model afresh: its compose markers are its own, and a
follow or unfollow is drawn from the seeded graph and the phase's own changes.

Phases: two closed-loop trials (32 clients, 10 s warmup, 20 s), `operations_per_second` their
median; three open-loop trials (evenly spaced arrivals, 10 s warmup, 60 s) offered 70% of that,
`latency_ms.p99` their median. Validity: no error, every operation of the mix completed, the open
loop sustains 95% of its rate.

## The accuracy check (`accuracy.py accuracy`)

vibesys-evaluator's accuracy run over bench.py's system: it builds and deploys the candidate as a
benchmark sample does (data tier, seed, application, readiness probes), then runs four open-loop
phases of `sn-accuracy-generator` (300 operations/s, 2 s warmup, 30 s; about 36,000 operations)
with every answer captured, applies the faults between them, tears the namespace down and runs
`sn-oracle --history DIR` on the captured history. Exit 0 PASS, 1 FAIL, 2 UNKNOWN (also when the
run cannot complete); the report, the failing partitions' timelines and the history are in
`accuracy/` of the job's artifacts.

Faults (`Fault` is nil in the oracle: none of them may change what a read returns):

- after phase 0, one pod of each of the twelve application Deployments, one after the other,
  each waited for until the application serves again;
- after phase 1, the cache tiers emptied in place: `flush_all` on the three memcached (bash's
  `/dev/tcp`), `FLUSHALL` on `social-graph-redis` and `user-timeline-redis` (`redis-cli`);
- after phase 2, every pod of each application Deployment at once, one Deployment after the
  other.

`home-timeline-redis` is the only store of home timelines in the pinned application (no
persistence, no other copy) and MongoDB holds the data: neither is restarted or flushed.

The accuracy workload (`evaluator/internal/snaccuracy`, see `evaluator/INTERFACE.md`) sends one
action per operation, its Info the action's JSON: composes, follows and unfollows among 40 hot
users (the 36 most active and the 4 most followed, whose posts fan out to thousands of seeded
followers) and reads of their timelines, reads over the whole dataset as the benchmark draws them,
a few registrations and a few invalid requests. The rate is fixed and modest so that the answers
are judged, not the capacity: more than 1% of unknown outcomes (lost answers and 5xx) fails.

## Accuracy oracle

`evaluator/cmd/oracle` is the task's semantic oracle on the vibesys-evaluator checker
(`check`, `sdk/vibesys-evaluator/docs/accuracy-checks.md`): `check.Main` over two `check.Spec`s
from `internal/snspec`, whose verdicts are ANDed.

- **`AuthorSpec`**, per author (`u:<id>`: composes, registration, follow edges that point at the
  user, its user timeline, and every home read projected to the authors on its page; `n:<name>`
  for username claims): visibility with the bound B = 1 s, edge uncertainty, mentions that
  deliver regardless of edges, post identity learned at first sight, content, order and the
  author's slice of the page.
- **`ReaderSpec`**, per reader (`r:<reader>`): the page arithmetic across authors, which the author
  view cannot see ("page starts too new", an empty page with posts that must be there). Every
  compose is replicated into the partitions of the readers that could receive it.
- **Projection**: two passes over the history (`check.Spec.Project` may range the phases more
  than once): the first collects who is registered, who follows, mentions and reads whom (ids
  only), the second adds the pieces, so memory stays at one phase plus those facts.
- **Faults**: `Fault` is nil in both. Every acknowledged write survives a restart or a flush, so
  a post, edge or timeline entry lost across one shows as an entry that must be visible and is
  missing. A lost response or 5xx is an unknown outcome (`EffectWindow` 10 s).
- **Tests**: `go test ./internal/snspec` (crafted histories for every rule, and 15 runs of the
  in-process fake application: correct with a synchronous or an asynchronous fan-out and with
  timeouts, and injected bugs: stale beyond the bound, stale cache or replica, a missing,
  phantom or misordered post, an unfollow ignored, posts lost on a restart, home timelines lost
  on a flush, a partial follower cache, a shifted page; `-short` skips the runs).
  `go test ./internal/snaccuracy` runs the accuracy workload itself against the fake application
  with a restart and a flush: the correct fake passes, a stale cache, an ignored unfollow, posts
  lost on a restart and a partial follower cache after a flush fail.
  `SN_ORACLE_CAPTURE=<spike's sn/runs/real1/hist> go test -v ./cmd/oracle` checks the oracle
  spike's real capture offline (171,000 calls, 2 restarts and 2 flushes: PASS; with newest posts
  dropped from five pages: FAIL).

## Image and build

`socialNetwork/Dockerfile` builds one image for every application pod: the C++ services
(`/usr/local/bin/<Name>Service`, working directory `/social-network-microservices`, which holds
`config/service-config.json`) on top of `yg397/openresty-thrift:xenial` (OpenResty with the
OpenTracing module, the Jaeger plugin and the Thrift Lua runtime), plus the candidate's
`nginx.conf`, Lua scripts, generated Lua code and pages, started by `/usr/local/bin/nginx-thrift`
(`nginx-web-server/nginx-thrift.sh`: points nginx's resolver at the pod's nameserver, then
starts OpenResty). Both base images are xenial, and libopentracing (1.5.1) is the same in both,
so the services' shared libraries are copied next to OpenResty's without conflict.

The C++ stage copies only `CMakeLists.txt`, `cmake/`, `third_party/`, `gen-cpp/` and `src/`, so a
change to the Lua scripts or nginx.conf leaves it cached, and it compiles through ccache in a
BuildKit cache mount (`id=social-network-ccache`), which the builder keeps between jobs.
Measured on the cluster's builder: cold build (empty cache) 77 s; one C++ source changed
18-26 s; a Lua or nginx.conf change under 1 s; unchanged tree 0 s (the image tag is the
tree hash and the layers are cached).

## Topology and resources

Requests equal limits on every container (Guaranteed QoS).

| Tier | Deployments | Each | CPU |
| --- | --- | --- | --- |
| Application | 11 C++ services + nginx-thrift, 2 replicas each | 1 CPU; 1 GiB (nginx-thrift 2 GiB) | 24 |
| MongoDB 4.4.6 | post-storage, social-graph, url-shorten, user, user-timeline | 2 CPU, 4 GiB, WiredTiger cache 2 GB | 10 |
| Redis 6.2.4 | home-timeline, social-graph, user-timeline | 1 CPU, 4 GiB, no persistence, maxmemory 3 GB noeviction | 3 |
| memcached 1.6.7 | post-storage, url-shorten, user | 1 CPU, 1 GiB, `-m 512 -t 2` | 3 |
| jaeger 1.57 all-in-one | jaeger | 2 CPU, 2 GiB, 100k traces in memory | 2 |
| Pods | the phase and seed pods of a benchmark sample or an accuracy check (vibesys-evaluator's sizes) | | |

About 42 CPUs for the deployment, plus the phase pod while a load phase runs. The media service has no
database in the pinned application (its MongoDB and memcached of the Helm chart are omitted),
and there is no RabbitMQ: the pinned ComposePostService fans out to home timelines
synchronously through home-timeline-service (WriteHomeTimelineService is not built).

Tracing is the task's: the ConfigMap `jaeger-config` is mounted over the services'
`config/jaeger-config.yml` and nginx's `jaeger-config.json` (probabilistic sampling 0.01,
agent `jaeger:6831`), so every candidate reports traces at the same ratio.

## Base-application changes

Changes to `socialNetwork/` that are part of the base every candidate starts from:

- `Dockerfile`: `CMAKE_BUILD_TYPE=Release` instead of Debug (`-O3` either way from
  `CMAKE_CXX_FLAGS`; Debug added `-g` and kept assertions).
- One image with the services and nginx-thrift (above); `nginx.conf` passes `fqdn_suffix` to
  the Lua scripts (which already append it to service names) and includes `resolver.conf`
  (Docker's `127.0.0.11` by default, mounted by the Compose files; rewritten by
  `nginx-thrift.sh` in the image).

- `SocialGraphHandler.h`: GetFollowers/GetFollowees no longer fail for a user with none (an
  empty set was ZADDed into Redis; composing a post by a user nobody follows returned 500).
- `SocialGraphHandler.h`: Follow/Unfollow delete the cached `<id>:followers`/`<id>:followees`
  sets once MongoDB holds the change, instead of ZADD/ZREM into a set that may not be loaded
  (after a flush one follow left a one-member set that GetFollowers trusted as complete).
- `machine_id.h`: unique-id and user services derive their 3-hex-digit machine id from the
  low 11 bits of the pod's IPv4 address instead of a 12-bit hash of the MAC address, so the
  two replicas cannot generate the same post or user id.

The seed writes the dataset with its indexes (evaluator `cmd/seed`): `user.username`,
`user.user_id`, `social-graph.user_id`, `post.post_id`, `user-timeline.user_id` and
`url-shorten.shortened_url`.

## Environment contract

| Variable | Meaning |
| --- | --- |
| `KUBECONFIG` | Kubeconfig of the cluster; its server address must be reachable from the job's network namespace (host networking). |
| `VIBESYS_K8S_REGISTRY` | Registry as both BuildKit and the cluster's nodes pull it, for example `localhost:5001` (plain http). |
| `VIBESYS_K8S_BUILDER` | BuildKit address, for example `tcp://127.0.0.1:1234`. |
| `VIBESYS_ARTIFACTS_DIR` | Optional. The benchmark's sample records and the accuracy check's report and history (`accuracy/`), by vibesys-evaluator. |
