# social-network-k8s task files

Task-owned inputs for the Social Network Kubernetes task. `OBJECTIVE.md` is for the
optimizing agents; this file is for whoever maintains the task. Nothing here is a
candidate input: the CLI versions these files as the task.

| Path | What |
| --- | --- |
| `vibesys.input.toml`, `objectives.toml` | Manifest and the two Pareto objectives. |
| `Dockerfile` | Job image: kubectl 1.34, buildctl, Go, Python 3, and `loadgen`, `seed` and `checker` built from `evaluator/` against a clone of the vibesys repository at commit `de037943cb6a8e6e73d00e8bc19fa53570558583` (the generic servicebench engine). Base images are pinned by digest. |
| `run.py` | Trusted wrapper: `check`, `benchmark`, `sweep`, and `ctl` (the checker's restart and flush hook). |
| `k8s/manifest.yaml` | The application: 24 Deployments, 24 Services, 1 ConfigMap (tracing). Flat, namespaced kinds only. |
| `evaluator/` | Owned by the evaluator: the Social Network adapter, dataset, seed, checker; `evaluator/INTERFACE.md` is the contract with `run.py`. |
| `workload/closed.toml`, `workload/open.toml` | The closed-loop (`operations_per_second`) and open-loop (`latency_ms.p99`) workloads. |

## What a job does

1. Builds `socialNetwork/` with `buildctl` against `$VIBESYS_K8S_BUILDER` and pushes
   `$VIBESYS_K8S_REGISTRY/social-network-k8s/social-network:<tree hash>`; builds and pushes the
   evaluator image (busybox + `loadgen` + `seed` + `workload/`).
2. Creates namespace `hk8s-sn-<random>` and applies `k8s/manifest.yaml` with `${NAMESPACE}` and
   `${SOCIAL_NETWORK_IMAGE}` substituted, in two parts: first the configuration, every Service
   and the data tier (`tier: data`: MongoDB, Redis, memcached, jaeger). Once the data tier is
   Available it runs the Job `seed`, which writes the task's dataset directly into the data
   tier (`seed.log`, `seed.summary.json` in the artifacts). Only then it applies the twelve
   application Deployments (`tier: app`) and waits for every Deployment to be Available.
3. Starts a local forwarder: one supervised `kubectl port-forward` per nginx-thrift pod and a
   local listener that hands each new connection to the next pod, so a client with several
   connections reaches both replicas (a forward to `service/nginx-thrift` would pick one pod).
   Waits until `/` and a user- and a home-timeline read answer 200, twice over each forward.
4. `check`: runs `checker` against the forwarder with `--restart-command-json` pointing at
   `run.py ctl`, which restarts one pod of a stateless Deployment (`restart NAME`: delete it,
   wait for the rollout, all Deployments Available and the API ready) or flushes a cache tier
   (`flush NAME`: memcached `flush_all`, Redis `FLUSHALL`; `home-timeline-redis` is a store and
   is refused). Exit 0 is a pass.
   `benchmark`: draws fresh `--seed`/`--fixture-seed` for each phase (`seeds.json`), runs the
   load-generator Job `loadgen-closed` against `http://nginx-thrift:8080` (closed loop,
   `operations_per_second`), then `loadgen-open` (open loop, `latency_ms.p99`) offered 70% of the
   closed loop's throughput (`offered-rate.json`). The closed phase's write journal
   (`--state-out`, base64 in its log) reaches the open phase (`--state-in`) as ConfigMaps of at
   most 700 KiB each. One protocol-2 result carries both values; `pods.txt` records the pods and
   their restarts.
5. Deletes the namespace on every exit path the process can see: normal end, error, SIGTERM,
   SIGINT and SIGHUP. The CLI ends a cancelled or timed-out job with SIGKILL, which leaks the
   namespace. Each namespace therefore carries labels `vibesys.dev/social-network-k8s=1`,
   `vibesys.dev/social-network-k8s-created=<epoch>` and `vibesys.dev/social-network-k8s-job=<job
   id>`, and every job first deletes the task's `hk8s-sn-*` namespaces older than the larger of
   the manifest's `timeout_seconds` plus 10 minutes. The label keys are this task's own:
   hotel-k8s sweeps only namespaces with its key `vibesys.dev/hotel-k8s`, so neither task's
   sweep touches the other's namespaces. `run.py sweep` runs only the sweep.

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
Measured on the cluster's builder: cold build (empty cache) 77 s; unchanged tree 0 s (the
image tag is the tree hash and the layers are cached).

## Topology and resources

Requests equal limits on every container (Guaranteed QoS).

| Tier | Deployments | Each | CPU |
| --- | --- | --- | --- |
| Application | 11 C++ services + nginx-thrift, 2 replicas each | 1 CPU; 1 GiB (nginx-thrift 2 GiB) | 24 |
| MongoDB 4.4.6 | post-storage, social-graph, url-shorten, user, user-timeline | 2 CPU, 4 GiB, WiredTiger cache 2 GB | 10 |
| Redis 6.2.4 | home-timeline, social-graph, user-timeline | 1 CPU, 4 GiB, no persistence, maxmemory 3 GB noeviction | 3 |
| memcached 1.6.7 | post-storage, url-shorten, user | 1 CPU, 1 GiB, `-m 512 -t 2` | 3 |
| jaeger 1.57 all-in-one | jaeger | 2 CPU, 2 GiB, 100k traces in memory | 2 |
| Jobs | loadgen (6 CPU, 4 GiB), seed (4 CPU, 2 GiB) | | 6 |

About 42 CPUs for the deployment and 48 while a load phase runs. The media service has no
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

## Environment contract

| Variable | Meaning |
| --- | --- |
| `KUBECONFIG` | Kubeconfig of the cluster; its server address must be reachable from the job's network namespace (host networking). |
| `VIBESYS_K8S_REGISTRY` | Registry as both BuildKit and the cluster's nodes pull it, for example `localhost:5001` (plain http). |
| `VIBESYS_K8S_BUILDER` | BuildKit address, for example `tcp://127.0.0.1:1234`. |
| `VIBESYS_ARTIFACTS_DIR` | Optional. Build, seed and phase logs, summaries, streams, `seeds.json`, `offered-rate.json`, `pods.txt`. |
