# social-network-k8s evaluator: interface for run.py

This file is the contract between the evaluator module (this directory) and
the task's trusted wrapper (`run.py`, the Dockerfile and the manifest). It is
owned by the evaluator; any change to it is announced in the commit that makes
it.

Go module `vibesys/social-network-k8s-evaluator`. Like hotel-k8s, `go.mod`
replaces the generic engine by a clone of the vibesys repository at the pinned
commit (de037943c) at `/src`:

    replace vibesys/microservice-evaluator => /src/resources/evaluators/microservice
    replace github.com/uw-syfi/vibesys/sdk/vs-evaluator/vseval => /src/sdk/vs-evaluator/vseval

Build (in the Dockerfile, `CGO_ENABLED=0`):

    go build -o /out/loadgen ./cmd/loadgen
    go build -o /out/seed    ./cmd/seed
    go build -o /out/checker ./cmd/checker

All three are static binaries without runtime files; the dataset is generated
in-process from the dataset seed. `loadgen` and `seed` go into the in-cluster
evaluator image, `checker` runs where `run.py check` runs (it only needs the
nginx URL and the restart hook).

## Application topology the evaluator assumes

- Gateway: nginx-thrift, `http://nginx-thrift:8080` inside the namespace, the
  public `/wrk2-api/...` routes of the pinned app only (no check-only
  endpoints, no timing headers).
- Data tier, addressed by Service name on its default port: `user-mongodb`,
  `social-graph-mongodb`, `post-storage-mongodb`, `user-timeline-mongodb`,
  `url-shorten-mongodb`, `media-mongodb` (27017); `home-timeline-redis`,
  `social-graph-redis`, `user-timeline-redis` (6379); `post-storage-memcached`,
  `user-memcached`, `url-shorten-memcached`, `media-memcached` (11211).
- Stateless deployments (2 replicas each): `nginx-thrift`,
  `compose-post-service`, `home-timeline-service`, `media-service`,
  `post-storage-service`, `social-graph-service`, `text-service`,
  `unique-id-service`, `url-shorten-service`, `user-mention-service`,
  `user-service`, `user-timeline-service`.

## `seed`: write the task's dataset into the data tier

The dataset (about 20k users, 0.4M follow edges, 0.1M posts, 2M home-timeline
entries; exact counts in the summary) cannot be loaded through the API in a
reasonable time (estimate in the evaluator README), and the API would assign
post ids, request ids and timestamps itself. `seed` therefore writes the
documents the services themselves write, with the same BSON types, directly:

    seed [--dataset-seed N] [--address NAME=HOST:PORT ...] [--timeout 10m]
    seed --summary-only        # generate and print the summary, write nothing
    seed --dump DIR            # write the documents as JSON lines, touch nothing

`NAME` is one of the data-tier Service names above that the dataset uses
(`user-mongodb`, `social-graph-mongodb`, `post-storage-mongodb`,
`user-timeline-mongodb`, `url-shorten-mongodb`, `home-timeline-redis`,
`social-graph-redis`); the defaults are `NAME:27017` / `NAME:6379`.

When to run it: as a Job in the namespace after the data-tier pods are Ready
and before any request reaches the application (simplest: before the
stateless services are applied; a readiness probe that reads a timeline of a
non-dataset user is harmless). Caches (memcached, `user-timeline-redis`) are
left empty; `social-graph-redis` is filled with the graph, because the pinned
social-graph service treats an existing follower set as complete.

Idempotent: an empty data tier is written; a data tier that already holds this
dataset (marker document `vibesys-seed.meta` in `post-storage-mongodb`, same
digest) is left alone and the command succeeds; anything else is an error
(exit 1). On success the last stdout line is one JSON object:

    {"dataset_seed": "...", "dataset_digest": "<sha256>", "counts": {...},
     "written": [{"target": "...", "documents": N, "seconds": S}, ...],
     "skipped": false, "seconds": S}

## `loadgen`: the load generator

`servicebenchcli.Run` with the task's Social Network adapter registered as
application `social-network`; flags and outputs are servicebench's
(`--mode benchmark --workload --base-url --seed --fixture-seed --rate
--startup-timeout --output-json --vs-output`), plus two task flags that are
removed before the servicebench flags are parsed:

    --state-out PATH   write the adapter's write journal to PATH on exit
    --state-in PATH    replay a journal written by an earlier phase before
                       the first trial

Why: both phases of a sample run against the same deployment. The closed
phase composes posts and changes follow edges; the open phase must know them
to validate exactly. run.py runs the closed phase with `--state-out`, carries
the file (gzip, normally a few hundred KiB, at most 900 KiB; the adapter fails
the phase rather than write more) to the open phase's Job (for example as a
ConfigMap mounted read-only) and passes `--state-in`. A phase run without
`--state-in` on a deployment another phase has written to fails validation.

`application_config` (in the workload files): `dataset_seed` (must equal the
seeded one; Prepare checks a sample of seeded timelines and fails fast if the
deployment holds another dataset), `freshness_ms` (1000).

Operations (all target `gateway`): `user_timeline_read`, `home_timeline_read`,
`compose_post` (compose, then read the author's user timeline: read your
write), `follow`, `unfollow`. Repetitions > 1 are allowed; the model carries
over between trials of one process.

## `checker`: the accuracy check

    checker --base-url URL --seed N|random --restart-command-json '["...", ...]'
            [--deployments a,b,...] [--caches a,b,...] [--dataset-seed N]
            [--skip-dataset] [--freshness-ms 1000] [--replica-reads 12]

Exit 0: every check passed; 1: a check failed; 2: usage or setup error (for
example the hook failed or the app is unreachable). One line per check on
stdout (`PASS name` / `FAIL name: detail`), then one JSON summary line.

The checker creates its own users (ids and names derived from the seed, far
from the dataset's), never follows or mentions dataset users, and reads the
dataset only, so it can run before or after a benchmark phase; with the
dataset check (the default) it must run on a freshly seeded deployment that no
benchmark has written to (`--skip-dataset` otherwise).

Restart hook: the checker runs the JSON argv with two extra arguments and
waits for it to exit 0:

    ARGV... restart DEPLOYMENT   delete one running pod of DEPLOYMENT; return
                                 once the deployment has all replicas Ready
                                 again and nginx answers
    ARGV... flush SERVICE        empty the cache SERVICE (Redis FLUSHALL,
                                 memcached flush_all); return when done

`--deployments` defaults to the stateless deployments above, `--caches` to
`post-storage-memcached,user-memcached,url-shorten-memcached,media-memcached,user-timeline-redis,social-graph-redis`.
`home-timeline-redis` is the only store of home timelines in the pinned app,
so it is not a cache and is never flushed.

## The contract the checker and the benchmark validation enforce

For OBJECTIVE.md; "acknowledged" means the request returned HTTP 200, the
bound B is 1 s.

1. Exact data. Every post served has the stored content: post id, creator,
   request id, timestamp, text (URLs replaced by their shortened form),
   user mentions (as a set), media, URLs; timelines are ordered newest first
   by timestamp, without duplicates; `start`/`stop` select that window.
2. User timeline: a compose acknowledged before a read of the author's user
   timeline starts is in it (read your write, no delay).
3. Home timeline: a post is in the home timeline of each follower of its
   author (and of each user it mentions) for every read that starts more than
   B after the compose was acknowledged; it is never in the home timeline of a
   user who neither follows the author nor is mentioned.
4. Follow/unfollow: a follow (unfollow) acknowledged more than B before a
   compose is sent makes that post appear in (stay out of) the follower's home
   timeline as in 3. Posts composed before the change may or may not appear in
   that follower's home timeline afterwards (push and pull designs both
   comply).
5. Every replica: 2-4 hold whichever instance serves the read.
6. Durability: acknowledged data survives a restart of any stateless pod and a
   flush of any cache listed above.

Requests in flight, or acknowledged less than B before the read, may or may
not be visible. Timestamp ties may be served in either order.
