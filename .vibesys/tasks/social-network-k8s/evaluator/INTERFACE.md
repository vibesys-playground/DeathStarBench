# social-network-k8s evaluator: interface for bench.py and run.py

This file is the contract between the evaluator module (this directory) and
the task's trusted code (`bench.py`, `run.py`, the Dockerfile and the manifest). It is
owned by the evaluator; any change to it is announced in the commit that makes
it.

Go module `vibesys/social-network-k8s-evaluator`. `go.mod` requires the
vibesys-evaluator Go SDK at the pseudo-version of the vibesys commit the Dockerfile's
`EVALUATOR_REF` names (the same commit as the loadgen binary the task image holds).

Build (in the Dockerfile, `CGO_ENABLED=0`):

    go build -o /out/sn-generator ./cmd/generator
    go build -o /out/seed         ./cmd/seed
    go build -o /out/checker      ./cmd/checker

All three are static binaries without runtime files; the dataset is generated
in-process from the dataset seed. They are on the task image's `PATH`:
bench.py's seed runs `seed`, its phase pods run `sn-generator`, and `checker`
runs where `run.py check` runs (it only needs the nginx URL and the restart
hook); run.py's own seed Job runs `seed` from a busybox image.

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

The dataset (20,000 users, 293,926 follow edges, 91,490 posts, 1,322,202
home-timeline entries; digest af53262d...; exact counts in the summary)
cannot be loaded through the API in a reasonable time (estimate in the
evaluator README), and the API would assign post ids, request ids and
timestamps itself. Written directly it takes about 4 s (measured against
local mongo/redis containers); Redis needs about 85 MB for home timelines and
20 MB for the social graph, post-storage MongoDB about 26 MB of documents. `seed` therefore writes the
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

## `sn-generator`: the benchmark's workload

`workload.Main` of the vibesys-evaluator Go SDK (`generate` and `check`, the
SDK's generator command line), with the operations `user_timeline_read`
(weight 500), `home_timeline_read` (400), `compose_post` (100: compose, then
read the author's user timeline [0, 10), read your write), `follow` (5) and
`unfollow` (5); a follow or unfollow holds its edge's exclusive key, so two
changes of one edge never overlap. `--dataset` is the file bench.py's
`dataset()` writes, `{"seed": N, "digest": "<sha256>"}`: the generator builds
the dataset of that seed and refuses one of another digest.

`check` judges the phase's responses with the oracle (`internal/oracle`): the
sample's earlier writes (the phase's history) and this phase's writes are
applied first (a write whose answer is lost or failed may have taken effect at
any later time), then each checked call in start order. Every write is
checked, and the SDK's default share (10%) of the reads.

## `checker`: the accuracy check

    checker --base-url URL --seed N|random --restart-command-json '["...", ...]'
            [--deployments a,b,...] [--caches a,b,...] [--dataset-seed N]
            [--skip-dataset] [--freshness-ms 1000] [--replica-reads 12]
            [--skip-follow-after-flush]

Exit 0: every check passed; 1: a check failed (a failing restart hook fails
the durability check); 2: usage or setup error (the app is unreachable). One line per check on
stdout (`PASS name` / `FAIL name: detail`), then one JSON summary line.

Each check is a program of the oracle (`internal/oracle/checks.go`: calls,
parallel groups of calls, restarts, flushes and waits for the bound), and it
passes when some ordering of its calls that respects their real-time order is
a run of the oracle's model that admits every answer; a failure names the
first step no ordering explains.

The checker creates its own users (ids from 3,000,000,000 and names derived
from the seed, far from the dataset's 1..20000), never follows or mentions dataset users, and reads the
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

After each flush the checker first has a new user follow an existing user,
before anything reads the flushed state, then composes as the followee. The
pinned social-graph service fails this after a `social-graph-redis` flush:
`Follow` ZADDs into `<id>:followers` whether or not the set is cached, and
`GetFollowers` treats any non-empty cached set as complete, so the older
followers stop receiving posts. Fix in the base (e.g. update the cached sets
only if they exist, or delete them on follow/unfollow), or run with
`--skip-follow-after-flush` until then.

## The contract the oracle enforces (in the checker and the benchmark's checks)

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
