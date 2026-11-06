# social-network-k8s evaluator: interface for bench.py

This file is the contract between the evaluator module (this directory) and
the task's trusted code (`bench.py`, the Dockerfile and the
manifest). It is owned by the evaluator; any change to it is announced in the
commit that makes it.

Go module `vibesys/social-network-k8s-evaluator`. `go.mod` requires the
vibesys-evaluator Go module (`github.com/uw-syfi/vibesys/sdk/vibesys-evaluator/go`:
the workload SDK and the checker) at the release of the evaluator image the
Dockerfile builds `FROM` (the same as the Python package and the loadgen the
task image holds; `tests/test_accuracy.py` checks they agree).

Build (in the Dockerfile, `CGO_ENABLED=0`):

    go build -o /out/seed                  ./cmd/seed
    go build -o /out/sn-generator          ./cmd/generator
    go build -o /out/sn-accuracy-generator ./cmd/accuracy-generator
    go build -o /out/sn-oracle             ./cmd/oracle

All four are static binaries without runtime files; the dataset is generated
in-process from the dataset seed. They are on the task image's `PATH`: the
seed Job runs `seed`, the benchmark's phase pods run `sn-generator`, the
accuracy run's phase pods `sn-accuracy-generator`, and the accuracy run runs
`sn-oracle` on the captured history where `bench.py accuracy` runs.

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

`workload.Main` of the vibesys-evaluator Go SDK (`generate`, the SDK's
generator command line), with the operations `user_timeline_read` (weight
500), `home_timeline_read` (400), `compose_post` (100: compose, then read the
author's user timeline [0, 10), read your write), `follow` (5) and `unfollow`
(5). `--dataset` is the file bench.py's `dataset()` writes,
`{"seed": N, "digest": "<sha256>"}`: the generator builds the dataset of that
seed and refuses one of another digest. It only measures: nothing checks the
answers (an answer other than 200 is an error of the window).

## `sn-accuracy-generator`: the accuracy check's workload

`workload.Main` over `internal/snaccuracy`. Every operation is one
`snapi.Action`: its Info is the action's JSON and its one step the action's
HTTP request (`snapi.Request`). The traffic concentrates on 40 hot users (the
36 most active and the 4 most followed): `compose` (18), `follow` (5),
`unfollow` (3), `home_read` (30) and `user_read` (20) of hot users,
`home_read_any` and `user_read_any` (10 each: the benchmark's draws over the
whole dataset), `register` (1: ids from 1,500,000,000 + phase << 20),
`read_registered` (1) and `invalid` (2: a non-numeric user, an empty or a
negative window, an unknown user, a compose without text). Markers and
registered users are unique per phase.

## `sn-oracle`: the accuracy check's oracle

    sn-oracle --history DIR [--out DIR] [budget flags]

`check.Main` of the vibesys-evaluator checker over two specs of
`internal/snspec`, whose verdicts are ANDed: per author (`u:<id>`: its
composes, registration, the follows that point at it, its user timeline and
every home read projected to the authors on its page; `n:<name>` for username
claims) and per reader (`r:<id>`: page arithmetic across authors). Exit 0
PASS, 1 FAIL, 2 UNKNOWN. `Spec.Fault` is nil in both: restarts and cache
flushes change nothing in the contract below.

## The contract the oracle enforces

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
   flush of any cache tier (memcached, `social-graph-redis`,
   `user-timeline-redis`). `home-timeline-redis` is the only store of home
   timelines in the pinned app, so it is not a cache and is never flushed.
7. Registration: registering a free username is acknowledged; a taken one
   never is.

Requests in flight, or acknowledged less than B before the read, may or may
not be visible. Timestamp ties may be served in either order. At the check's
fixed rate, more than 1% of unknown outcomes (lost answers and 5xx) fails.
