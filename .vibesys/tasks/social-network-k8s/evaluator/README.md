# social-network-k8s evaluator

The task's Go module: dataset, seeding, load generator and checker for
DeathStarBench Social Network. `INTERFACE.md` is the contract with run.py.

| Path | What |
| --- | --- |
| `internal/dataset` | the dataset, generated from a seed (digest pinned by a test) |
| `internal/seed`, `cmd/seed` | writes it into MongoDB and Redis as the services would (schemas in `internal/seed/documents.go`) |
| `internal/apps/socialnetwork`, `cmd/loadgen` | benchmark adapter: request distributions and exact validation |
| `internal/timeline` | wire format and the page-window check shared by both |
| `internal/checker`, `cmd/checker` | accuracy checks over the public API, restart/flush durability |
| `internal/fakeapp` | in-process fake of the public API with injectable faults (tests) |

## Dataset

20,000 users `user<id>`; a directed follow graph: each user follows
1 + Pareto(2) followees (scale 12, cap 2000) drawn by Zipf 0.75 over a random
popularity order, 20% followed back: 293,926 edges, median 8 followers, up to
3,802. Posts per user Pareto(2) (scale 5, cap 400, 31% post nothing): 91,490
posts over the 180 days before 2026-06-01 with unique timestamps; text of
3-30 words from a short fixed list, 25% mention a followee (`@user<id>`), 15%
end with a URL (`http://example<n>.com`, stored shortened), 10% carry media.
Home timelines (followers and mentioned users) hold 1,322,202 entries, up to
6,960 per user.

## Why seeding writes the data tier

Through the API the dataset needs 20k registrations, 294k follows (each two
user lookups and two `$elemMatch` updates of growing arrays) and 91k composes
(each eight RPCs and a fan-out of 14 ZADDs on average). At the base's likely
rates with 2 replicas of 1 CPU (a few thousand follows/s, around a thousand
composes/s) that is about 2-4 minutes, and composes would stamp every post with
the seeding time and let the services pick ids, so the load generator could
not know the dataset's content. Writing the documents directly takes about
4 s and makes the dataset exact and digest-pinned.

## Testing

Build against the pinned engine as hotel-k8s does: extract the vibesys
repository at the Dockerfile's commit, point a temporary copy of `go.mod`
at it (`replace ... => <extract>/resources/evaluators/microservice`, same for
`sdk/vs-evaluator/vseval`), and run `go test -modfile=<copy> ./...`.
