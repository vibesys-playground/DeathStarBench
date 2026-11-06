# social-network-k8s evaluator

The task's Go module: dataset, seeding, the benchmark's request generator, and
the accuracy check's workload and oracle for DeathStarBench Social Network.
`INTERFACE.md` is the contract with bench.py.

| Path | What |
| --- | --- |
| `internal/dataset` | the dataset, generated from a seed (digest pinned by a test) |
| `internal/seed`, `cmd/seed` | writes it into MongoDB and Redis as the services would (schemas in `internal/seed/documents.go`) |
| `internal/timeline` | wire format and the page-window check |
| `internal/snapi` | the public API as the workloads and the oracle see it: an `Action` (what one call sends), its HTTP request, and an HTTP client (tests) |
| `internal/snworkload`, `cmd/generator` | the benchmark's workload on the vibesys-evaluator Go SDK (built as `sn-generator`): request distributions only |
| `internal/snaccuracy`, `cmd/accuracy-generator` | the accuracy check's workload (built as `sn-accuracy-generator`): one action per operation, its Info the action |
| `internal/snspec`, `cmd/oracle` | the contract as two `check.Spec`s (per author, per reader) on vibesys-evaluator's checker, and `check.Main` over a captured history directory (built as `sn-oracle`; see the task README, Accuracy oracle) |
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

`go.mod` requires the vibesys-evaluator Go module at the release of the evaluator image the
Dockerfile builds `FROM` (change both together; `tests/test_accuracy.py` checks they agree), so
`go test ./...` works anywhere with network access. The stream test pins the generator's request stream for two phases
(`cmd/generator/testdata`); `SN_STREAM_RECORD=1` re-records it, which is a change of the task's
requests. `internal/snspec` and `internal/snaccuracy` run the in-process fake application
(`-short` skips those runs).
