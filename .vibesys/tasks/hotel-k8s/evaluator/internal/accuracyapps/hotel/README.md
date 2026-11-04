# Hotel accuracy oracle

The Hotel accuracy adapter owns an independent compatibility oracle for the
pinned DeathStarBench Hotel Reservation API. Existing targeted checks validate
strict response schemas, the seeded catalog, negative requests, state
transitions, and managed crash recovery.

The seeded data is the task's generated dataset (`internal/dataset`, written
by the seed Job). Its read-only answers come from `hotelsupport.Reference`:
search candidates are the geo service's go-geoindex answer (the 5 nearest
hotels within 10 km, approximate, reproduced exactly) restricted to hotels
with rate plans; recommendations are the global nearest, best-rated and
cheapest hotels; logins and capacities are the dataset's. Checks sample
hotels and users from the dataset (80 distance probes at hotel coordinates,
fuzzed searches within about 100 m of random hotels on rate-window dates).

`hotelOracle` is the canonical application-specific reference behavior. Given a
logical `hotelState` and one `differentialAction`, its `Step` method returns the
required observation and successor state. Its `AfterCrash` method preserves
acknowledged reservations. The oracle does not issue requests or schedule
events.

Generators produce shared `accuracy.Program` values. A program contains calls,
parallel call groups, and crash/start events, but no expected results. The
Hotel candidate adapter translates call actions into HTTP requests. The shared
verifier schedules those events, records a replayable trace, and accepts an
execution exactly when the observations match some execution admitted by
`hotelOracle`.

The model preserves externally observable reference behavior, including
capacity by hotel and night, atomic over-capacity rejection, search visibility,
hotel/date isolation, and the pinned frontend behavior where an invalid-login
reservation may mutate capacity before returning the authentication failure.
GeoJSON object and feature order are normalized, while schemas, profile values,
messages, and result membership remain exact.

This package does not model internal gRPC calls, databases, caches, or service
topology; the write checks model only the 1 s visibility bound.

## Event programs

Sequential differential, concurrency, and durability checks use the same
reference behavior and request adapter. Each generator owns a disjoint slice of
the seeded date namespace (see namespaces.go), because the public API has no
delete operation. Hotel accuracy accepts at most 25 cases; larger runs would
make the per-case multi-night range overlap the fixed endpoint-liveness range.
The default remains four required cases plus up to three randomized extras.

| Property | What it asserts |
| --- | --- |
| `sequential_differential` | A generated sequence of login, recommendation, search, and reservation calls matches the canonical state machine. |
| `degenerate_date_ranges` | A range enclosing no nights (`inDate == outDate`, or reversed) is acknowledged and consumes nothing. |
| `multi_night_atomicity` | A span over a full interior night is rejected whole, and the flanking nights keep their rooms. |
| `concurrent_isolation` | Simultaneous single-room reservations on distinct hotels all succeed and each consumes exactly one room. |
| `crash_recovery` | A filled hotel remains full across explicit crash and start events. |
| `durable_capacity` | Acknowledged reservations remain visible to the same model after program crash/start events. |

The shared verifier checks a parallel group against legal serializations of the
ordinary `hotelOracle.Step` transition. Contention fills a night to `k` remaining
rooms, then issues between six and eight simultaneous single-room requests.
Every legal serialization acknowledges exactly `k`. Follow-up calls pin the
resulting capacity through the same oracle rather than a separate concurrency
predicate.

Durability programs place crash and start between acknowledged writes and
readback calls. The managed lifecycle hooks perform the real process events;
`hotelOracle.AfterCrash` defines which logical state must survive. Search and
reservation readbacks are ordinary actions judged by `Step`.

## Writes

`writes.go` checks the owner routes against the contract in the task's
OBJECTIVE.md, with `hotelsupport.State` (the dataset plus the writes) as the
model. These checks run after all others, since they change seeded answers,
and use the dates from `writesOffset` and hotels in a sandbox cell of their own
for additions.

| Property | What it asserts |
| --- | --- |
| `write_validation` | GET on an owner route is 405; missing or negative parameters and malformed bodies are 400; unknown hotels and plans are 404; none of them changes anything. |
| `write_rate_visibility` | New prices of a plan (one plan written twice) are served by `/rates` at 1 s + 50 ms, in 8 reads; a read right after the write shows a value acknowledged within the last second or the one before. |
| `write_profile_visibility` | A new phone (and once a new name) shows in search and distance recommendations, under the same rule. |
| `write_capacity` | A capacity below a night's reserved rooms is 409 and changes nothing; equal is accepted; an increase and a decrease apply to the next reservations (8 probes); capacity 0 hides a hotel from search within the bound and restoring it shows it again. |
| `write_hotel_addition` | A hotel rated above and priced below every other appears in search, the three recommendations, `/rates` and reservations within the bound; a second one next to it joins the search; a taken ID is 409 and an invalid one 400. |
| `durable_writes` | After crash and start, every write above is served again (no restarted cache or index brings an old value back), and capacities still hold. Needs the lifecycle hooks. |

Repeated reads are the replica check: internal calls round-robin over the
instances of a service, so 8 reads reach both instances of a two-replica
service unless the same one is chosen 8 times. The tests run the check against
a two-replica fake whose second replica never sees one kind of write, and
expect it to fail.

## Opt-in properties the pinned reference violates

Three properties are declared but only checked when a workload asks for them,
because unmodified DeathStarBench fails all three. Turning one on rejects the
reference implementation, not just a regressed candidate.

```toml
[application_config]
strict_linearizable_capacity = true
strict_durable_availability = true
strict_endpoint_liveness = true
```

| Property | Upstream behavior |
| --- | --- |
| `linearizable_capacity` | `MakeReservation` reads the room count from memcached, releases it, and writes back after the capacity test, so simultaneous requests oversell. |
| `durable_availability` | `CheckAvailability` serves availability from cached counts; its datastore miss path is unreachable, so a full hotel is advertised again once the cache tier restarts. |
| `endpoint_liveness` | The capacity lookup calls `log.Panic` when no seeded row matches, so one request for an out-of-catalog hotel takes the reservation service down. |

Issue [#254](https://github.com/uw-syfi/vibesys/issues/254) records the measured
counterexamples, the exact commands, and what they imply for using this oracle
as an accuracy gate.

## Scope

Interleaved read/write coherence inside one parallel group, in-flight requests
interrupted by a crash, and multi-key transactional histories remain
unmodeled. Version 1 programs permit lifecycle events only at quiescent step
boundaries and bound parallel groups to eight calls.
