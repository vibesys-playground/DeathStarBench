# Hotel benchmark adapter

This package defines typed benchmark traffic for DeathStarBench's Hotel
Reservation application over the task's generated dataset
(`internal/dataset`). Expected answers come from `hotelsupport.Reference`:
search is the geo service's go-geoindex answer (5 nearest hotels within 10 km,
reproduced operation for operation) restricted to hotels with rate plans;
recommendations are the global nearest, best-rated and cheapest hotels.
Search and recommendation responses are checked as strict GeoJSON against the
dataset's profiles (float32 coordinates). Login responses require exact
messages.

Request distributions (see `operations.go`) are deterministic functions of
the sample's random word: locations by city popularity (Zipf) with a normal
spread around the centre, stays of 1-7 nights over the 2027 rate window,
logins and reservation users by Zipf rank over the users, reservations, rate
reads and writes by hotel popularity.

`reserve_capacity` reserves the complete seeded capacity of a hotel for a
trial-unique far-future night (128 distinct hotels per night, a 512-night
block per trial), then verifies that one additional room is rejected.
Reservation plans are serialized because the upstream application has no
reservation deletion API and its capacity check is not transactional.

## Writes (`writes.go`, `history.go`, `readwrite.go`)

About 1% of the mix are admin writes: `update_rate` (one plan's prices),
`update_profile` (phone, sometimes name), `set_capacity` (on the quarter of
hotels that `reserve_capacity` never books, followed by a reservation of one
room more than the new capacity, which must be rejected at once) and
`add_hotel` (in the trial's sandbox, `dataset.Sandbox`, with a rating and a
price that leave the "rate" and "price" recommendations unchanged).
`hotel_rates` reads `GET /rates`. 3% of the searches and distance
recommendations query the sandbox once it holds a visible addition.

Reads stay exact under writes. The adapter records each write it made with
the time its plan was built (no later than the request was sent) and the time
its response was validated (no earlier than it arrived). Writes to one item
are serialized (a plan draws another item while one is in flight), so an
item's versions are in application order. A read built at `start` and
validated at `end` may show, per item, the latest value acknowledged more
than 1 s before `start` (the seeded value if none) or any value written after
it whose plan was built before `end`; added hotels likewise are required or
optional, and a sandbox answer must equal the answer for the required
additions plus some subset of the optional ones. A failed write counts as
possibly applied.

`Reset`, after every trial, writes the seeded value back to every item the
adapter changed, so the next trial and the open-loop load generator (same
namespace, new process) start from the dataset. Added hotels stay; they lie
in the trial's sandbox, which nothing else queries (a trial pair shares a
cell with probability 1 in about 31,000).
