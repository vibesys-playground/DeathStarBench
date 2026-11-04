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
logins and reservation users by Zipf rank over the users, reservations by
hotel popularity.

`reserve_capacity` reserves the complete seeded capacity of a hotel for a
trial-unique far-future night (128 distinct hotels per night, a 512-night
block per trial), then verifies that one additional room is rejected.
Reservation plans are serialized because the upstream application has no
reservation deletion API and its capacity check is not transactional.
