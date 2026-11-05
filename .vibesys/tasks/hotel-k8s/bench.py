"""hotel-k8s benchmark: DeathStarBench Hotel Reservation on Kubernetes, on vibesys-evaluator.

    python /task/bench.py benchmark --project-root ROOT --vs-output PATH   (one sample)

The five parts: (1) the system under test, (2) the initial data (seed),
(3) the workload (operations and the model their responses are checked
against), (4) the benchmark function, (5) the validity rules. The data is
dataset.py's; its reference answers are computed there.

Request distributions (each draw from the operation's own `Rand` stream):
locations by city popularity (Zipf, s = 1) with normal offsets of the city's
hotel spread, written with 5 decimals; stays of 1-7 nights (skewed short) in
the 2027 rate window; users by Zipf rank (s = 0.75); hotels for rates,
reservations and writes by popularity (a city by Zipf rank, then a hotel by
Zipf rank, s = 0.75, within it). About 1% of the mix are admin writes, checked
against the 1 s staleness bound of OBJECTIVE.md through `Versions`.
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import threading
import time
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import dataset as ds  # noqa: E402
import envelope  # noqa: E402
import vibesys_evaluator as vse  # noqa: E402
from vibesys_evaluator import Call, Rand, Versions  # noqa: E402
from vibesys_evaluator.deploy import k8s  # noqa: E402
from vibesys_evaluator.service import Operation, Request, Workload, get, post  # noqa: E402

# ---------------------------------------------------------------- (2) seed

MONGO = {
    "geo-db": "mongodb-geo:27018",
    "profile-db": "mongodb-profile:27019",
    "rate-db": "mongodb-rate:27020",
    "recommendation-db": "mongodb-recommendation:27021",
    "reservation-db": "mongodb-reservation:27022",
    "user-db": "mongodb-user:27023",
}
INSERT_BATCH = 1000


def seed(data: ds.Dataset) -> None:
    """Write the dataset into the six MongoDB servers (runs in the seed Job).

    It runs once the data tier is available and before the services start:
    each service seeds its own test data only into an empty collection, and
    geo and recommendation load their collection once at startup. Each
    collection is replaced: dropped, filled in batches, indexed, counted.
    """
    import pymongo  # in the task image

    collections = ds.documents(data)
    print(f"seed: documents digest {ds.documents_digest(collections)}", flush=True)
    errors: list[str] = []

    def write(database: str) -> None:
        client = pymongo.MongoClient(f"mongodb://{MONGO[database]}", serverSelectionTimeoutMS=5000)
        deadline = time.monotonic() + 300
        while True:
            try:
                client.admin.command("ping")
                break
            except pymongo.errors.PyMongoError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
        for collection in (c for c in collections if c.database == database):
            started = time.monotonic()
            db = client[database]
            db.drop_collection(collection.name)
            db.create_collection(collection.name)
            target = db[collection.name]
            documents = [dict(d) for d in collection.documents]  # insert_many adds _id
            for start in range(0, len(documents), INSERT_BATCH):
                target.insert_many(documents[start:start + INSERT_BATCH], ordered=True)
            target.create_indexes([
                pymongo.IndexModel([(key, pymongo.ASCENDING) for key in keys], name=name, unique=unique)
                for name, keys, unique in collection.indexes
            ])
            count = target.count_documents({})
            if count != len(documents):
                errors.append(f"{database}.{collection.name} holds {count} documents, want {len(documents)}")
            print(f"seed: {database}.{collection.name}: {count} documents in "
                  f"{time.monotonic() - started:.1f}s", flush=True)
        client.close()

    def guarded(database: str) -> None:
        try:
            write(database)
        except Exception as error:  # reported below, as the Job's failure
            errors.append(f"{database}: {type(error).__name__}: {error}")

    threads = [threading.Thread(target=guarded, args=(database,)) for database in MONGO]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise RuntimeError("; ".join(sorted(errors)))


# -------------------------------------------------- (1) system under test

PROBE_LOCATION = {"lat": "37.7867", "lon": "-122.4112"}

# The deployed manifest is the task's k8s/manifest.yaml (data tier, ConfigMaps, frontend
# Service) followed by the candidate's application tier (hotelReservation/deploy/app.yaml),
# which `prepare` validates against the capacity envelope and writes to DEPLOYED in this
# script's own directory before the package reads it (k8s.App takes one manifest file of
# the task directory). It must be this directory, not a copy: the package starts its pods
# with this script's path (/task/bench.py), and vibesys-cli builds the task image from
# regular files only (no symlinks). /task is the image's own writable layer in the job.
DEPLOYED = "k8s/deployed.yaml"

SYSTEM = k8s.App(
    name="hotel-k8s",
    manifest=DEPLOYED,  # ${NAMESPACE} and ${HOTEL_IMAGE} are substituted
    images={"HOTEL_IMAGE": "hotelReservation"},  # built from PROJECT_ROOT/hotelReservation
    # MongoDB, memcached, consul and jaeger: applied and available before the seed; the
    # candidate's application Deployments start after it.
    data_tier="tier=data",
    seed=seed,
    gateway="http://frontend:5000",
    ready=[
        get("/user", query={"username": "Cornell_30", "password": "0000000000"}),
        get("/recommendations", query={"require": "rate", **PROBE_LOCATION}),
        get("/hotels", query={"inDate": "2300-01-01", "outDate": "2300-01-02", **PROBE_LOCATION}),
    ],
)


# --------------------------------------------------------------- (3) model

STALENESS_SECS = 1.0  # the visibility contract of OBJECTIVE.md
# A sample books far-future nights only, outside the rate window that searches
# use: a block of RESERVATION_NIGHTS from a night drawn per sample, at most
# RESERVATIONS_PER_NIGHT distinct hotels per night, so every reservation finds
# its hotel's whole capacity free (the application cannot delete reservations).
RESERVATION_NIGHTS = 512 * 16
RESERVATIONS_PER_NIGHT = 128
FIRST_RESERVATION_NIGHT = date(3000, 1, 1)
RESERVATION_RANGE = 146097 * 17  # days of seventeen 400-year cycles: four-digit years
RESERVATION_ATTEMPTS = 16  # popularity draws of a hotel free that night, then the city's next
WRITE_ATTEMPTS = 16  # popularity draws of a capacity-managed hotel, then the city's next
SANDBOX_SHARE = 0.03  # searches and distance recommendations near an addition
SANDBOX_JITTER_KM = 0.5
MAX_OPTIONAL_ADDITIONS = 8  # beyond it a sandbox answer is only checked for membership

_REFERENCES: dict[int, ds.Reference] = {}


def reference(data: ds.Dataset) -> ds.Reference:
    """The reference over the dataset, built once per process."""
    ref = _REFERENCES.get(id(data))
    if ref is None or ref.data is not data:
        ref = _REFERENCES[id(data)] = ds.Reference(data)
    return ref


class Model:
    """The seeded data plus this sample's writes, with their times.

    Built in each phase pod from the dataset and the sample's stream, then
    every earlier write of the sample replayed into it through its `apply`, so
    a phase continues from the values the earlier ones wrote. Generation
    applies a write as pending as soon as it is planned; the check sees every
    write with its outcome and times.
    """

    def __init__(self, data: ds.Dataset, rand: Rand) -> None:
        self.data = data
        self.ref = reference(data)
        self.profiles: Versions[str, tuple[str, str]] = Versions(bound_secs=STALENESS_SECS)
        self.prices: Versions[tuple[str, str], tuple[float, float, float]] = Versions(bound_secs=STALENESS_SECS)
        self.rooms: Versions[str, int] = Versions(bound_secs=STALENESS_SECS)
        self.additions: Versions[str, ds.Hotel | None] = Versions(bound_secs=STALENESS_SECS)
        self.added: list[ds.Hotel] = []  # additions planned or sent, in order
        self.confirmed: list[ds.Hotel] = []  # additions acknowledged in earlier phases
        self.first_night = FIRST_RESERVATION_NIGHT + timedelta(rand.below(RESERVATION_RANGE - RESERVATION_NIGHTS))
        self.next_slot = 0
        self.used: dict[int, set[int]] = {}  # night -> hotel indices reserved on it
        self.next_addition = 1
        self.cell = rand.below(ds.SANDBOX_COUNT)  # the sandbox cell of this sample's additions
        self.tag = rand.below(1 << 48)  # added hotels are a<tag>-<n>

    def hotel(self, hotel_id: str) -> ds.Hotel | None:
        return self.data.by_id.get(hotel_id)


# --------------------------------------------------- (3) request distributions

STAY_NIGHTS = (30, 24, 16, 11, 8, 6, 5)  # relative weight of a stay of n+1 nights
SEARCH_ATTEMPTS = 8  # redraws of a location where geo's own ranking ties


def coordinate(value: float) -> str:
    return f"{value:.5f}"


def location(m: Model, rand: Rand) -> tuple[str, str]:
    """A city by Zipf rank, then normal offsets of the city's hotel spread from its centre."""
    city = m.data.cities[rand.zipf(len(m.data.cities), 1.0)]
    lat = city.lat + city.spread_km * rand.normal() / ds.KM_PER_DEGREE_LAT
    lon = city.lon + city.spread_km * rand.normal() / city.km_per_degree_lon
    return coordinate(lat), coordinate(lon)


def stay(rand: Rand) -> tuple[str, str]:
    nights = 1 + rand.weighted(STAY_NIGHTS)
    start = ds.RATE_WINDOW_START + timedelta(rand.below(ds.RATE_WINDOW_DAYS - nights + 1))
    return str(start), str(start + timedelta(nights))


def popular(m: Model, rand: Rand) -> tuple[int, int]:
    """(city, rank within it): a city by Zipf rank (s = 1), then a hotel by Zipf rank (s = 0.75)."""
    city = rand.zipf(len(m.data.cities), 1.0)
    return city, rand.zipf(len(m.data.cities[city].hotels), 0.75)


def popular_hotel(m: Model, rand: Rand) -> ds.Hotel:
    city, rank = popular(m, rand)
    return m.data.hotels[m.data.cities[city].hotels[rank]]


def pick_hotel(m: Model, rand: Rand, attempts: int, eligible) -> ds.Hotel:  # noqa: ANN001
    """A popular hotel that `eligible` accepts: `attempts` draws, then the first draw's city's next."""
    for attempt in range(attempts):
        city, rank = popular(m, rand.child("hotel", attempt))
        index = m.data.cities[city].hotels[rank]
        if eligible(index):
            return m.data.hotels[index]
    city, rank = popular(m, rand.child("hotel", 0))
    hotels = m.data.cities[city].hotels
    for offset in range(1, len(hotels) + 1):
        index = hotels[(rank + offset) % len(hotels)]
        if eligible(index):
            return m.data.hotels[index]
    raise ValueError(f"no eligible hotel in city {city}")


def user(m: Model, rand: Rand) -> ds.User:
    return m.data.users[rand.zipf(len(m.data.users), 0.75)]


def capacity_managed(index: int) -> bool:
    """A quarter of the hotels, fixed by index: set_capacity changes them, reservations never book them."""
    return Rand(index, "capacity-managed").below(4) == 0


def sandbox_location(m: Model, rand: Rand) -> tuple[str, str] | None:
    """3% of the time, a point within 0.5 km of an addition acknowledged in an earlier phase."""
    if rand.random() >= SANDBOX_SHARE or not m.confirmed:
        return None
    anchor = m.confirmed[rand.below(len(m.confirmed))]
    _, _, km_per_lon = ds.sandbox(m.cell)
    dy = (2 * rand.random() - 1) * SANDBOX_JITTER_KM
    dx = (2 * rand.random() - 1) * SANDBOX_JITTER_KM
    return coordinate(anchor.lat + dy / ds.KM_PER_DEGREE_LAT), coordinate(anchor.lon + dx / km_per_lon)


def number(value: float) -> str:
    """A price as Go's strconv.FormatFloat(v, 'f', -1, 64) writes it."""
    text = repr(value)
    return text[:-2] if text.endswith(".0") else text


# ---------------------------------------------------------- (3) operations


def search(m: Model, rand: Rand) -> Request:
    near = sandbox_location(m, rand.child("sandbox"))
    in_date, out_date = stay(rand.child("stay"))
    if near is not None:
        query = {"inDate": in_date, "outDate": out_date, "lat": near[0], "lon": near[1]}
        return Request(get("/hotels", query=query), info={"near": list(near)})
    for attempt in range(SEARCH_ATTEMPTS):
        lat, lon = location(m, rand.child("location", attempt))
        expected, exact = m.ref.search_candidates(lat, lon)
        if exact:
            query = {"inDate": in_date, "outDate": out_date, "lat": lat, "lon": lon}
            if rand.random() < 0.5:
                query["locale"] = "en"
            return Request(get("/hotels", query=query), info={"expected": expected})
    raise ValueError(f"no unambiguous search location in {SEARCH_ATTEMPTS} draws")


def recommend(require: str):  # noqa: ANN201
    def build(m: Model, rand: Rand) -> Request:
        near = sandbox_location(m, rand.child("sandbox")) if require == "dis" else None
        if near is not None:
            query = {"require": require, "lat": near[0], "lon": near[1]}
            return Request(get("/recommendations", query=query),
                           info={"lat": near[0], "lon": near[1], "sandbox": True})
        lat, lon = location(m, rand.child("location"))
        query = {"require": require, "lat": lat, "lon": lon}
        if rand.random() < 0.5:
            query["locale"] = "en"
        return Request(get("/recommendations", query=query), info={"lat": lat, "lon": lon})
    return build


def login(valid: bool):  # noqa: ANN201
    def build(m: Model, rand: Rand) -> Request:
        u = user(m, rand)
        password = u.password if valid else u.password + "-invalid"
        return Request(get("/user", query={"username": u.username, "password": password}))
    return build


def reserve_capacity(m: Model, rand: Rand) -> Request:
    """Reserve a hotel's whole capacity on a sample-owned night, then one room more (rejected).

    `exclusive` serializes every reservation: the service's capacity check
    and write are not transactional, so two concurrent fills could both see
    free rooms and the rejection would not be determined.
    """
    slot = m.next_slot
    night = slot // RESERVATIONS_PER_NIGHT
    if night >= RESERVATION_NIGHTS:
        raise ValueError(f"the sample used its {RESERVATION_NIGHTS * RESERVATIONS_PER_NIGHT} reservation slots")
    used = m.used.get(night, set())
    hotel = pick_hotel(m, rand, RESERVATION_ATTEMPTS, lambda i: i not in used and not capacity_managed(i))
    u = user(m, rand.child("user"))
    in_date = m.first_night + timedelta(night)
    query = {"inDate": str(in_date), "outDate": str(in_date + timedelta(1)), "hotelId": hotel.id,
             "customerName": f"vibesys-{rand.below(1 << 63):016x}", "username": u.username,
             "password": u.password}
    return Request(
        get("/reservation", query={**query, "number": str(hotel.capacity)}),
        get("/reservation", query={**query, "number": "1"}),
        info={"slot": slot, "hotel": int(hotel.id) - 1},
        exclusive="reservation",
    )


def update_rate(m: Model, rand: Rand) -> Request:
    """One plan of a popular hotel, its prices scaled by 0.8-1.25."""
    hotel = popular_hotel(m, rand.child("hotel"))
    plan = hotel.rate_plans[rand.below(len(hotel.rate_plans))]
    total = ds.go_round(plan.total_rate * (0.8 + 0.45 * rand.random()), 100)
    prices = (total, total, ds.go_round(total * plan.total_rate_inclusive / plan.total_rate, 100))
    query = {"hotelId": hotel.id, "code": plan.code, "bookableRate": number(prices[0]),
             "totalRate": number(prices[1]), "totalRateInclusive": number(prices[2])}
    # Writes to one item never overlap, so its versions are in application order.
    return Request(post("/admin/rate", query=query), exclusive=f"rate:{hotel.id}:{plan.code}",
                   info={"hotel": hotel.id, "code": plan.code, "prices": list(prices)})


def update_profile(m: Model, rand: Rand) -> Request:
    """A new phone in the city's format; in 10% of the updates a new name too."""
    hotel = popular_hotel(m, rand.child("hotel"))
    name, _ = m.profiles.latest(hotel.id, (hotel.name, hotel.phone))
    phone = ds.new_phone(m.data, hotel, rand.child("phone"))
    query = {"hotelId": hotel.id, "phone": phone}
    if rand.random() < 0.1:
        name = query["name"] = ds.new_name(m.data, hotel, rand.child("name"))
    return Request(post("/admin/profile", query=query), exclusive=f"profile:{hotel.id}",
                   info={"hotel": hotel.id, "name": name, "phone": phone})


def set_capacity(m: Model, rand: Rand) -> Request:
    """A capacity-managed hotel's rooms to 0.5-2x the seeded ones, then one room more: rejected at once."""
    hotel = pick_hotel(m, rand, WRITE_ATTEMPTS, capacity_managed)
    low, high = max(1, hotel.capacity // 2), 2 * hotel.capacity
    rooms = low + int(rand.random() * (high - low + 1))
    u = user(m, rand.child("user"))
    probe = {"inDate": str(m.first_night), "outDate": str(m.first_night + timedelta(1)),
             "hotelId": hotel.id, "customerName": f"vibesys-capacity-{rand.below(1 << 63):016x}",
             "username": u.username, "password": u.password, "number": str(rooms + 1)}
    return Request(
        post("/admin/capacity", query={"hotelId": hotel.id, "rooms": str(rooms)}),
        get("/reservation", query=probe),
        exclusive=f"capacity:{hotel.id}",
        info={"hotel": hotel.id, "rooms": rooms},
    )


def addition(m: Model, n: int, seed_: int) -> ds.Hotel:
    """The n-th hotel this sample adds, from its seed, in the sample's sandbox cell.

    Its rating stays below the best and its price above the cheapest, so the
    "rate" and "price" recommendations of the namespace never change.
    """
    lat, lon = ds.sandbox_point(m.cell, Rand(seed_, "point"))
    hotel = ds.added_hotel(f"a{m.tag:x}-{n}", Rand(seed_, "hotel"), lat, lon)
    return replace(hotel, rating=min(hotel.rating, ds.go_round(m.ref.max_rating - 0.01, 100)),
                   price=max(hotel.price, ds.go_round(m.ref.min_price + 0.01, 100)))


def add_hotel(m: Model, rand: Rand) -> Request:
    n, seed_ = m.next_addition, rand.below(1 << 62)
    h = addition(m, n, seed_)
    a = h.address
    body = {
        "hotelId": h.id, "name": h.name, "phoneNumber": h.phone, "description": h.description,
        "address": {"streetNumber": a.street_number, "streetName": a.street_name, "city": a.city,
                    "state": a.state, "country": a.country, "postalCode": a.postal_code},
        "lat": h.lat, "lon": h.lon, "rating": h.rating, "price": h.price, "rooms": h.capacity,
        "ratePlans": [{"code": p.code, "inDate": p.in_date, "outDate": p.out_date,
                       "roomType": {"code": p.room_code, "roomDescription": p.room_description,
                                    "bookableRate": p.bookable_rate, "totalRate": p.total_rate,
                                    "totalRateInclusive": p.total_rate_inclusive}}
                      for p in h.rate_plans],
    }
    return Request(post("/admin/hotel", json=body), info={"n": n, "seed": seed_})


def rates(m: Model, rand: Rand) -> Request:
    hotel = popular_hotel(m, rand)
    return Request(get("/rates", query={"hotelId": hotel.id}), info={"hotel": hotel.id})


# ------------------------------------------------- (3) apply: writes -> model
# Pending (call.ok None, no times) while the phase is generated; with the
# outcome and times when the history is replayed. A failed write counts as
# possibly applied (Versions).


def apply_rate(m: Model, call: Call) -> None:
    m.prices.write((call.info["hotel"], call.info["code"]), tuple(call.info["prices"]), call)


def apply_profile(m: Model, call: Call) -> None:
    m.profiles.write(call.info["hotel"], (call.info["name"], call.info["phone"]), call)


def apply_capacity(m: Model, call: Call) -> None:
    m.rooms.write(call.info["hotel"], call.info["rooms"], call)


def apply_reservation(m: Model, call: Call) -> None:
    slot = call.info["slot"]
    m.next_slot = max(m.next_slot, slot + 1)
    m.used.setdefault(slot // RESERVATIONS_PER_NIGHT, set()).add(call.info["hotel"])


def apply_addition(m: Model, call: Call) -> None:
    n = call.info["n"]
    hotel = addition(m, n, call.info["seed"])
    m.next_addition = max(m.next_addition, n + 1)
    m.added.append(hotel)
    m.additions.write(hotel.id, hotel, call)
    if call.ok:
        m.confirmed.append(hotel)


# ------------------------------------------------------ (3) check: responses
# check(model, call) -> None or what is wrong; after the phase, in start
# order, for every write and 10% of the reads. Responses are checked strictly:
# exact JSON fields, values and the data or values the writes allow.

LOGIN_SUCCESS = "Login successfully!"
LOGIN_FAILURE = "Failed. Please check your username and password. "
RESERVATION_SUCCESS = "Reserve successfully!"
RESERVATION_FAILURE = "Failed. Already reserved. "


def fields(value: object, want: list[str], what: str) -> str | None:
    if not isinstance(value, dict):
        return f"{what} must be an object, got {type(value).__name__}"
    if sorted(value) != want:
        return f"{what} fields {sorted(value)}, want {want}"
    return None


def message(call: Call, step: int, expected: str) -> str | None:
    try:
        value = call.responses[step].json()
    except ValueError as error:
        return f"step {step + 1}: not JSON ({error})"
    problem = fields(value, ["message"], f"step {step + 1} response")
    if problem is None and value["message"] != expected:
        problem = f"step {step + 1}: message {value['message']!r}, want {expected!r}"
    return problem


def window(call: Call) -> tuple[float, float]:
    assert call.start is not None and call.end is not None
    return call.start, call.end


def check_features(m: Model, call: Call, accept, added: tuple[ds.Hotel, ...] = ()) -> str | None:  # noqa: ANN001
    """A strict GeoJSON FeatureCollection: profiles the writes allow, float32 coordinates, IDs `accept` takes."""
    start, end = window(call)
    try:
        root = call.responses[0].json()
    except ValueError as error:
        return f"not JSON ({error})"
    problem = fields(root, ["features", "type"], "GeoJSON root")
    if problem:
        return problem
    if root["type"] != "FeatureCollection":
        return f"GeoJSON type {root['type']!r}, want FeatureCollection"
    if not isinstance(root["features"], list):
        return "GeoJSON features must be an array"
    by_id = {h.id: h for h in added}
    ids: list[str] = []
    for n, feature in enumerate(root["features"]):
        problem = fields(feature, ["geometry", "id", "properties", "type"], f"feature {n}")
        if problem:
            return problem
        if feature["type"] != "Feature" or not isinstance(feature["id"], str):
            return f"feature {n}: type {feature['type']!r} and id {feature['id']!r}"
        hotel = m.hotel(feature["id"])
        if hotel is not None:
            allowed = m.profiles.allowed(hotel.id, (hotel.name, hotel.phone), start=start, end=end)
        elif feature["id"] in by_id:
            hotel = by_id[feature["id"]]
            allowed = ((hotel.name, hotel.phone),)
        else:
            return f"feature {n}: unknown hotel {feature['id']!r}"
        properties = feature["properties"]
        problem = fields(properties, ["name", "phone_number"], f"feature {n} properties")
        if problem:
            return problem
        if (properties["name"], properties["phone_number"]) not in allowed:
            return f"hotel {hotel.id}: profile {properties}, allowed {allowed}"
        geometry = feature["geometry"]
        problem = fields(geometry, ["coordinates", "type"], f"feature {n} geometry")
        if problem:
            return problem
        coordinates = geometry["coordinates"]
        if geometry["type"] != "Point" or not isinstance(coordinates, list) or len(coordinates) != 2:
            return f"hotel {hotel.id}: geometry {geometry}"
        if tuple(coordinates) != m.ref.encoded(hotel):
            return f"hotel {hotel.id}: coordinates {coordinates}, want {list(m.ref.encoded(hotel))}"
        if hotel.id in ids:
            return f"hotel {hotel.id} twice"
        ids.append(hotel.id)
    return accept(sorted(ids))


def exactly(expected: list[str]):  # noqa: ANN201
    want = sorted(expected)
    return lambda ids: None if ids == want else f"hotels {ids}, want exactly {want}"


def additions_at(m: Model, call: Call) -> tuple[list[ds.Hotel], list[ds.Hotel]]:
    """(required, optional): the additions a read over [start, end] must, or may, see."""
    start, end = window(call)
    required, optional = [], []
    for hotel in m.added:
        allowed = m.additions.allowed(hotel.id, None, start=start, end=end)
        if allowed == (hotel,):
            required.append(hotel)
        elif hotel in allowed:
            optional.append(hotel)
    return required, optional


def subset_of(hotels: list[ds.Hotel], limit: int):  # noqa: ANN201
    allowed = {h.id for h in hotels}

    def accept(ids: list[str]) -> str | None:
        if len(ids) > limit:
            return f"{len(ids)} hotels, at most {limit}"
        stray = [i for i in ids if i not in allowed]
        return f"hotels {stray} not among {sorted(allowed)}" if stray else None
    return accept


def sandbox_search(m: Model, lat: str, lon: str, required: list[ds.Hotel], optional: list[ds.Hotel]):  # noqa: ANN201
    """Accept the answer for the required additions plus some subset of the optional ones."""
    if len(optional) > MAX_OPTIONAL_ADDITIONS:
        return subset_of(required + optional, ds.GEO_MAX_RESULTS)

    def accept(ids: list[str]) -> str | None:
        answers = []
        for mask in range(1 << len(optional)):
            hotels = required + [h for bit, h in enumerate(optional) if mask >> bit & 1]
            candidates, exact = m.ref.search_candidates(lat, lon, tuple(hotels))
            if not exact:  # geo's ranking ties: the set is not determined
                return subset_of(hotels, ds.GEO_MAX_RESULTS)(ids)
            if sorted(candidates) == ids:
                return None
            answers.append(sorted(candidates))
        return f"sandbox search answered {ids}, want one of {answers}"
    return accept


def sandbox_nearest(m: Model, lat: float, lon: float, required: list[ds.Hotel], optional: list[ds.Hotel]):  # noqa: ANN201
    """Accept the hotels at one minimal distance over the seeded, required and some optional ones."""
    seeded, seeded_distance = m.ref.nearest(lat, lon)
    distances = {i: seeded_distance for i in seeded}
    mandatory = set(seeded)
    for hotel in required:
        distances[hotel.id] = ds.recommend_distance_km(lat, lon, hotel)
        mandatory.add(hotel.id)
    for hotel in optional:
        distances[hotel.id] = ds.recommend_distance_km(lat, lon, hotel)

    def accept(ids: list[str]) -> str | None:
        if not ids:
            return "distance recommendation answered no hotel"
        if any(i not in distances for i in ids) or len({distances[i] for i in ids}) != 1:
            return f"distance recommendation answered {ids}, not hotels at one minimal distance"
        distance = distances[ids[0]]
        for i, d in distances.items():
            if i in mandatory and (d < distance or (d == distance and i not in ids)):
                return f"distance recommendation answered {ids} at {distance:.6f} km, but {i} is at {d:.6f} km"
        return None
    return accept


def check_search(m: Model, call: Call) -> str | None:
    if "expected" in call.info:
        return check_features(m, call, exactly(call.info["expected"]))
    lat, lon = call.info["near"]
    required, optional = additions_at(m, call)
    return check_features(m, call, sandbox_search(m, lat, lon, required, optional), tuple(required + optional))


def check_recommendation(require: str):  # noqa: ANN201
    def check(m: Model, call: Call) -> str | None:
        if require == "rate":
            return check_features(m, call, exactly(m.ref.best_rated))
        if require == "price":
            return check_features(m, call, exactly(m.ref.cheapest))
        lat, lon = float(call.info["lat"]), float(call.info["lon"])
        if not call.info.get("sandbox"):
            return check_features(m, call, exactly(m.ref.nearest(lat, lon)[0]))
        required, optional = additions_at(m, call)
        return check_features(m, call, sandbox_nearest(m, lat, lon, required, optional),
                              tuple(required + optional))
    return check


def check_login(valid: bool):  # noqa: ANN201
    def check(m: Model, call: Call) -> str | None:
        return message(call, 0, LOGIN_SUCCESS if valid else LOGIN_FAILURE)
    return check


def check_reservation(m: Model, call: Call) -> str | None:
    return message(call, 0, RESERVATION_SUCCESS) or message(call, 1, RESERVATION_FAILURE)


def check_write(expected: str):  # noqa: ANN201
    def check(m: Model, call: Call) -> str | None:
        return message(call, 0, expected)
    return check


def check_capacity(m: Model, call: Call) -> str | None:
    return message(call, 0, "Capacity updated.") or message(call, 1, RESERVATION_FAILURE)


def check_rates(m: Model, call: Call) -> str | None:
    """Every seeded plan once, with its dates and room, and prices the writes allow."""
    start, end = window(call)
    hotel = m.data.by_id[call.info["hotel"]]
    try:
        root = call.responses[0].json()
    except ValueError as error:
        return f"not JSON ({error})"
    problem = fields(root, ["hotelId", "ratePlans"], "rates response")
    if problem:
        return problem
    if root["hotelId"] != hotel.id or not isinstance(root["ratePlans"], list):
        return f"rates of hotel {root['hotelId']!r}, want {hotel.id}"
    if len(root["ratePlans"]) != len(hotel.rate_plans):
        return f"hotel {hotel.id} has {len(root['ratePlans'])} rate plans, want {len(hotel.rate_plans)}"
    plans = {p.code: p for p in hotel.rate_plans}
    seen: set[str] = set()
    for n, got in enumerate(root["ratePlans"]):
        problem = fields(got, ["code", "inDate", "outDate", "roomType"], f"rate plan {n}")
        if problem:
            return problem
        plan = plans.get(got["code"])
        if plan is None or got["code"] in seen:
            return f"hotel {hotel.id}: rate plan {got['code']!r} unknown or listed twice"
        seen.add(plan.code)
        if (got["inDate"], got["outDate"]) != (plan.in_date, plan.out_date):
            return f"plan {plan.code} of hotel {hotel.id}: dates {got['inDate']}-{got['outDate']}"
        room = got["roomType"]
        problem = fields(room, ["bookableRate", "code", "roomDescription", "totalRate", "totalRateInclusive"],
                         f"plan {plan.code} roomType")
        if problem:
            return problem
        if (room["code"], room["roomDescription"]) != (plan.room_code, plan.room_description):
            return f"plan {plan.code} of hotel {hotel.id}: room {room['code']} {room['roomDescription']!r}"
        prices = (room["bookableRate"], room["totalRate"], room["totalRateInclusive"])
        if not all(isinstance(p, (int, float)) and not isinstance(p, bool) for p in prices):
            return f"plan {plan.code} of hotel {hotel.id}: prices {prices} are not numbers"
        seeded = (plan.bookable_rate, plan.total_rate, plan.total_rate_inclusive)
        allowed = m.prices.allowed((hotel.id, plan.code), seeded, start=start, end=end)
        if tuple(float(p) for p in prices) not in allowed:
            return f"plan {plan.code} of hotel {hotel.id}: prices {prices}, allowed {allowed}"
    return None


# ------------------------------------------------------------- (3) workload

WORKLOAD = Workload(
    model=Model,
    operations=[
        Operation(name="search_hotels", weight=6000, build=search, check=check_search),
        Operation(name="recommend_distance", weight=1300, build=recommend("dis"),
                  check=check_recommendation("dis")),
        Operation(name="recommend_rate", weight=1300, build=recommend("rate"),
                  check=check_recommendation("rate")),
        Operation(name="recommend_price", weight=1300, build=recommend("price"),
                  check=check_recommendation("price")),
        Operation(name="login_valid", weight=30, build=login(True), check=check_login(True)),
        Operation(name="login_invalid", weight=20, build=login(False), check=check_login(False)),
        Operation(name="reserve_capacity", weight=50, build=reserve_capacity,
                  check=check_reservation, apply=apply_reservation),
        Operation(name="hotel_rates", weight=400, build=rates, check=check_rates),
        Operation(name="update_rate", weight=42, build=update_rate,
                  check=check_write("Rate updated."), apply=apply_rate),
        Operation(name="update_profile", weight=42, build=update_profile,
                  check=check_write("Profile updated."), apply=apply_profile),
        Operation(name="set_capacity", weight=12, build=set_capacity,
                  check=check_capacity, apply=apply_capacity),
        Operation(name="add_hotel", weight=8, build=add_hotel,
                  check=check_write("Hotel added."), apply=apply_addition),
    ],
)


# ------------------------------------------------------------ (4) benchmark

CLIENTS = 32
CLOSED_TRIALS, CLOSED_SECS, CLOSED_WARMUP = 2, 60, 15
OPEN_TRIALS, OPEN_SECS, OPEN_WARMUP = 3, 60, 10
OPEN_LOOP_LOAD_FRACTION = 0.7
# The closed loop's plan is sized for this many operations/s (a faster
# candidate's phase is rerun once with a larger plan); later trials plan for
# twice what the earlier ones reached.
MIN_PLANNED_OPS_PER_SEC = 2000.0


def benchmark(run: vse.Run) -> None:
    """Capacity in a closed loop, then the p99 latency at 70% of it in an open loop.

    Two closed-loop trials (32 clients, 15 s warmup, 60 s): the throughput is
    their median. Three open-loop trials (10 s warmup, 60 s, evenly spaced
    arrivals as before) at 0.7 x that throughput: the p99 is their median.
    The sample's writes carry from phase to phase, so nothing is reset.
    """
    closed: list[float] = []
    planned = MIN_PLANNED_OPS_PER_SEC
    for _ in range(CLOSED_TRIALS):
        phase = run.closed_loop(clients=CLIENTS, secs=CLOSED_SECS, warmup=CLOSED_WARMUP, max_ops_per_sec=planned)
        closed.append(phase.ops_per_sec)
        planned = max(planned, 2 * phase.ops_per_sec)
    throughput = statistics.median(closed)
    # p99 is measured at a fixed fraction of this sample's own throughput, so
    # a faster candidate is held to proportionally more load.
    rate = OPEN_LOOP_LOAD_FRACTION * throughput
    p99 = statistics.median(
        run.open_loop(rate=rate, secs=OPEN_SECS, warmup=OPEN_WARMUP, arrivals="uniform").p99_ms
        for _ in range(OPEN_TRIALS)
    )
    run.report({"operations_per_second": throughput, "latency_ms.p99": p99})


# ------------------------------------------------------------- (5) validity

_ENVELOPE_ERROR: list[envelope.EnvelopeError] = []


def prepare(argv: list[str], target: Path) -> envelope.App | None:
    """For `benchmark`: validate the candidate's application tier and write the deployed manifest.

    A violation is kept and raised by `dataset()`, the first thing a sample calls inside
    its error handling and before it builds or deploys anything, so that the sample fails
    with the validator's message in its records.
    """
    if argv[:1] != ["benchmark"]:
        return None
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--project-root", type=Path, required=True)
    args, _ = parser.parse_known_args(argv[1:])
    target.unlink(missing_ok=True)
    try:
        app = envelope.load(args.project_root.resolve(), log=lambda line: print(line, file=sys.stderr))
    except envelope.EnvelopeError as error:
        _ENVELOPE_ERROR.append(error)
        return None
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(envelope.compose(app))
    os.replace(temporary, target)
    return app


def dataset() -> ds.Dataset:
    if _ENVELOPE_ERROR:
        raise _ENVELOPE_ERROR[0]
    return ds.generate()


if __name__ == "__main__":
    prepare(sys.argv[1:], Path(__file__).resolve().parent / DEPLOYED)
    vse.main(
        dataset=dataset,
        system=SYSTEM,
        workload=WORKLOAD,
        benchmark=benchmark,
        metrics={
            "operations_per_second": vse.Metric(unit="ops/s", direction="max"),
            "latency_ms.p99": vse.Metric(unit="ms", direction="min"),
        },
        # No error and no failed check in any window, every operation of the mix
        # completed, and the open loop offers at least 95% of its rate.
        validity=vse.Validity(max_error_rate=0.0, min_sustained_ratio=0.95, min_ops_per_operation=1),
    )
