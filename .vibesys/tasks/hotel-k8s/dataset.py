"""The hotel-k8s dataset: hotels, rate plans and users, generated from SEED.

This module is the single owner of the task's data. `generate()` builds it;
vibesys-evaluator writes it as the sample's dataset file (`dataset.json`),
from which the Go programs read it (the benchmark's request generator and the
accuracy oracle, evaluator/internal/dataset), and bench.py's seed writes
`documents()` into MongoDB. What the workload derives from the data (the
values writes put in, the reference answers of the read paths) is Go:
evaluator/internal/dataset and internal/appsupport/hotel.

Generation is a pure function of SEED and uses only IEEE-exact arithmetic
(+, -, *, /, sqrt, rounding) on its own `Rand` streams (one per hotel, per
city and for the users), so it is byte-identical on every machine; it equals
the task's earlier Go generator draw for draw (tests/test_dataset.py pins the
MongoDB documents' digest).

Shape: 20 cities in popularity order; 25,000 hotels spread over them by Zipf
rank (exponent 1), each near its city centre (Irwin-Hall normal offsets of
sigma 1.5-4 km growing with the city's size, 15% of the hotels at 2.5 sigma),
coordinates rounded to 6 decimals and unique; 1-5 rate plans each (RACK over
the whole 2027 rate window, the others over 14-180 days of it); a rating
1 + 4 Beta(5, 2) rounded to 0.01; a room capacity by star class; a popularity
rank within the city; 10,000 users with unique names and random passwords.
Hotel IDs are "1".."25000" in a random order of cities.
"""

from __future__ import annotations

import hashlib
import json
import math
import struct
import sys
from dataclasses import asdict, dataclass, replace
from datetime import date, timedelta
from pathlib import Path

from vibesys_evaluator import Rand

# Changing SEED or any constant below changes the task: results measured on
# different datasets do not compare.
SEED = 0x686F74656C2D7631  # "hotel-v1"
HOTEL_COUNT = 25_000
USER_COUNT = 10_000
RATE_WINDOW_START = date(2027, 1, 1)
RATE_WINDOW_DAYS = 365
KM_PER_DEGREE_LAT = 111.32
OUTSKIRTS_SHARE = 0.15
OUTSKIRTS_SPREAD = 2.5


# ----------------------------------------------------------------- the data
# The dataset file is these dataclasses, field by field (snake_case names);
# the Go oracle's internal/dataset reads it with the same names.


@dataclass(frozen=True)
class RatePlan:
    code: str
    in_date: str
    out_date: str
    room_code: str
    room_description: str
    bookable_rate: float
    total_rate: float
    total_rate_inclusive: float


@dataclass(frozen=True)
class Address:
    street_number: str
    street_name: str
    city: str
    state: str
    country: str
    postal_code: str


@dataclass(frozen=True)
class Hotel:
    id: str
    city: int  # index into Dataset.cities; -1 for a hotel a write adds
    name: str
    phone: str
    description: str
    address: Address
    lat: float  # as geo-db and recommendation-db store it; profile-db stores float32
    lon: float
    stars: int
    rating: float  # recommendation-db "rate"
    price: float  # recommendation-db "price": the cheapest plan's total rate
    capacity: int  # reservation-db "numberOfRoom"
    popularity: int  # 1-based rank within the city; 0 for an added hotel
    rate_plans: tuple[RatePlan, ...]


@dataclass(frozen=True)
class User:
    username: str
    password: str  # plaintext; user-db stores its SHA-256


@dataclass(frozen=True)
class City:
    name: str
    region: str
    country: str
    lat: float  # the centre
    lon: float
    phone_format: str  # 'x' becomes a digit, 'A' an upper-case letter
    districts: tuple[str, ...]  # hotel names and descriptions draw from them
    spread_km: float
    km_per_degree_lon: float
    hotels: tuple[int, ...]  # indices into Dataset.hotels, most popular first


@dataclass(frozen=True)
class Dataset:
    seed: int
    cities: tuple[City, ...]
    hotels: tuple[Hotel, ...]  # hotels[i].id == str(i + 1)
    users: tuple[User, ...]


# ------------------------------------------------------------------ tables


@dataclass(frozen=True)
class CityTemplate:
    name: str
    region: str
    country: str
    lat: float
    lon: float
    phone: str
    postal: str
    base_price: float  # a typical nightly rate of a three-star room
    tax: float  # the share added for the tax-inclusive rate
    districts: tuple[str, ...]
    streets: tuple[str, ...]


def _city(name, region, country, lat, lon, phone, postal, price, tax, districts, streets):
    return CityTemplate(name, region, country, lat, lon, phone, postal, float(price), float(tax),
                        tuple(districts), tuple(streets))


# In popularity order: hotels and request locations follow Zipf rank.
CITY_TABLE: tuple[CityTemplate, ...] = (
    _city("London", "England", "United Kingdom", 51.5074, -0.1278, "+44 20 xxxx xxxx", "WAx xAA", 210, 0.20,
          ["Covent Garden", "Soho", "Mayfair", "South Bank", "Kensington"],
          ["Strand", "Oxford St", "Piccadilly", "Regent St", "Baker St", "Shaftesbury Ave"]),
    _city("Paris", "Ile-de-France", "France", 48.8566, 2.3522, "+33 1 xx xx xx xx", "750xx", 200, 0.10,
          ["Le Marais", "Saint-Germain", "Montmartre", "Opera", "Latin Quarter"],
          ["Rue de Rivoli", "Boulevard Haussmann", "Rue Saint-Honore", "Avenue de l'Opera", "Rue de la Paix"]),
    _city("New York", "NY", "United States", 40.7580, -73.9855, "(212) xxx-xxxx", "100xx", 260, 0.1475,
          ["Midtown", "Times Square", "Chelsea", "the Financial District", "SoHo"],
          ["Broadway", "7th Ave", "Lexington Ave", "W 44th St", "Park Ave", "Madison Ave"]),
    _city("Tokyo", "Tokyo", "Japan", 35.6812, 139.7671, "+81 3-xxxx-xxxx", "1xx-xxxx", 170, 0.10,
          ["Ginza", "Shinjuku", "Shibuya", "Asakusa", "Marunouchi"],
          ["Chuo-dori", "Harumi-dori", "Meiji-dori", "Yasukuni-dori", "Sotobori-dori"]),
    _city("Bangkok", "Bangkok", "Thailand", 13.7563, 100.5018, "+66 2 xxx xxxx", "10xxx", 70, 0.17,
          ["Sukhumvit", "Silom", "Siam", "the Riverside", "Old Town"],
          ["Sukhumvit Rd", "Silom Rd", "Rama I Rd", "Charoen Krung Rd", "Ratchadamri Rd"]),
    _city("Dubai", "Dubai", "United Arab Emirates", 25.2048, 55.2708, "+971 4 xxx xxxx", "00000", 160, 0.20,
          ["Downtown", "Dubai Marina", "Deira", "Jumeirah", "Business Bay"],
          ["Sheikh Zayed Rd", "Al Wasl Rd", "Jumeirah Beach Rd", "Al Maktoum Rd", "Financial Centre Rd"]),
    _city("Singapore", "Singapore", "Singapore", 1.2903, 103.8520, "+65 6xxx xxxx", "0xxxxx", 190, 0.19,
          ["Marina Bay", "Orchard", "Chinatown", "Clarke Quay", "Bugis"],
          ["Orchard Rd", "Raffles Ave", "Beach Rd", "Bras Basah Rd", "Havelock Rd"]),
    _city("Istanbul", "Istanbul", "Turkey", 41.0082, 28.9784, "+90 212 xxx xx xx", "34xxx", 90, 0.10,
          ["Sultanahmet", "Beyoglu", "Taksim", "Besiktas", "Karakoy"],
          ["Istiklal Cd", "Divan Yolu Cd", "Cumhuriyet Cd", "Ordu Cd", "Kennedy Cd"]),
    _city("Rome", "Lazio", "Italy", 41.9028, 12.4964, "+39 06 xxxx xxxx", "001xx", 160, 0.10,
          ["Trastevere", "Monti", "the Spanish Steps", "Termini", "Prati"],
          ["Via del Corso", "Via Nazionale", "Via Veneto", "Via Cavour", "Via Condotti"]),
    _city("Barcelona", "Catalonia", "Spain", 41.3874, 2.1686, "+34 93 xxx xx xx", "080xx", 150, 0.10,
          ["the Gothic Quarter", "Eixample", "El Born", "Barceloneta", "Gracia"],
          ["La Rambla", "Passeig de Gracia", "Carrer de Balmes", "Via Laietana", "Avinguda Diagonal"]),
    _city("Hong Kong", "Hong Kong", "China", 22.2800, 114.1588, "+852 xxxx xxxx", "000000", 170, 0.0,
          ["Central", "Tsim Sha Tsui", "Causeway Bay", "Wan Chai", "Mong Kok"],
          ["Queen's Rd Central", "Nathan Rd", "Hennessy Rd", "Des Voeux Rd", "Canton Rd"]),
    _city("Los Angeles", "CA", "United States", 34.0522, -118.2437, "(213) xxx-xxxx", "900xx", 190, 0.155,
          ["Downtown", "Hollywood", "Santa Monica", "Beverly Hills", "Koreatown"],
          ["Wilshire Blvd", "Sunset Blvd", "S Figueroa St", "Hollywood Blvd", "W 7th St"]),
    _city("Amsterdam", "North Holland", "Netherlands", 52.3676, 4.9041, "+31 20 xxx xxxx", "10xx AA", 190, 0.125,
          ["the Canal Ring", "De Pijp", "the Jordaan", "Museumplein", "Oost"],
          ["Damrak", "Prinsengracht", "Herengracht", "Rokin", "Leidsestraat"]),
    _city("Sydney", "NSW", "Australia", -33.8688, 151.2093, "+61 2 xxxx xxxx", "20xx", 180, 0.10,
          ["Circular Quay", "Darling Harbour", "The Rocks", "Surry Hills", "Bondi"],
          ["George St", "Pitt St", "Elizabeth St", "Macquarie St", "Kent St"]),
    _city("Seoul", "Seoul", "South Korea", 37.5665, 126.9780, "+82 2-xxxx-xxxx", "0xxxx", 120, 0.10,
          ["Myeongdong", "Gangnam", "Itaewon", "Hongdae", "Jongno"],
          ["Sejong-daero", "Teheran-ro", "Eulji-ro", "Toegye-ro", "Itaewon-ro"]),
    _city("San Francisco", "CA", "United States", 37.7749, -122.4194, "(415) xxx-xxxx", "941xx", 230, 0.16,
          ["Union Square", "SoMa", "Fisherman's Wharf", "Nob Hill", "the Embarcadero"],
          ["Geary St", "Market St", "Mission St", "Powell St", "3rd St", "Post St"]),
    _city("Berlin", "Berlin", "Germany", 52.5200, 13.4050, "+49 30 xxxxxxxx", "10xxx", 120, 0.12,
          ["Mitte", "Kreuzberg", "Prenzlauer Berg", "Charlottenburg", "Friedrichshain"],
          ["Unter den Linden", "Friedrichstrasse", "Kurfuerstendamm", "Alexanderplatz", "Torstrasse"]),
    _city("Mexico City", "CDMX", "Mexico", 19.4326, -99.1332, "+52 55 xxxx xxxx", "06xxx", 80, 0.19,
          ["the Centro Historico", "Roma", "Condesa", "Polanco", "Zona Rosa"],
          ["Paseo de la Reforma", "Av Juarez", "Calle Madero", "Av Insurgentes", "Av Presidente Masaryk"]),
    _city("Cape Town", "Western Cape", "South Africa", -33.9249, 18.4241, "+27 21 xxx xxxx", "80xx", 90, 0.15,
          ["the V&A Waterfront", "the City Bowl", "Green Point", "Sea Point", "Camps Bay"],
          ["Long St", "Bree St", "Strand St", "Beach Rd", "Victoria Rd"]),
    _city("Rio de Janeiro", "RJ", "Brazil", -22.9068, -43.1729, "+55 21 xxxx-xxxx", "2xxxx-xxx", 90, 0.05,
          ["Copacabana", "Ipanema", "Leblon", "Botafogo", "Centro"],
          ["Av Atlantica", "Rua Visconde de Piraja", "Av Rio Branco", "Rua Barata Ribeiro", "Av Vieira Souto"]),
)

NAME_PREFIXES = (
    "Grand", "Royal", "Park", "City", "Central", "Plaza", "Metropole", "Palace", "Garden",
    "Harbour", "Riverside", "Crown", "Imperial", "Regent", "Majestic", "Continental", "Astor",
    "Belmont", "Carlton", "Savoy", "Windsor", "Ambassador", "Excelsior", "Atlantic", "Vista",
)
NAME_SUFFIXES = (
    "Hotel", "Hotel", "Hotel", "Inn", "Suites", "Residences", "Boutique Hotel", "Lodge",
    "House", "Hostel", "Resort", "Apartments",
)
STAR_ADJECTIVES = (
    ("budget", "no-frills", "simple"),
    ("casual", "friendly", "affordable"),
    ("modern", "comfortable", "stylish"),
    ("upscale", "elegant", "contemporary"),
    ("luxury", "landmark", "five-star"),
)
KINDS = ("hotel", "hotel", "boutique hotel", "inn", "guesthouse", "hotel")
AMENITIES = (
    "a rooftop bar", "free Wi-Fi", "a 24-hour fitness centre", "an indoor pool", "a spa",
    "a buffet breakfast", "a business centre", "family rooms", "airport shuttles",
    "a restaurant serving local dishes", "bike rental", "a garden terrace", "meeting rooms",
    "soundproofed rooms", "a cocktail lounge", "valet parking",
)
TRANSIT = ("a metro station", "the main railway station", "a bus terminal", "a tram stop", "the ferry pier")
FIRST_NAMES = (
    "james", "mary", "john", "patricia", "robert", "jennifer", "michael", "linda", "william",
    "elizabeth", "david", "barbara", "richard", "susan", "joseph", "jessica", "thomas", "sarah",
    "charles", "karen", "wei", "fang", "hiroshi", "yuki", "minjun", "seoyeon", "arjun", "priya",
    "mohammed", "fatima", "lucas", "sofia", "mateo", "valentina", "noah", "emma", "liam", "olivia",
    "ahmet", "elif", "giulia", "marco", "chloe", "louis", "anna", "felix", "thabo", "naledi",
)
LAST_NAMES = (
    "smith", "johnson", "williams", "brown", "jones", "garcia", "miller", "davis", "rodriguez",
    "martinez", "hernandez", "lopez", "gonzalez", "wilson", "anderson", "thomas", "taylor",
    "moore", "jackson", "martin", "lee", "wang", "li", "zhang", "chen", "tanaka", "suzuki",
    "kim", "park", "sharma", "patel", "khan", "silva", "santos", "rossi", "bianchi", "dubois",
    "moreau", "muller", "schmidt", "yilmaz", "kaya", "dlamini", "nkosi", "novak", "jansen",
)
PASSWORD_ALPHABET = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
# RACK is the full public rate, always present and valid all year.
RATE_PLAN_CODES = (
    ("RACK", 1.00), ("BAR", 0.95), ("ADV14", 0.85), ("AAA", 0.90), ("GOV", 0.80),
    ("CORP", 0.88), ("PROMO", 0.75), ("SENIOR", 0.90),
)
ROOM_TYPES = (
    ("KNG", "King sized bed", 1.10), ("QN", "Queen sized bed", 1.00), ("DBL", "Two double beds", 1.05),
    ("TWN", "Two twin beds", 0.95), ("STD", "Standard room", 0.90), ("STE", "Suite", 1.90),
)
STAR_WEIGHTS = (8, 20, 35, 25, 12)
STAR_PRICE = (0.45, 0.65, 1.0, 1.6, 2.8)
STAR_ROOMS = ((10, 60), (30, 150), (60, 300), (100, 450), (150, 800))
PLAN_WEIGHTS = (20, 30, 25, 15, 10)
NAME_STYLES = (40, 35, 25)
MIN_PLAN_DAYS, MAX_PLAN_DAYS = 14, 180
USERNAME_DIGITS = 0.7


# -------------------------------------------------------------- generation


def generate() -> Dataset:
    """The task's dataset (about 1 s); every process of a sample builds the same one."""
    counts = zipf_counts(HOTEL_COUNT, len(CITY_TABLE))
    spreads = [1.5 + 2.5 * math.sqrt(count / counts[0]) for count in counts]
    km_per_lon = [KM_PER_DEGREE_LAT * cos_degrees(t.lat) for t in CITY_TABLE]
    assignment = [city for city, count in enumerate(counts) for _ in range(count)]
    shuffle(Rand(SEED, "assignment", 0), assignment)
    members: list[list[int]] = [[] for _ in CITY_TABLE]
    for index, city in enumerate(assignment):
        members[city].append(index)
    taken: set[tuple[float, float]] = set()
    drafts = [
        _hotel(Rand(SEED, "hotel", index), index, city, spreads[city], km_per_lon[city], taken)
        for index, city in enumerate(assignment)
    ]
    popularity = [0] * HOTEL_COUNT
    cities = []
    for city, template in enumerate(CITY_TABLE):
        order = members[city]
        shuffle(Rand(SEED, "popularity", city), order)
        for rank, index in enumerate(order):
            popularity[index] = rank + 1
        cities.append(City(template.name, template.region, template.country, template.lat,
                           template.lon, template.phone, template.districts, spreads[city], km_per_lon[city], tuple(order)))
    hotels = tuple(replace(draft, popularity=popularity[n]) for n, draft in enumerate(drafts))
    return Dataset(SEED, tuple(cities), hotels, generate_users(Rand(SEED, "users", 0)))


def _hotel(r: Rand, index: int, city: int, spread_km: float, km_per_lon: float,
           taken: set[tuple[float, float]]) -> Hotel:
    template = CITY_TABLE[city]
    stars = 1 + r.weighted(STAR_WEIGHTS)
    spread = spread_km
    if r.random() < OUTSKIRTS_SHARE:
        spread *= OUTSKIRTS_SPREAD
    while True:
        dy = spread * r.normal()
        dx = spread * r.normal()
        lat = go_round(template.lat + dy / KM_PER_DEGREE_LAT, 1e6)
        lon = go_round(template.lon + dx / km_per_lon, 1e6)
        if (lat, lon) not in taken:
            taken.add((lat, lon))
            break
    return describe(r, str(index + 1), city, template, stars, lat, lon)


def describe(r: Rand, hotel_id: str, city: int, template: CityTemplate, stars: int,
             lat: float, lon: float) -> Hotel:
    """Everything of a hotel after its location, drawn from r in the generator's order."""
    district = template.districts[r.below(len(template.districts))]
    bare = district.removeprefix("the ")
    prefix = NAME_PREFIXES[r.below(len(NAME_PREFIXES))]
    suffix = NAME_SUFFIXES[r.below(len(NAME_SUFFIXES))]
    style = r.weighted(NAME_STYLES)
    if style == 0:
        name = f"{prefix} {suffix}"
    elif style == 1:
        name = f"{prefix} {bare} {suffix}"
    else:
        name = f"Hotel {prefix} {template.name}"
    phone = fill(r, template.phone)
    address = Address(str(1 + r.below(400)), template.streets[r.below(len(template.streets))],
                      template.name, template.region, template.country, fill(r, template.postal))
    adjectives = STAR_ADJECTIVES[stars - 1]
    first = r.below(len(AMENITIES))
    second = (first + 1 + r.below(len(AMENITIES) - 1)) % len(AMENITIES)
    other = template.districts[r.below(len(template.districts))]
    walk = 2 + r.below(14)
    transit = TRANSIT[r.below(len(TRANSIT))]
    adjective = adjectives[r.below(len(adjectives))]
    kind = KINDS[r.below(len(KINDS))]
    minutes = 5 + r.below(26)
    description = (f"A {walk}-minute walk from {transit}, this {adjective} {kind} in {district} is "
                   f"{minutes} minutes from {other} and offers {AMENITIES[first]} and {AMENITIES[second]}.")
    # 1 + 4 Beta(5, 2): the fifth smallest of six uniforms is Beta(5, 2).
    uniforms = sorted(r.random() for _ in range(6))
    rating = go_round(1 + 4 * uniforms[4], 100)
    low, high = STAR_ROOMS[stars - 1]
    a = r.random()
    capacity = low + int(float(high - low) * (a + r.random()) / 2)
    a = r.random()
    base = template.base_price * STAR_PRICE[stars - 1] * (0.75 + 0.25 * (a + r.random()))
    count = 1 + r.weighted(PLAN_WEIGHTS)
    codes = list(range(1, len(RATE_PLAN_CODES)))
    price = sys.float_info.max
    plans = []
    for n in range(count):
        code, start, days = 0, 0, RATE_WINDOW_DAYS
        if n > 0:
            pick = n - 1 + r.below(len(codes) - (n - 1))
            codes[n - 1], codes[pick] = codes[pick], codes[n - 1]
            code = codes[n - 1]
            days = MIN_PLAN_DAYS + r.below(MAX_PLAN_DAYS - MIN_PLAN_DAYS + 1)
            start = r.below(RATE_WINDOW_DAYS - days + 1)
        room_code, room_description, room_factor = ROOM_TYPES[r.below(len(ROOM_TYPES))]
        total = go_round(base * RATE_PLAN_CODES[code][1] * room_factor, 100)
        plans.append(RatePlan(
            RATE_PLAN_CODES[code][0], str(RATE_WINDOW_START + timedelta(start)),
            str(RATE_WINDOW_START + timedelta(start + days)), room_code, room_description,
            total, total, go_round(total * (1 + template.tax), 100)))
        price = min(price, total)
    return Hotel(hotel_id, city, name, phone, description, address, lat, lon, stars, rating,
                 price, capacity, 0, tuple(plans))


def generate_users(r: Rand) -> tuple[User, ...]:
    users: list[User] = []
    seen: set[str] = set()
    while len(users) < USER_COUNT:
        name = FIRST_NAMES[r.below(len(FIRST_NAMES))]
        name += "." + LAST_NAMES[r.below(len(LAST_NAMES))]
        if r.random() < USERNAME_DIGITS:
            name += str(r.below(1000))
        while name in seen:
            name += str(r.below(10))
        seen.add(name)
        length = 10 + r.below(7)
        password = "".join(PASSWORD_ALPHABET[r.below(len(PASSWORD_ALPHABET))] for _ in range(length))
        users.append(User(name, password))
    return tuple(users)


def zipf_counts(total: int, n: int) -> list[int]:
    """total split over n ranks in proportion to 1/rank, by largest remainder (lower rank first)."""
    weights = [1 / (k + 1) for k in range(n)]
    norm = 0.0
    for weight in weights:
        norm += weight
    counts, remainders = [], []
    for weight in weights:
        share = float(total) * (weight / norm)
        counts.append(int(share))
        remainders.append(share - counts[-1])
    for _ in range(total - sum(counts)):
        best = 0
        for index, remainder in enumerate(remainders):
            if remainder > remainders[best]:
                best = index
        counts[best] += 1
        remainders[best] = -1.0
    return counts


def shuffle(r: Rand, items: list[int]) -> None:
    for i in range(len(items) - 1, 0, -1):
        j = r.below(i + 1)
        items[i], items[j] = items[j], items[i]


def fill(r: Rand, template: str) -> str:
    out = []
    for char in template:
        if char == "x":
            out.append(chr(48 + r.below(10)))
        elif char == "A":
            out.append(chr(65 + r.below(26)))
        else:
            out.append(char)
    return "".join(out)


def go_round(value: float, scale: float) -> float:
    """round(value * scale) / scale, rounding half away from zero (Go's math.Round)."""
    x = value * scale
    t = float(math.trunc(x))
    if abs(x - t) >= 0.5:
        t += math.copysign(1.0, x)
    return t / scale


def cos_degrees(degrees: float) -> float:
    """cos by its Taylor series, so the data does not depend on the platform's libm."""
    x = degrees * math.pi / 180
    x2 = x * x
    term = total = 1.0
    for n in range(1, 13):
        term *= -x2 / float((2 * n - 1) * (2 * n))
        total += term
    return total


# ---------------------------------------------------------- MongoDB documents


def password_hash(password: str) -> str:
    """The stored form of a password: hex SHA-256, as the user service computes it."""
    return hashlib.sha256(password.encode()).hexdigest()


def float32(value: float) -> float:
    return struct.unpack("<f", struct.pack("<f", value))[0]


@dataclass(frozen=True)
class Collection:
    database: str
    name: str
    documents: list[dict[str, object]]
    indexes: tuple[tuple[str, tuple[str, ...], bool], ...]  # (name, keys, unique)


def documents(data: Dataset) -> list[Collection]:
    """Every collection the seed writes, with the services' own field names and types.

    Profile coordinates are float32 values (the profile service's schema) and
    capacities int32 (`bson` encodes a Python int below 2**31 as int32). The
    indexes match the services' query shapes. reservation-db.reservation is
    created empty, with its index.
    """
    geo, profiles, plans, recommendations, numbers = [], [], [], [], []
    for h in data.hotels:
        geo.append({"hotelId": h.id, "lat": h.lat, "lon": h.lon})
        a = h.address
        profiles.append({"id": h.id, "name": h.name, "phoneNumber": h.phone, "description": h.description,
                         "address": {"streetNumber": a.street_number, "streetName": a.street_name,
                                     "city": a.city, "state": a.state, "country": a.country,
                                     "postalCode": a.postal_code, "lat": float32(h.lat), "lon": float32(h.lon)}})
        for p in h.rate_plans:
            plans.append({"hotelId": h.id, "code": p.code, "inDate": p.in_date, "outDate": p.out_date,
                          "roomType": {"bookableRate": p.bookable_rate, "code": p.room_code,
                                       "roomDescription": p.room_description, "totalRate": p.total_rate,
                                       "totalRateInclusive": p.total_rate_inclusive}})
        recommendations.append({"hotelId": h.id, "lat": h.lat, "lon": h.lon, "rate": h.rating,
                                "price": h.price})
        numbers.append({"hotelId": h.id, "numberOfRoom": h.capacity})
    users = [{"username": u.username, "password": password_hash(u.password)} for u in data.users]
    by_hotel = (("hotelId_1", ("hotelId",), True),)
    return [
        Collection("geo-db", "geo", geo, by_hotel),
        Collection("profile-db", "hotels", profiles, (("id_1", ("id",), True),)),
        Collection("rate-db", "inventory", plans, (("hotelId_1", ("hotelId",), False),)),
        Collection("recommendation-db", "recommendation", recommendations, by_hotel),
        Collection("reservation-db", "number", numbers, by_hotel),
        Collection("reservation-db", "reservation", [],
                   (("hotelId_1_inDate_1_outDate_1", ("hotelId", "inDate", "outDate"), False),)),
        Collection("user-db", "user", users, (("username_1", ("username",), True),)),
    ]


def documents_digest(collections: list[Collection]) -> str:
    """SHA-256 over each collection's name, count and documents' BSON: what a seed leaves (no _id)."""
    import bson  # pymongo's, in the task image

    digest = hashlib.sha256()
    for collection in collections:
        digest.update(f"{collection.database}.{collection.name}\0{len(collection.documents)}\0".encode())
        for document in collection.documents:
            digest.update(bson.encode(document))
    return digest.hexdigest()


def encode(data: Dataset) -> bytes:
    """The dataset file: the dataclasses as JSON, field by field (what vibesys-evaluator writes too)."""
    return json.dumps(asdict(data), allow_nan=False, separators=(",", ":")).encode() + b"\n"


if __name__ == "__main__":
    # python dataset.py PATH: write the dataset file (the accuracy check's oracle reads it).
    Path(sys.argv[1]).write_bytes(encode(generate()))
