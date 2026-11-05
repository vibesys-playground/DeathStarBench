"""bench.py's operations and checks, on synthetic calls (no cluster):  python -m unittest discover -s tests"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import bench  # noqa: E402
from vibesys_evaluator import Call, Rand, Response  # noqa: E402
from vibesys_evaluator.service import Request  # noqa: E402

from test_dataset import DATA  # noqa: E402

OPS = {op.name: op for op in bench.WORKLOAD.operations}
_REQUEST = bench.Request
_INFOS: dict[int, dict] = {}


def _recording_request(*steps, info=None, exclusive=None):
    """bench's Request, remembering each request's info (as JSON) for the tests."""
    request = _REQUEST(*steps, info=info, exclusive=exclusive)
    _INFOS[id(request)] = json.loads(json.dumps(info or {}))
    return request


bench.Request = _recording_request


def pending(info):
    return Call(info=info, start=None, end=None, ok=None, responses=())


def done(info, *bodies, start=100.0, end=100.1):
    return Call(info=json.loads(json.dumps(info)), start=start, end=end, ok=True,
                responses=tuple(Response(status=200, body=json.dumps(b).encode()) for b in bodies))


def geojson(m, ids, profiles=None):
    features = []
    for hotel_id in ids:
        hotel = m.data.by_id[hotel_id]
        name, phone = (profiles or {}).get(hotel_id, (hotel.name, hotel.phone))
        features.append({"type": "Feature", "id": hotel_id, "properties": {"name": name, "phone_number": phone},
                         "geometry": {"type": "Point", "coordinates": list(m.ref.encoded(hotel))}})
    return {"type": "FeatureCollection", "features": features}


class BenchTest(unittest.TestCase):
    def setUp(self):
        self.m = bench.Model(DATA, Rand(7, "model"))

    def build(self, name, n):
        op = OPS[name]
        request = op.build(self.m, Rand(7, "phase", 0).child(n))
        self.assertIsInstance(request, Request)
        return request

    def test_mix_is_the_old_workloads(self):
        self.assertEqual({op.name: op.weight for op in bench.WORKLOAD.operations}, {
            "search_hotels": 6000, "recommend_distance": 1300, "recommend_rate": 1300, "recommend_price": 1300,
            "login_valid": 30, "login_invalid": 20, "reserve_capacity": 50, "hotel_rates": 400,
            "update_rate": 42, "update_profile": 42, "set_capacity": 12, "add_hotel": 8})

    def test_every_operation_builds(self):
        for name, op in OPS.items():
            for n in range(30):
                request = self.build(name, n)
                if op.apply is not None:
                    op.apply(self.m, pending(request_info(request)))

    def test_reservations_fill_distinct_hotels_per_night(self):
        op = OPS["reserve_capacity"]
        seen = set()
        for n in range(3 * bench.RESERVATIONS_PER_NIGHT):
            slot = self.m.next_slot
            request = self.build("reserve_capacity", n)
            info = request_info(request)
            self.assertEqual(info["slot"], slot)
            self.assertFalse(bench.capacity_managed(info["hotel"]))
            key = (slot // bench.RESERVATIONS_PER_NIGHT, info["hotel"])
            self.assertNotIn(key, seen)
            seen.add(key)
            op.apply(self.m, pending(info))

    def test_capacity_writes_go_to_managed_hotels(self):
        for n in range(50):
            info = request_info(self.build("set_capacity", n))
            hotel = DATA.by_id[info["hotel"]]
            self.assertTrue(bench.capacity_managed(int(hotel.id) - 1))
            self.assertTrue(max(1, hotel.capacity // 2) <= info["rooms"] <= 2 * hotel.capacity)

    def test_search_check_accepts_the_reference_and_rejects_others(self):
        request = self.build("search_hotels", 3)
        info = request_info(request)
        call = done(info, geojson(self.m, info["expected"]))
        self.assertIsNone(bench.check_search(self.m, call))
        wrong = done(info, geojson(self.m, info["expected"][:-1] + ["1"]))
        self.assertIsNotNone(bench.check_search(self.m, wrong))

    def test_profile_writes_are_visible_within_the_bound(self):
        hotel = DATA.hotels[0]
        write = done({"hotel": hotel.id, "name": hotel.name, "phone": "new"}, {"message": "Profile updated."},
                     start=10.0, end=10.2)
        bench.apply_profile(self.m, write)
        ids, _ = self.m.ref.search_candidates(f"{hotel.lat:.5f}", f"{hotel.lon:.5f}")
        self.assertIn(hotel.id, ids)
        info = {"expected": ids}
        old, new = (hotel.name, hotel.phone), (hotel.name, "new")
        during = dict(start=10.5, end=10.6)
        later = dict(start=11.5, end=11.6)
        self.assertIsNone(bench.check_search(self.m, done(info, geojson(self.m, ids, {hotel.id: old}), **during)))
        self.assertIsNone(bench.check_search(self.m, done(info, geojson(self.m, ids, {hotel.id: new}), **later)))
        self.assertIsNotNone(bench.check_search(self.m, done(info, geojson(self.m, ids, {hotel.id: old}), **later)))

    def test_rates_check(self):
        hotel = DATA.hotels[5]
        body = {"hotelId": hotel.id, "ratePlans": [
            {"code": p.code, "inDate": p.in_date, "outDate": p.out_date,
             "roomType": {"bookableRate": p.bookable_rate, "code": p.room_code, "roomDescription": p.room_description,
                          "totalRate": p.total_rate, "totalRateInclusive": p.total_rate_inclusive}}
            for p in reversed(hotel.rate_plans)]}
        self.assertIsNone(bench.check_rates(self.m, done({"hotel": hotel.id}, body)))
        body["ratePlans"][0]["roomType"]["totalRate"] += 1
        self.assertIsNotNone(bench.check_rates(self.m, done({"hotel": hotel.id}, body)))

    def test_messages_are_exact(self):
        check = OPS["login_valid"].check
        self.assertIsNone(check(self.m, done({}, {"message": "Login successfully!"})))
        self.assertIsNotNone(check(self.m, done({}, {"message": "Login successfully!", "x": 1})))
        self.assertIsNotNone(check(self.m, done({}, {"message": "Login successfully"})))


def request_info(request):
    """What a Request was built from, as the check sees it (after a JSON round trip)."""
    return _INFOS[id(request)]


if __name__ == "__main__":
    unittest.main()
