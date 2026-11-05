"""dataset.py: the seeded documents, the geo reference and float32 parsing.

    python -m unittest discover -s tests      (needs vibesys-evaluator and pymongo's bson)
"""

import json
import struct
import sys
import unittest
from fractions import Fraction
from pathlib import Path

TASK_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(TASK_DIR))

import dataset as ds  # noqa: E402

# The documents digest of the task's earlier Go generator and seed for the same
# SEED (evaluator cmd/seed --summary-only at the commit before the port): the
# Python generator writes exactly the same MongoDB documents.
GO_SEED_DOCUMENTS_DIGEST = "7670033f806863ce76f47d2ba5922e04f5d9496ab5f339190affe5c634bb5cc6"
GOLDEN = TASK_DIR / "evaluator" / "internal" / "appsupport" / "hotel" / "testdata" / "geoindex_golden.json"

DATA = ds.generate()


class DatasetTest(unittest.TestCase):
    def test_documents_equal_the_go_seeds(self):
        self.assertEqual(ds.documents_digest(ds.documents(DATA)), GO_SEED_DOCUMENTS_DIGEST)

    def test_shape(self):
        self.assertEqual(len(DATA.hotels), ds.HOTEL_COUNT)
        self.assertEqual(len({u.username for u in DATA.users}), ds.USER_COUNT)
        self.assertEqual(sorted(i for c in DATA.cities for i in c.hotels), list(range(ds.HOTEL_COUNT)))
        self.assertEqual(len({(h.lat, h.lon) for h in DATA.hotels}), ds.HOTEL_COUNT)
        for hotel in DATA.hotels[:500]:
            self.assertEqual(hotel.rate_plans[0].code, "RACK")
            self.assertEqual(hotel.price, min(p.total_rate for p in hotel.rate_plans))
            self.assertEqual(DATA.cities[hotel.city].hotels[hotel.popularity - 1], int(hotel.id) - 1)

    def test_pure(self):
        self.assertEqual(ds.generate().users[:50], DATA.users[:50])


class ReferenceTest(unittest.TestCase):
    """Against answers of the real go-geoindex library over this dataset."""

    @classmethod
    def setUpClass(cls):
        cls.golden = json.loads(GOLDEN.read_text())
        cls.reference = ds.Reference(DATA)

    def test_search_matches_go_geoindex(self):
        for answer in self.golden["nearby"]:
            got, exact = self.reference.search_candidates(answer["lat"], answer["lon"])
            self.assertTrue(exact, answer)
            self.assertEqual(sorted(got), answer["ids"], answer)

    def test_distance_recommendation_matches_go_geoindex(self):
        for answer in self.golden["nearest"]:
            got, _ = self.reference.nearest(float(answer["lat"]), float(answer["lon"]))
            self.assertEqual(sorted(got), sorted(answer["ids"]), answer)

    def test_additions_join_the_index(self):
        lat, lon = ds.sandbox_point(7, ds.Rand(1))
        hotel = ds.added_hotel("a1-1", ds.Rand(2), lat, lon)
        text = (f"{lat:.5f}", f"{lon:.5f}")
        self.assertEqual(self.reference.search_candidates(*text), ([], True))
        self.assertEqual(self.reference.search_candidates(*text, (hotel,))[0], ["a1-1"])


class Float32Test(unittest.TestCase):
    def test_parse_rounds_the_decimal_once(self):
        for text in ("51.50740", "-0.12780", "0.1", "16777217", "1.00000005960464477539062500001",
                     "1.000000059604644775390625", "-33.86880", "139.76710"):
            exact = Fraction(text)
            got = ds.parse_search_coordinate(text)
            below = ds._f32_neighbour(got, False)
            above = ds._f32_neighbour(got, True)
            self.assertLessEqual(abs(Fraction(got) - exact), abs(Fraction(below) - exact), text)
            self.assertLessEqual(abs(Fraction(got) - exact), abs(Fraction(above) - exact), text)
            self.assertEqual(got, struct.unpack("<f", struct.pack("<f", got))[0])

    def test_tie_goes_to_even(self):
        # 1 + 2**-24 lies half-way between 1 and the next float32; even is 1.
        self.assertEqual(ds.parse_search_coordinate("1.000000059604644775390625"), 1.0)

    def test_encoded_coordinate_is_the_shortest_float32_text(self):
        self.assertEqual(ds.encoded_coordinate(51.5074), 51.5074)
        self.assertEqual(ds.encoded_coordinate(0.1), 0.1)


if __name__ == "__main__":
    unittest.main()
