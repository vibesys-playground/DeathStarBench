"""dataset.py: the seeded documents and the dataset's shape.

    python -m unittest discover -s tests      (needs vibesys-evaluator and pymongo's bson)
"""

import sys
import unittest
from pathlib import Path

TASK_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(TASK_DIR))

import dataset as ds  # noqa: E402

# The documents digest of the task's earlier Go generator and seed for the same
# SEED (evaluator cmd/seed --summary-only at the commit before the port): the
# Python generator writes exactly the same MongoDB documents.
GO_SEED_DOCUMENTS_DIGEST = "7670033f806863ce76f47d2ba5922e04f5d9496ab5f339190affe5c634bb5cc6"

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


if __name__ == "__main__":
    unittest.main()
