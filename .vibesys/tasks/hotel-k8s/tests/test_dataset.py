"""dataset.py: the dataset file (the spec the Go generator generates the data from).

    python -m unittest discover -s tests      (needs vibesys-evaluator)
"""

import json
import re
import subprocess
import sys
import tempfile
import unittest
from dataclasses import asdict, fields
from pathlib import Path

TASK_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(TASK_DIR))

import dataset as ds  # noqa: E402

GO_DATASET = TASK_DIR / "evaluator" / "internal" / "dataset" / "dataset.go"


class DatasetTest(unittest.TestCase):
    def test_the_file_is_the_spec_field_by_field(self):
        """vibesys-evaluator writes a dataclass dataset as an object of its fields in declaration
        order (documented behaviour of its dataset file); dataset.py's encode is that, for the
        accuracy check, which does not go through the package."""
        self.assertIs(ds.generate(), ds.SPEC)
        self.assertEqual(json.loads(ds.encode(ds.SPEC)), asdict(ds.SPEC))
        self.assertEqual(list(json.loads(ds.encode(ds.SPEC))), [f.name for f in fields(ds.SPEC)])

    def test_the_spec_is_the_go_generators(self):
        """Field names and the format are the Go Spec's (whose ParseSpec rejects others)."""
        source = GO_DATASET.read_text()
        spec = re.search(r"type Spec struct \{(.*?)\n\}", source, re.S).group(1)
        self.assertEqual(re.findall(r'json:"([a-z_]+)"', spec), [f.name for f in fields(ds.Spec)])
        self.assertEqual(int(re.search(r"\bFormat\s*=\s*(\d+)", source).group(1)), ds.FORMAT)
        self.assertEqual(ds.SPEC.format, ds.FORMAT)

    def test_the_dataset_is_large(self):
        """The task's data is about ten times the candidate's memory (OBJECTIVE.md, Data)."""
        self.assertEqual(ds.SPEC.hotels, 1_500_000)
        self.assertEqual(ds.SPEC.users, 500_000)

    def test_pure_and_written_by_the_script(self):
        self.assertIs(ds.generate(), ds.generate())
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch) / "dataset.json"
            subprocess.run([sys.executable, str(TASK_DIR / "dataset.py"), str(path)], check=True)
            self.assertEqual(path.read_bytes(), ds.encode(ds.SPEC))
            self.assertEqual(json.loads(path.read_text()), {
                "format": ds.FORMAT, "seed": ds.SPEC.seed, "hotels": ds.SPEC.hotels, "users": ds.SPEC.users})


if __name__ == "__main__":
    unittest.main()
