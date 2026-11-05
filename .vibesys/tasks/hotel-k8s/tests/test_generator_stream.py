"""The Go generator (evaluator/cmd/generator) writes bench.py's Python workload's request stream.

    HOTEL_GENERATOR=/path/to/hotel-generator python -m pytest tests/test_generator_stream.py

For the same phase (a first phase, and a later one whose history is the
first one's writes), both generators' `generate` must write the same
messages (each side writes a message's fields in its own order, so they are
compared decoded) and keep the same operations with the same infos (compared
as JSON values). With HOTEL_STREAM_GOLDEN=DIR it then records the phases and
digests of the Go stream there, for the Go test that pins the stream once
the Python workload is gone (cmd/generator/stream_test.go).
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

TASK_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(TASK_DIR))

import dataset as ds  # noqa: E402

# test-isolation: the dataset file the phase pod writes, and the schema's messages.
from vibesys_evaluator._private import dataset as dataset_file  # noqa: E402
from vibesys_evaluator._proto.vibesys.evaluator.v1 import loadgen_pb as pb  # noqa: E402
from vibesys_evaluator._proto.vibesys.evaluator.v1 import phase_pb  # noqa: E402

GENERATOR = os.environ.get("HOTEL_GENERATOR", "")
GOLDEN = os.environ.get("HOTEL_STREAM_GOLDEN", "")
SAMPLE_SEED = 0x5EED_0F_7E57
FIRST_OPS = 30_000  # of the first phase: its writes are the later phase's history
LATER_OPS = 60_000
T0 = 1_800_000_000_000_000_000


def _varint(read) -> int | None:  # noqa: ANN001
    value, shift = 0, 0
    while True:
        byte = read(1)
        if not byte:
            return None
        value |= (byte[0] & 0x7F) << shift
        if byte[0] < 0x80:
            return value
        shift += 7


def _frames(read, limit: int | None = None) -> list[bytes]:  # noqa: ANN001
    frames = []
    while limit is None or len(frames) < limit:
        length = _varint(read)
        if length is None:
            break
        frame = read(length)
        if len(frame) < length:
            break
        frames.append(frame)
    return frames


def _generate(command: list[str], phase: Path, dataset: Path, state: Path, ops: int) -> tuple[list[bytes], list[bytes]]:
    """The first `ops` operations (after the FileHeader and RequestsHeader) and the kept records."""
    state.mkdir()
    argv = [*command, "generate", "--phase-input", str(phase), "--dataset", str(dataset), "--state", str(state)]
    with subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as child:  # noqa: S603
        assert child.stdout is not None
        frames = _frames(child.stdout.read, ops + 2)
        child.stdout.close()
        _, stderr = child.communicate(timeout=300)
    assert child.returncode == 0, stderr.decode(errors="replace")
    assert len(frames) == ops + 2, f"{argv[0]} wrote {len(frames)} frames"
    with (state / "kept.binpb").open("rb") as kept:
        records = _frames(kept.read)
    return frames[1:], records


def _kept(records: list[bytes], ops: int) -> list[phase_pb.KeptOperation]:
    out = [phase_pb.KeptOperation.from_binary(r) for r in records]
    return [k for k in out if k.index < ops]


def _phase(path: Path, index: int, history: list[phase_pb.HistoryEntry]) -> Path:
    phase = phase_pb.PhaseInput(sample_seed=SAMPLE_SEED, phase_index=index, model=pb.LoadModel.CLOSED,
                                clients=32, secs=60.0, warmup_secs=15.0, history=history)
    path.write_bytes(phase.to_binary())
    return path


def _history(names: list[str], kept: list[phase_pb.KeptOperation]) -> list[phase_pb.HistoryEntry]:
    """The writes of a phase as a later one's history: every seventh failed, the others acknowledged."""
    writes = [k for k in kept if names[k.operation] in WRITES]
    return [
        phase_pb.HistoryEntry(operation=names[k.operation], info_json=k.info_json,
                              start_unix_nanos=T0 + 1_000_000 * k.index,
                              end_unix_nanos=T0 + 1_000_000 * k.index + 3_000_000, ok=n % 7 != 3)
        for n, k in enumerate(writes)
    ]


WRITES = {"reserve_capacity", "update_rate", "update_profile", "set_capacity", "add_hotel"}


def _digest(frames: list[bytes]) -> str:
    digest = hashlib.sha256()
    for frame in frames:
        digest.update(len(frame).to_bytes(4, "little") + frame)
    return digest.hexdigest()


@unittest.skipUnless(GENERATOR, "HOTEL_GENERATOR names no Go generator binary")
class StreamTest(unittest.TestCase):
    def test_the_go_generator_writes_the_python_workloads_stream(self):
        import tempfile

        with tempfile.TemporaryDirectory() as scratch:
            tmp = Path(scratch)
            dataset = tmp / "dataset.json"
            dataset.write_bytes(dataset_file.encode(ds.generate()))
            python = [sys.executable, str(TASK_DIR / "bench.py")]
            golden = []
            history: list[phase_pb.HistoryEntry] = []
            for index, ops in ((0, FIRST_OPS), (2, LATER_OPS)):
                phase = _phase(tmp / f"phase{index}.binpb", index, history)
                py_frames, py_records = _generate(python, phase, dataset, tmp / f"py{index}", ops)
                go_frames, go_records = _generate([GENERATOR], phase, dataset, tmp / f"go{index}", ops)
                header = pb.RequestsHeader.from_binary(py_frames[0])
                self.assertEqual(header, pb.RequestsHeader.from_binary(go_frames[0]))
                for n in range(1, ops + 1):
                    self.assertEqual(pb.PlannedOperation.from_binary(py_frames[n]),
                                     pb.PlannedOperation.from_binary(go_frames[n]), f"phase {index}, operation {n - 1}")
                py_kept, go_kept = _kept(py_records, ops), _kept(go_records, ops)
                self.assertEqual([(k.index, k.operation, k.sampled) for k in py_kept],
                                 [(k.index, k.operation, k.sampled) for k in go_kept])
                for p, g in zip(py_kept, go_kept, strict=True):
                    self.assertEqual(json.loads(p.info_json), json.loads(g.info_json), f"kept {p.index}")
                names = list(header.operation_names)
                counts = {name: 0 for name in names}
                for frame in py_frames[1:]:
                    counts[names[pb.PlannedOperation.from_binary(frame).operation]] += 1
                print(f"phase {index}: {ops} operations equal, {len(py_kept)} kept, {len(history)} in its history; "
                      f"mix {counts}", file=sys.stderr)
                golden.append({"phase_input": phase.name, "operations": ops,
                               "stream_sha256": _digest(go_frames),
                               "kept_sha256": _digest(go_records[:len(go_kept)])})
                history = _history(names, py_kept)
                self.assertTrue(any(h.operation == "add_hotel" and h.ok for h in history))
            if GOLDEN:
                out = Path(GOLDEN)
                out.mkdir(parents=True, exist_ok=True)
                for entry in golden:
                    (out / entry["phase_input"]).write_bytes((tmp / entry["phase_input"]).read_bytes())
                (out / "stream.json").write_text(json.dumps({
                    "about": "the Python workload's stream (bench.py before the Go port) for these phases: "
                             "SHA-256 over the Go generator's frames (4-byte little-endian length, then the "
                             "frame) after the FileHeader: the RequestsHeader and the first operations; and "
                             "over its kept records of those operations",
                    "dataset_documents_sha256": ds.documents_digest(ds.documents(ds.generate())),
                    "phases": golden}, indent=2) + "\n")


if __name__ == "__main__":
    unittest.main()
