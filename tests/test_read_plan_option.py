import copy
import sys
import subprocess
import os
from io import BytesIO

import pytest

import fastavro
from fastavro.read import MessageReader

from .test_read_behaviour import (
    READER,
    WRITER,
    WRITER_RECORD,
    outcome,
    roundtrip_file,
    schemaless_bytes,
)

# A probe that tells the two readers apart: a negative enum index, which read
# plans reject (IndexError) and the previous reader wraps around to the last
# symbol. read_plan_enabled() is False on the pure-Python implementation and
# with FASTAVRO_READ_PLAN=0, where every call takes the previous reader.
SCHEMA = fastavro.parse_schema(
    {
        "type": "record",
        "name": "R",
        "fields": [
            {
                "name": "e",
                "type": {"type": "enum", "name": "E", "symbols": ["A", "B", "C"]},
            }
        ],
    }
)
MESSAGE = b"\x01"  # enum index -1
PREVIOUS_READER = ("ok", {"e": "C"})


def by_default():
    return ("err", IndexError) if fastavro.read.read_plan_enabled() else PREVIOUS_READER


def corrupt_file():
    fo = BytesIO()
    fastavro.writer(fo, SCHEMA, [{"e": "C"}])
    data = bytearray(fo.getvalue())
    assert data[-17] == 0x04  # the one datum, just before the 16-byte sync marker
    data[-17] = 0x01
    return bytes(data)


READS = {
    "MessageReader": lambda **kw: MessageReader(SCHEMA, **kw).read(MESSAGE),
    "reader": lambda **kw: next(iter(fastavro.reader(BytesIO(corrupt_file()), **kw))),
    "block_reader": lambda **kw: next(
        rec for b in fastavro.block_reader(BytesIO(corrupt_file()), **kw) for rec in b
    ),
}


@pytest.mark.parametrize("api", READS)
def test_read_plan_false_uses_the_previous_reader_for_that_call_only(api):
    read = READS[api]
    assert outcome(lambda: read(read_plan=False)) == PREVIOUS_READER
    assert outcome(read) == by_default()
    assert outcome(lambda: read(read_plan=True)) == by_default()


@pytest.mark.parametrize("api", READS)
def test_global_switch_still_turns_read_plans_off(api):
    previous = fastavro.read.set_read_plan_enabled(False)
    try:
        assert outcome(lambda: READS[api](read_plan=True)) == PREVIOUS_READER
    finally:
        fastavro.read.set_read_plan_enabled(previous)


def test_read_plan_false_gives_the_same_results_on_valid_data():
    expected = roundtrip_file(WRITER, [WRITER_RECORD], READER)
    previous = roundtrip_file(WRITER, [WRITER_RECORD], READER, read_plan=False)
    assert previous == expected and [list(r) for r in previous] == [
        list(r) for r in expected
    ]
    writer, reader = fastavro.parse_schema(WRITER), fastavro.parse_schema(READER)
    payload = schemaless_bytes(WRITER, WRITER_RECORD)
    for read_plan in (True, False):
        record = MessageReader(writer, reader, read_plan=read_plan).read(payload)
        assert record == expected[0] and list(record) == list(expected[0])


def test_message_reader_keeps_read_plan_when_copied():
    reader = copy.deepcopy(MessageReader(SCHEMA, read_plan=False))
    assert reader.read_plan is False
    assert outcome(lambda: reader.read(MESSAGE)) == PREVIOUS_READER


@pytest.mark.skipif(
    not hasattr(fastavro.read._read, "set_read_plan_enabled"),
    reason="compiled read plans are Cython-only",
)
@pytest.mark.parametrize(
    "setting,enabled,warns",
    [
        (None, True, False),
        ("", True, False),
        ("1", True, False),
        ("true", True, False),
        ("On", True, False),
        ("0", False, False),
        (" 0", False, False),
        ("false", False, False),
        ("False", False, False),
        ("no", False, False),
        ("OFF", False, False),
        ("maybe", True, True),
        ("0.0", True, True),
    ],
)
def test_environment_variable(setting, enabled, warns):
    # It is read when fastavro is imported: check in a new process.
    env = {k: v for k, v in os.environ.items() if k != "FASTAVRO_READ_PLAN"}
    if setting is not None:
        env["FASTAVRO_READ_PLAN"] = setting
    out = subprocess.run(
        [
            sys.executable,
            "-c",
            "import fastavro.read as r; print(r.read_plan_enabled())",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip() == str(enabled)
    assert ("FASTAVRO_READ_PLAN" in out.stderr) == warns
