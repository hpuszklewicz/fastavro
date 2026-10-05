import tracemalloc
from io import BytesIO

import pytest

import fastavro
from fastavro.read import LOGICAL_READERS, _read

from .test_read_behaviour import outcome, record_schema

pytestmark = [
    pytest.mark.skipif(
        not hasattr(_read, "compile_read_plan"),
        reason="compiled read plans are Cython-only",
    ),
    pytest.mark.skipif(
        not fastavro.read.read_plan_enabled(),
        reason="compiled read plans are disabled (FASTAVRO_READ_PLAN=0)",
    ),
]


def parsed(fields):
    return fastavro.parse_schema(record_schema("R", fields))


@pytest.mark.parametrize(
    "writer,reader,payload,expected",
    [
        # length -1 in a field the reader drops (must not move the cursor back)
        (
            [{"name": "skip", "type": "bytes"}, {"name": "x", "type": "int"}],
            [{"name": "x", "type": "int"}],
            b"\x01\x02",
            EOFError,
        ),
        # length -1 in a field that is read
        ([{"name": "s", "type": "string"}], None, b"\x01", EOFError),
        # 11 continuation bytes cannot encode a 64-bit value
        ([{"name": "n", "type": "long"}], None, b"\xff" * 11 + b"\x01", ValueError),
    ],
)
def test_corrupt_input(writer, reader, payload, expected):
    reader_schema = parsed(reader) if reader else None
    assert outcome(
        lambda: fastavro.schemaless_reader(
            BytesIO(payload), parsed(writer), reader_schema
        )
    ) == ("err", expected)


def test_stream_position_after_error_is_where_decoding_stopped():
    schema = parsed([{"name": "a", "type": "int"}, {"name": "s", "type": "string"}])
    fo = BytesIO(b"\x02\x0aab")  # a=1, then a 5-byte string of which 2 are present
    with pytest.raises(EOFError):
        fastavro.schemaless_reader(fo, schema)
    assert fo.tell() == 2


def test_replacing_a_logical_reader_is_seen_by_cached_plans():
    key = "int-fastpath-probe"
    schema = parsed(
        [{"name": "x", "type": {"type": "int", "logicalType": "fastpath-probe"}}]
    )
    try:
        LOGICAL_READERS[key] = lambda data, w, r: ("first", data)
        assert fastavro.schemaless_reader(BytesIO(b"\x02"), schema) == {
            "x": ("first", 1)
        }
        LOGICAL_READERS[key] = lambda data, w, r: ("second", data)
        assert fastavro.schemaless_reader(BytesIO(b"\x02"), schema) == {
            "x": ("second", 1)
        }
        del LOGICAL_READERS[key]
        assert fastavro.schemaless_reader(BytesIO(b"\x02"), schema) == {"x": 1}
    finally:
        LOGICAL_READERS.pop(key, None)


def test_cache_is_keyed_by_schema_identity_and_evicts_oldest_first():
    previous = _read.set_schemaless_plan_cache_size(4)
    try:
        fields = [{"name": "x", "type": "int"}]
        schemas = [parsed(fields) for _ in range(6)]
        for s in schemas:
            assert fastavro.schemaless_reader(BytesIO(b"\x02"), s) == {"x": 1}
        assert _read.schemaless_plan_cache_info() == {"size": 4, "capacity": 4}
        fastavro.schemaless_reader(
            BytesIO(b"\x02"), schemas[5], return_record_name=True
        )
        assert _read.schemaless_plan_cache_info()["size"] == 4
        _read.set_schemaless_plan_cache_size(0)
        assert _read.schemaless_plan_cache_info()["size"] == 0
        assert fastavro.schemaless_reader(BytesIO(b"\x02"), schemas[0]) == {"x": 1}
        assert _read.schemaless_plan_cache_info()["size"] == 0
    finally:
        _read.set_schemaless_plan_cache_size(previous)


def test_schemaless_read_does_not_copy_the_buffer():
    schema = parsed([{"name": "x", "type": "int"}])
    payload = b"\x02" + b"\x00" * ((1 << 20) - 1)
    fastavro.schemaless_reader(BytesIO(payload), schema)
    tracemalloc.start()
    fastavro.schemaless_reader(BytesIO(payload), schema)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert peak < 64 * 1024
