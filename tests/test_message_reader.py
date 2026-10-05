import random
from io import BytesIO

import pytest

import fastavro
from fastavro.read import LOGICAL_READERS, MessageReader, SchemaResolutionError

from .test_read_behaviour import (
    LINKED,
    READER,
    UNION_RECORDS,
    UNION_SCHEMA,
    WRITER,
    WRITER_RECORD,
    outcome,
    record_schema,
    schemaless_bytes,
)


def assert_same_as_schemaless(data, writer, reader=None, **options):
    # Callers pass unparsed schemas, so schemaless_reader takes the generic path
    # and MessageReader is compared against independent code.
    expected = outcome(
        lambda: fastavro.schemaless_reader(BytesIO(data), writer, reader, **options)
    )
    got = outcome(lambda: MessageReader(writer, reader, **options).read(data))
    if got == ("err", EOFError):
        assert expected[1] in (EOFError, IndexError)
    else:
        assert got == expected
        if got[0] == "ok" and isinstance(got[1], dict):
            assert list(got[1]) == list(expected[1])
    return got


def test_resolution_matches_and_reader_schema_equal_to_writer_is_dropped():
    data = schemaless_bytes(WRITER, WRITER_RECORD)
    got = assert_same_as_schemaless(data, WRITER, READER)
    assert list(got[1]) == ["a", "b", "c", "d", "e", "new_name", "added", "added_null"]
    assert MessageReader(WRITER, WRITER).reader_schema is None


def test_resolution_error_is_raised_on_read_not_at_construction():
    schema = record_schema("R", [{"name": "a", "type": "int"}])
    r = MessageReader(schema, record_schema("R", [{"name": "a", "type": "string"}]))
    with pytest.raises(SchemaResolutionError):
        r.read(schemaless_bytes(schema, {"a": 1}))


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"return_record_name": True},
        {"return_record_name": True, "return_record_name_override": True},
        {"return_named_type": True},
        {"return_named_type": True, "return_named_type_override": True},
    ],
)
def test_union_naming_options(options):
    for rec in UNION_RECORDS:
        data = schemaless_bytes(UNION_SCHEMA, rec)
        assert_same_as_schemaless(data, UNION_SCHEMA, **options)
        assert_same_as_schemaless(data, UNION_SCHEMA, UNION_SCHEMA, **options)


@pytest.mark.parametrize("errors", ["strict", "replace", "ignore"])
def test_handle_unicode_errors(errors):
    schema = record_schema("R", [{"name": "s", "type": "string"}])
    assert_same_as_schemaless(b"\x04\xff\xfe", schema, handle_unicode_errors=errors)


def test_truncation_and_trailing_bytes():
    data = schemaless_bytes(WRITER, WRITER_RECORD)
    for cut in range(len(data)):
        assert_same_as_schemaless(data[:cut], WRITER)
    r = MessageReader(WRITER)
    assert r.read(data + b"trailing") == r.read(data)


def test_accepts_any_buffer():
    data = schemaless_bytes(WRITER, WRITER_RECORD)
    r = MessageReader(WRITER)
    assert r.read(memoryview(data)) == r.read(bytearray(data)) == r.read(data)


def test_recursive_schema():
    chain = {"value": 1, "next": {"value": 2, "next": None}}
    data = schemaless_bytes(LINKED, chain)
    assert MessageReader(LINKED).read(data) == chain
    assert MessageReader(LINKED, LINKED).read(data) == chain


def test_logical_readers_are_bound_at_construction_only_by_compiled_plans():
    key = "int-message-reader-probe"
    schema = record_schema(
        "R",
        [{"name": "x", "type": {"type": "int", "logicalType": "message-reader-probe"}}],
    )
    data = schemaless_bytes(schema, {"x": 1})
    try:
        LOGICAL_READERS[key] = lambda d, w, r: ("first", d)
        r = MessageReader(schema)
        LOGICAL_READERS[key] = lambda d, w, r: ("second", d)
        bound = "first" if fastavro.read.read_plan_enabled() else "second"
        assert r.read(data) == {"x": (bound, 1)}
        assert MessageReader(schema).read(data) == {"x": ("second", 1)}
        if fastavro.read.set_read_plan_enabled(False):
            try:
                assert r.read(data) == {"x": ("second", 1)}
            finally:
                fastavro.read.set_read_plan_enabled(True)
    finally:
        LOGICAL_READERS.pop(key, None)


def test_matches_schemaless_reader_on_generated_schemas():
    from .test_read_plan import N_SEEDS, make_file, mutate_reader

    for seed in range(0, N_SEEDS, 2):
        schema, records, _ = make_file(seed, n_records=1)
        data = schemaless_bytes(schema, records[0])
        assert_same_as_schemaless(data, schema)
        reader, _ = mutate_reader(schema, random.Random(seed))
        assert_same_as_schemaless(data, schema, reader)
        assert_same_as_schemaless(data, schema, return_record_name=True)
