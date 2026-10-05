from io import BytesIO

import pytest

import fastavro
from fastavro.read import SchemaResolutionError, LOGICAL_READERS


def record_schema(name, fields, namespace="t"):
    return {"type": "record", "name": name, "namespace": namespace, "fields": fields}


def roundtrip_file(schema, records, reader_schema=None, **options):
    fo = BytesIO()
    fastavro.writer(fo, fastavro.parse_schema(schema), records)
    fo.seek(0)
    return list(fastavro.reader(fo, reader_schema, **options))


def schemaless_bytes(schema, record):
    fo = BytesIO()
    fastavro.schemaless_writer(fo, fastavro.parse_schema(schema), record)
    return fo.getvalue()


def outcome(fn):
    try:
        return ("ok", fn())
    except Exception as e:  # noqa: BLE001
        return ("err", type(e))


@pytest.mark.parametrize(
    "writer_field,reader_field,record",
    [
        ({"name": "a", "type": "int"}, {"name": "a", "type": "string"}, {"a": 1}),
        (
            {"name": "a", "type": {"type": "array", "items": "int"}},
            {"name": "a", "type": {"type": "array", "items": "string"}},
            {"a": []},
        ),
    ],
)
def test_resolution_error_is_raised_on_first_record_not_at_construction(
    writer_field, reader_field, record
):
    fo = BytesIO()
    fastavro.writer(fo, record_schema("R", [writer_field]), [record])
    fo.seek(0)
    r = fastavro.reader(fo, record_schema("R", [reader_field]))
    with pytest.raises(SchemaResolutionError):
        next(r)


def test_union_branch_mismatch_only_raises_when_branch_is_taken():
    schema = record_schema("R", [{"name": "a", "type": ["null", "string"]}])
    reader_schema = record_schema("R", [{"name": "a", "type": ["null", "int"]}])
    assert roundtrip_file(schema, [{"a": None}] * 3, reader_schema) == [{"a": None}] * 3
    with pytest.raises(SchemaResolutionError):
        roundtrip_file(schema, [{"a": None}, {"a": "x"}], reader_schema)


def test_missing_default_raises():
    schema = record_schema("R", [{"name": "a", "type": "int"}])
    reader_schema = record_schema(
        "R", [{"name": "a", "type": "int"}, {"name": "b", "type": "int"}]
    )
    with pytest.raises(SchemaResolutionError):
        roundtrip_file(schema, [{"a": 1}], reader_schema)


@pytest.mark.parametrize(
    "schema,record",
    [
        (record_schema("R", [{"name": "s", "type": "string"}]), {"s": "hello world"}),
        (record_schema("R", [{"name": "b", "type": "bytes"}]), {"b": b"0123456789"}),
        (record_schema("R", [{"name": "d", "type": "double"}]), {"d": 1.5}),
        (record_schema("R", [{"name": "f", "type": "float"}]), {"f": 1.5}),
        (
            record_schema(
                "R", [{"name": "x", "type": {"type": "fixed", "name": "F", "size": 4}}]
            ),
            {"x": b"abcd"},
        ),
        (record_schema("R", [{"name": "b", "type": "boolean"}]), {"b": True}),
    ],
)
def test_truncated_value_body_raises_eof(schema, record):
    data = schemaless_bytes(schema, record)
    with pytest.raises(EOFError):
        fastavro.schemaless_reader(BytesIO(data[:-1]), fastavro.parse_schema(schema))


def test_truncated_varint_raises():
    # Upstream lets IndexError escape here; a replacement may tighten it to
    # EOFError, so both are accepted.
    schema = record_schema("R", [{"name": "n", "type": "long"}])
    data = schemaless_bytes(schema, {"n": 1 << 40})
    with pytest.raises((EOFError, IndexError)):
        fastavro.schemaless_reader(BytesIO(data[:2]), fastavro.parse_schema(schema))


def test_truncated_file_raises_eof_after_the_complete_blocks():
    schema = record_schema("R", [{"name": "s", "type": "string"}])
    records = [{"s": str(i) * 10} for i in range(200)]
    fo = BytesIO()
    fastavro.writer(fo, schema, records, sync_interval=500)
    data = fo.getvalue()
    read = []
    with pytest.raises(EOFError):
        for record in fastavro.reader(BytesIO(data[: len(data) - 30])):
            read.append(record)
    assert 0 < len(read) < len(records)
    assert read == records[: len(read)]


@pytest.mark.parametrize(
    "field_type,payload",
    [
        (["null", "string"], b"\x04"),
        ({"type": "enum", "name": "E", "symbols": ["A", "B"]}, b"\x06"),
    ],
)
def test_index_out_of_range_raises_index_error(field_type, payload):
    schema = fastavro.parse_schema(
        record_schema("R", [{"name": "f", "type": field_type}])
    )
    with pytest.raises(IndexError):
        fastavro.schemaless_reader(BytesIO(payload), schema)


@pytest.mark.parametrize(
    "errors,expected",
    [("replace", {"s": "\ufffd\ufffd"}), ("ignore", {"s": ""})],
)
def test_handle_unicode_errors(errors, expected):
    schema = fastavro.parse_schema(
        record_schema("R", [{"name": "s", "type": "string"}])
    )
    data = b"\x04\xff\xfe"
    with pytest.raises(UnicodeDecodeError):
        fastavro.schemaless_reader(BytesIO(data), schema)
    assert (
        fastavro.schemaless_reader(BytesIO(data), schema, handle_unicode_errors=errors)
        == expected
    )


def test_logical_reader_registered_before_reading_is_used():
    key = "string-test-upper"
    schema = record_schema(
        "R", [{"name": "s", "type": {"type": "string", "logicalType": "test-upper"}}]
    )
    parsed = fastavro.parse_schema(schema)
    data = schemaless_bytes(schema, {"s": "abc"})
    LOGICAL_READERS[key] = lambda data, w, r: data.upper()
    try:
        assert roundtrip_file(schema, [{"s": "abc"}]) == [{"s": "ABC"}]
        assert fastavro.schemaless_reader(BytesIO(data), parsed) == {"s": "ABC"}
    finally:
        del LOGICAL_READERS[key]
    assert roundtrip_file(schema, [{"s": "abc"}]) == [{"s": "abc"}]
    assert fastavro.schemaless_reader(BytesIO(data), parsed) == {"s": "abc"}
