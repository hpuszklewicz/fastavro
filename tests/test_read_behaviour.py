import pickle
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


WRITER = record_schema(
    "R",
    [
        {"name": "a", "type": "int"},
        {"name": "b", "type": "string"},
        {"name": "c", "type": "long"},
        {"name": "d", "type": "bytes"},
        {"name": "e", "type": "float"},
        {"name": "old_name", "type": "int"},
        {"name": "dropped", "type": "string"},
    ],
)
WRITER_RECORD = {
    "a": 1,
    "b": "two",
    "c": 3,
    "d": b"four",
    "e": 5.0,
    "old_name": 6,
    "dropped": "gone",
}
READER = record_schema(
    "R",
    [
        {"name": "added", "type": "string", "default": "dflt"},
        {"name": "c", "type": "double"},
        {"name": "b", "type": "bytes"},
        {"name": "a", "type": "long"},
        {"name": "new_name", "type": "int", "aliases": ["old_name"]},
        {"name": "d", "type": "string"},
        {"name": "e", "type": "double"},
        {"name": "added_null", "type": ["null", "int"], "default": None},
    ],
)
UNION_SCHEMA = record_schema(
    "Top",
    [
        {
            "name": "u",
            "type": [
                "null",
                record_schema("A", [{"name": "x", "type": "int"}]),
                record_schema("B", [{"name": "y", "type": "int"}]),
                {"type": "enum", "name": "E", "symbols": ["P", "Q"]},
                "string",
            ],
        },
        {"name": "ref", "type": ["null", "t.A"]},
    ],
)
UNION_RECORDS = [
    {"u": None, "ref": None},
    {"u": ("t.A", {"x": 1}), "ref": ("t.A", {"x": 2})},
    {"u": ("t.B", {"y": 3}), "ref": None},
    {"u": "P", "ref": None},
    {"u": "s", "ref": None},
]
LINKED = record_schema(
    "Node",
    [
        {"name": "value", "type": "long"},
        {"name": "next", "type": ["null", "Node"], "default": None},
    ],
)
TREE = record_schema(
    "Tree",
    [
        {"name": "label", "type": "string"},
        {
            "name": "children",
            "type": {
                "type": "array",
                "items": record_schema(
                    "Edge",
                    [{"name": "w", "type": "double"}, {"name": "to", "type": "Tree"}],
                ),
            },
        },
    ],
)


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


def test_resolution_values_and_key_order():
    [out] = roundtrip_file(WRITER, [WRITER_RECORD], READER)
    assert list(out) == ["a", "b", "c", "d", "e", "new_name", "added", "added_null"]
    assert out == {
        "a": 1,
        "b": b"two",
        "c": 3.0,
        "d": "four",
        "e": 5.0,
        "new_name": 6,
        "added": "dflt",
        "added_null": None,
    }
    assert [type(out[k]) for k in "abcde"] == [int, bytes, float, str, float]


def test_reader_union_picks_matching_branch_and_promotes():
    schema = record_schema("R", [{"name": "a", "type": "int"}])
    reader_schema = record_schema("R", [{"name": "a", "type": ["null", "double"]}])
    assert roundtrip_file(schema, [{"a": 7}], reader_schema) == [{"a": 7.0}]


def test_reader_union_uses_first_matching_branch_even_over_an_exact_match():
    # Upstream behaviour, pinned so that changing it is a conscious decision:
    # bytes resolve to the earlier "string" branch by promotion, not to the
    # later exact "bytes" branch.
    schema = record_schema("R", [{"name": "u", "type": ["string", "bytes"]}])
    assert roundtrip_file(schema, [{"u": b"abc"}], schema) == [{"u": "abc"}]
    with pytest.raises(UnicodeDecodeError):
        roundtrip_file(schema, [{"u": b"\xff\xfe"}], schema)


def test_enum_resolution_default_and_error():
    def enum(symbols, **extra):
        return record_schema(
            "R",
            [
                {
                    "name": "e",
                    "type": {"type": "enum", "name": "E", "symbols": symbols, **extra},
                }
            ],
        )

    records = [{"e": "C"}, {"e": "B"}]
    schema = enum(["A", "B", "C"])
    assert roundtrip_file(schema, records, enum(["A", "B"], default="A")) == [
        {"e": "A"},
        {"e": "B"},
    ]
    with pytest.raises(SchemaResolutionError):
        roundtrip_file(schema, records, enum(["A", "B"]))


def test_nested_record_resolution_and_map_value_promotion():
    def outer(inner_fields, map_values):
        return record_schema(
            "Outer",
            [
                {"name": "inner", "type": record_schema("Inner", inner_fields)},
                {"name": "m", "type": {"type": "map", "values": map_values}},
                {"name": "again", "type": "t.Inner"},
            ],
        )

    schema = outer([{"name": "x", "type": "int"}, {"name": "y", "type": "int"}], "int")
    reader_schema = outer(
        [{"name": "y", "type": "long"}, {"name": "z", "type": "int", "default": 9}],
        "double",
    )
    rec = {"inner": {"x": 1, "y": 2}, "m": {"k": 3}, "again": {"x": 4, "y": 5}}
    assert roundtrip_file(schema, [rec], reader_schema) == [
        {"inner": {"y": 2, "z": 9}, "m": {"k": 3.0}, "again": {"y": 5, "z": 9}}
    ]


@pytest.mark.parametrize(
    "options,expected",
    [
        (
            {},
            [
                {"u": None, "ref": None},
                {"u": {"x": 1}, "ref": {"x": 2}},
                {"u": {"y": 3}, "ref": None},
                {"u": "P", "ref": None},
                {"u": "s", "ref": None},
            ],
        ),
        (
            {"return_record_name": True},
            [
                {"u": None, "ref": None},
                {"u": ("t.A", {"x": 1}), "ref": ("t.A", {"x": 2})},
                {"u": ("t.B", {"y": 3}), "ref": None},
                {"u": "P", "ref": None},
                {"u": "s", "ref": None},
            ],
        ),
        (
            {"return_record_name": True, "return_record_name_override": True},
            [
                {"u": None, "ref": None},
                {"u": ("t.A", {"x": 1}), "ref": {"x": 2}},
                {"u": ("t.B", {"y": 3}), "ref": None},
                {"u": "P", "ref": None},
                {"u": "s", "ref": None},
            ],
        ),
        (
            {"return_named_type": True},
            [
                {"u": None, "ref": None},
                {"u": ("t.A", {"x": 1}), "ref": ("t.A", {"x": 2})},
                {"u": ("t.B", {"y": 3}), "ref": None},
                {"u": ("t.E", "P"), "ref": None},
                {"u": "s", "ref": None},
            ],
        ),
        (
            {"return_named_type": True, "return_named_type_override": True},
            [
                {"u": None, "ref": None},
                {"u": ("t.A", {"x": 1}), "ref": {"x": 2}},
                {"u": ("t.B", {"y": 3}), "ref": None},
                {"u": ("t.E", "P"), "ref": None},
                {"u": "s", "ref": None},
            ],
        ),
    ],
)
def test_union_naming_options(options, expected):
    assert roundtrip_file(UNION_SCHEMA, UNION_RECORDS, **options) == expected


def test_return_record_name_uses_the_reader_schema_name():
    schema = record_schema(
        "Top",
        [
            {
                "name": "u",
                "type": ["null", record_schema("A", [{"name": "x", "type": "int"}])],
            }
        ],
    )
    reader_schema = record_schema(
        "Top",
        [
            {
                "name": "u",
                "type": [
                    "null",
                    {
                        **record_schema(
                            "Renamed",
                            [
                                {"name": "x", "type": "long"},
                                {"name": "w", "type": "int", "default": 0},
                            ],
                        ),
                        "aliases": ["A"],
                    },
                ],
            }
        ],
    )
    out = roundtrip_file(
        schema, [{"u": ("t.A", {"x": 1})}], reader_schema, return_record_name=True
    )
    assert out == [{"u": ("t.Renamed", {"x": 1, "w": 0})}]


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


def test_unknown_logical_type_is_ignored_and_promotion_still_applies():
    schema = record_schema(
        "R", [{"name": "n", "type": {"type": "int", "logicalType": "nope"}}]
    )
    reader_schema = record_schema("R", [{"name": "n", "type": "double"}])
    assert roundtrip_file(schema, [{"n": 3}], reader_schema) == [{"n": 3.0}]


def test_block_reader_iterates_blocks_then_records():
    schema = record_schema("R", [{"name": "s", "type": "string"}])
    records = [{"s": str(i)} for i in range(5000)]
    fo = BytesIO()
    fastavro.writer(fo, schema, records, sync_interval=2000)
    fo.seek(0)
    blocks = list(fastavro.block_reader(fo))
    assert len(blocks) > 1
    assert sum(b.num_records for b in blocks) == len(records)
    assert [rec for b in blocks for rec in b] == records


def test_block_reader_with_reader_schema():
    schema = record_schema("R", [{"name": "n", "type": "int"}])
    reader_schema = record_schema(
        "R", [{"name": "n", "type": "long"}, {"name": "d", "type": "int", "default": 1}]
    )
    fo = BytesIO()
    fastavro.writer(fo, schema, [{"n": i} for i in range(10)])
    fo.seek(0)
    out = [rec for b in fastavro.block_reader(fo, reader_schema) for rec in b]
    assert out == [{"n": i, "d": 1} for i in range(10)]


def test_block_can_be_pickled_after_another_block_was_decoded():
    schema = record_schema("R", [{"name": "s", "type": "string"}])
    fo = BytesIO()
    fastavro.writer(
        fo, schema, [{"s": str(i)} for i in range(5000)], sync_interval=2000
    )
    fo.seek(0)
    first, second, *_ = fastavro.block_reader(fo)
    list(first)
    assert list(pickle.loads(pickle.dumps(second))) == list(second)


@pytest.mark.parametrize(
    "schema,record",
    [
        (
            LINKED,
            {"value": 1, "next": {"value": 2, "next": {"value": 3, "next": None}}},
        ),
        (
            TREE,
            {
                "label": "root",
                "children": [
                    {"w": 0.5, "to": {"label": "a", "children": []}},
                    {
                        "w": 1.5,
                        "to": {
                            "label": "b",
                            "children": [
                                {"w": 2.0, "to": {"label": "c", "children": []}}
                            ],
                        },
                    },
                ],
            },
        ),
    ],
)
def test_recursive_schemas_roundtrip(schema, record):
    assert roundtrip_file(schema, [record]) == [record]
    assert roundtrip_file(schema, [record], schema) == [record]


def test_schemaless_reader_leaves_position_after_datum():
    schema = record_schema(
        "R", [{"name": "s", "type": "string"}, {"name": "n", "type": "long"}]
    )
    parsed = fastavro.parse_schema(schema)
    one = schemaless_bytes(schema, {"s": "abc", "n": 5})
    two = schemaless_bytes(schema, {"s": "defg", "n": -7})
    fo = BytesIO(one + two + b"trailing")
    assert fastavro.schemaless_reader(fo, parsed) == {"s": "abc", "n": 5}
    assert fo.tell() == len(one)
    assert fastavro.schemaless_reader(fo, parsed) == {"s": "defg", "n": -7}
    assert fo.read() == b"trailing"


def test_schemaless_reader_accepts_any_file_like_object():
    class Stream:
        def __init__(self, data):
            self.data = data
            self.pos = 0

        def read(self, n=-1):
            chunk = self.data[self.pos : None if n < 0 else self.pos + n]
            self.pos += len(chunk)
            return chunk

    schema = record_schema(
        "R", [{"name": "s", "type": "string"}, {"name": "n", "type": "long"}]
    )
    data = schemaless_bytes(schema, {"s": "abc", "n": 5})
    assert fastavro.schemaless_reader(Stream(data), schema) == {"s": "abc", "n": 5}


def test_schemaless_reader_uses_the_read_method_of_a_bytesio_subclass():
    class Shifted(BytesIO):
        def read(self, n=-1):
            return bytes(b - 4 for b in super().read(n))

    schema = fastavro.parse_schema(record_schema("R", [{"name": "x", "type": "int"}]))
    assert fastavro.schemaless_reader(Shifted(b"\x06"), schema) == {"x": 1}


def test_schemaless_reader_parsed_and_unparsed_schemas_agree():
    schema = record_schema("R", [{"name": "n", "type": "int"}])
    data = schemaless_bytes(schema, {"n": 4})
    parsed = fastavro.parse_schema(schema)
    reader_schema = record_schema("R", [{"name": "n", "type": "double"}])
    for writer in (schema, parsed):
        assert fastavro.schemaless_reader(BytesIO(data), writer) == {"n": 4}
        assert fastavro.schemaless_reader(BytesIO(data), writer, writer) == {"n": 4}
        assert fastavro.schemaless_reader(BytesIO(data), writer, reader_schema) == {
            "n": 4.0
        }
