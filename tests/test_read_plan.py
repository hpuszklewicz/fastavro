import datetime
import os
import random
import tracemalloc
import uuid
from decimal import Decimal
from io import BytesIO

import pytest

import fastavro
from fastavro.read import SchemaResolutionError, _read

from .test_read_behaviour import outcome

pytestmark = pytest.mark.skipif(
    not hasattr(_read, "CYTHON_MODULE"), reason="compiled read plans are Cython-only"
)

N_SEEDS = int(os.environ.get("FASTAVRO_FUZZ_SEEDS", "40"))

PRIMITIVES = ["null", "boolean", "int", "long", "float", "double", "bytes", "string"]
LOGICALS = [
    {"type": "long", "logicalType": "timestamp-micros"},
    {"type": "long", "logicalType": "timestamp-millis"},
    {"type": "int", "logicalType": "date"},
    {"type": "int", "logicalType": "time-millis"},
    {"type": "string", "logicalType": "uuid"},
    {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2},
]
# bytes -> string is also a promotion, but random bytes are not valid UTF-8
PROMOTIONS = {
    "int": ["long", "float", "double"],
    "long": ["float", "double"],
    "float": ["double"],
    "string": ["bytes"],
}
WORDS = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"]


class SchemaGen:
    def __init__(self, seed):
        self.rng = random.Random(seed)
        self.counter = 0
        self.named = []

    def name(self, prefix):
        self.counter += 1
        return f"{prefix}{self.counter}"

    def enum(self):
        n = self.name("E")
        self.named.append(f"ns.{n}")
        return {
            "type": "enum",
            "name": n,
            "symbols": [f"S{i}" for i in range(self.rng.randint(2, 5))],
        }

    def fixed(self):
        n = self.name("F")
        self.named.append(f"ns.{n}")
        return {"type": "fixed", "name": n, "size": self.rng.choice([1, 4, 8])}

    def any_type(self, depth, in_union=False):
        r = self.rng.random()
        if depth >= 2 or r < 0.40:
            return self.rng.choice(PRIMITIVES)
        if r < 0.50:
            return dict(self.rng.choice(LOGICALS))
        if r < 0.60 and not in_union:
            return ["null", self.any_type(depth + 1, in_union=True)]
        if r < 0.66 and not in_union:
            a, b = self.rng.sample(PRIMITIVES[1:], 2)
            if {a, b} == {"string", "bytes"}:
                # a reader resolves bytes to the earlier "string" branch and
                # fails on random bytes (pinned in test_read_behaviour)
                b = "int"
            return [a, b]
        if r < 0.74:
            return {"type": "array", "items": self.any_type(depth + 1)}
        if r < 0.80:
            return {"type": "map", "values": self.any_type(depth + 1)}
        if r < 0.85:
            return self.enum()
        if r < 0.88:
            return self.fixed()
        if r < 0.93 and self.named:
            return self.rng.choice(self.named)
        if r < 0.97 and not in_union:
            return ["null", self.record(depth + 1)]
        return self.record(depth + 1)

    def record(self, depth=0, nfields=None, recursive=False):
        n = self.name("R")
        fields = [
            {"name": f"f{i}", "type": self.any_type(depth)}
            for i in range(nfields or self.rng.randint(1, 6))
        ]
        if recursive:
            fields.append(
                {"name": "next", "type": ["null", f"ns.{n}"], "default": None}
            )
        self.named.append(f"ns.{n}")
        rec = {"type": "record", "name": n, "fields": fields}
        if depth == 0:
            rec["namespace"] = "ns"
        return rec


class RecordGen:
    """Values for a parsed schema; unions use the tuple notation so the branch
    is explicit."""

    def __init__(self, seed, named_schemas):
        self.rng = random.Random(seed)
        self.named = named_schemas
        self.depth = 0

    def value(self, schema):
        if isinstance(schema, list):
            branch = self.rng.choice(schema)
            if isinstance(branch, str) and branch not in PRIMITIVES:
                return (branch, self.value(self.named[branch]))
            if isinstance(branch, dict) and branch["type"] in (
                "record",
                "enum",
                "fixed",
            ):
                return (branch["name"], self.value(branch))
            return self.value(branch)
        if isinstance(schema, str):
            if schema in PRIMITIVES:
                return self.primitive(schema)
            return self.value(self.named[schema])
        t = schema["type"]
        lt = schema.get("logicalType")
        if lt in ("timestamp-micros", "timestamp-millis"):
            return datetime.datetime(
                2020, 1, 1, tzinfo=datetime.timezone.utc
            ) + datetime.timedelta(
                seconds=self.rng.randint(0, 10**8),
                milliseconds=self.rng.randint(0, 999),
            )
        if lt == "date":
            return datetime.date(2020, 1, 1) + datetime.timedelta(
                days=self.rng.randint(0, 3000)
            )
        if lt == "time-millis":
            return datetime.time(
                self.rng.randint(0, 23),
                self.rng.randint(0, 59),
                0,
                self.rng.randint(0, 999) * 1000,
            )
        if lt == "uuid":
            return uuid.UUID(int=self.rng.getrandbits(128))
        if lt == "decimal":
            return Decimal(self.rng.randint(-(10**6), 10**6)).scaleb(-2)
        if t in PRIMITIVES:
            return self.primitive(t)
        if t == "array":
            return [self.value(schema["items"]) for _ in range(self.rng.randint(0, 3))]
        if t == "map":
            return {
                self.rng.choice(WORDS) + str(i): self.value(schema["values"])
                for i in range(self.rng.randint(0, 3))
            }
        if t == "enum":
            return self.rng.choice(schema["symbols"])
        if t == "fixed":
            return bytes(self.rng.randrange(256) for _ in range(schema["size"]))
        self.depth += 1
        try:
            return {
                f["name"]: (
                    None
                    if f["name"] == "next" and self.depth > 3
                    else self.value(f["type"])
                )
                for f in schema["fields"]
            }
        finally:
            self.depth -= 1

    def primitive(self, t):
        r = self.rng
        return {
            "null": lambda: None,
            "boolean": lambda: r.random() < 0.5,
            "int": lambda: r.randint(-(2**31), 2**31 - 1),
            "long": lambda: r.choice(
                [r.randint(-100, 100), r.randint(-(2**63), 2**63 - 1)]
            ),
            "float": lambda: float(r.randint(-1000, 1000)) / 4,
            "double": lambda: r.random() * 1e6,
            "bytes": lambda: bytes(r.randrange(256) for _ in range(r.randint(0, 6))),
            "string": lambda: " ".join(r.choice(WORDS) for _ in range(r.randint(0, 4)))
            + r.choice(["", "é", "日本", "🙂"]),
        }[t]()


def mutate_reader(schema, rng):
    """A reader-schema variant exercising one resolution rule; returns
    (reader_schema, expect_error)."""
    reader = dict(schema)
    fields = [dict(f) for f in schema["fields"]]
    expect_error = False
    kind = rng.choice(
        [
            "same",
            "reorder",
            "drop",
            "add_default",
            "alias",
            "promote",
            "wrap_union",
            "mismatch",
            "add_no_default",
        ]
    )
    if kind == "reorder":
        # a by-name reference must stay after its inline definition
        if not any(
            isinstance(f["type"], str) and f["type"] not in PRIMITIVES for f in fields
        ):
            rng.shuffle(fields)
    elif kind == "drop" and len(fields) > 1:
        del fields[rng.randrange(len(fields))]
    elif kind == "add_default":
        fields.insert(
            rng.randint(0, len(fields)),
            {"name": "added", "type": "string", "default": "d"},
        )
    elif kind == "add_no_default":
        fields.append({"name": "added", "type": "int"})
        expect_error = True
    elif kind == "alias":
        f = rng.choice(fields)
        f["aliases"] = [f["name"]]
        f["name"] += "_renamed"
    elif kind == "promote":
        cands = [
            f for f in fields if isinstance(f["type"], str) and f["type"] in PROMOTIONS
        ]
        if cands:
            f = rng.choice(cands)
            f["type"] = rng.choice(PROMOTIONS[f["type"]])
    elif kind == "wrap_union":
        cands = [
            f
            for f in fields
            if isinstance(f["type"], str) and f["type"] in PRIMITIVES[1:]
        ]
        if cands:
            f = rng.choice(cands)
            f["type"] = ["null", f["type"]]
    elif kind == "mismatch":
        cands = [
            f
            for f in fields
            if isinstance(f["type"], str)
            and f["type"] in ("int", "long", "string", "boolean")
        ]
        if cands:
            f = rng.choice(cands)
            f["type"] = "bytes" if f["type"] != "string" else "boolean"
            expect_error = True
    reader["fields"] = fields
    return reader, expect_error


def make_file(seed, n_records=None, recursive=False):
    schema = SchemaGen(seed).record(recursive=recursive)
    parsed = fastavro.parse_schema(schema)
    rgen = RecordGen(seed, parsed["__named_schemas"])
    records = [
        rgen.value(parsed)
        for _ in range(n_records or random.Random(seed).randint(1, 30))
    ]
    fo = BytesIO()
    fastavro.writer(fo, parsed, records, sync_marker=b"\x00" * 16)
    return schema, records, fo.getvalue()


def with_plan(enabled, fn):
    previous = fastavro.read.set_read_plan_enabled(enabled)
    try:
        return fn()
    finally:
        fastavro.read.set_read_plan_enabled(previous)


def same_outcome(a, b):
    # Documented difference: the plan raises EOFError on a truncated varint
    # where the generic reader could let IndexError escape.
    if a[0] == b[0] == "err" and {a[1], b[1]} == {EOFError, IndexError}:
        return True
    if a != b:
        return False
    return a[0] == "err" or [list(r) for r in a[1]] == [list(r) for r in b[1]]


def assert_same(data, reader_schema=None, **options):
    def read():
        return list(fastavro.reader(BytesIO(data), reader_schema, **options))

    plan = with_plan(True, lambda: outcome(read))
    generic = with_plan(False, lambda: outcome(read))
    assert same_outcome(plan, generic), (plan, generic)
    return plan


@pytest.mark.parametrize("seed", range(N_SEEDS))
def test_plain_decode_matches_generic_reader(seed):
    schema, records, data = make_file(seed)
    result = assert_same(data)
    assert result[0] == "ok" and len(result[1]) == len(records)


@pytest.mark.parametrize("seed", range(N_SEEDS))
def test_reader_schema_variants_match_generic_reader(seed):
    schema, records, data = make_file(seed)
    rng = random.Random(seed * 7919)
    for _ in range(4):
        reader_schema, expect_error = mutate_reader(schema, rng)
        result = assert_same(data, reader_schema)
        if expect_error:
            assert result == ("err", SchemaResolutionError)


@pytest.mark.parametrize("seed", range(0, N_SEEDS, 2))
@pytest.mark.parametrize(
    "options",
    [
        {"return_record_name": True},
        {"return_record_name": True, "return_record_name_override": True},
        {"return_named_type": True},
        {"return_named_type": True, "return_named_type_override": True},
        {"handle_unicode_errors": "replace"},
    ],
)
def test_options_match_generic_reader(seed, options):
    schema, records, data = make_file(seed)
    assert_same(data, **options)
    assert_same(data, schema, **options)


@pytest.mark.parametrize("seed", range(0, N_SEEDS, 4))
def test_recursive_schemas_match_generic_reader(seed):
    schema, records, data = make_file(seed, recursive=True)
    assert_same(data)
    assert_same(data, schema)


@pytest.mark.parametrize("seed", range(0, N_SEEDS, 8))
def test_truncation_at_every_offset_matches_generic_reader(seed):
    schema, records, data = make_file(seed, n_records=3)
    header_end = data.index(b"\x00" * 16) + 16
    for cut in range(header_end, len(data)):
        assert_same(data[:cut])


@pytest.mark.parametrize("seed", range(0, N_SEEDS, 2))
def test_schemaless_reader_matches_generic_reader(seed):
    schema, records, data = make_file(seed, n_records=1)
    parsed = fastavro.parse_schema(schema)
    fo = BytesIO()
    fastavro.schemaless_writer(fo, parsed, records[0])
    one = fo.getvalue()
    reader_schema, expect_error = mutate_reader(schema, random.Random(seed))
    parsed_reader = fastavro.parse_schema(reader_schema)

    def both(payload, writer, reader=None, **options):
        def read(stream):
            return fastavro.schemaless_reader(stream, writer, reader, **options)

        results = []
        for enabled in (True, False):
            stream = BytesIO(payload + b"trailing")
            results.append(
                (
                    with_plan(enabled, lambda: outcome(lambda: read(stream))),
                    stream.tell(),
                )
            )
        (plan, plan_pos), (generic, generic_pos) = results
        if plan[0] == "ok":
            assert plan == generic and list(plan[1]) == list(generic[1])
            assert plan_pos == generic_pos
        elif plan[1] is EOFError:
            assert generic[1] in (EOFError, IndexError)
        else:
            assert plan == generic
        return plan, plan_pos

    assert both(one, parsed) == both(one, schema)
    assert both(one, parsed)[1] == len(one)
    assert both(one, parsed, parsed_reader) == both(one, schema, reader_schema)
    if expect_error:
        assert both(one, parsed, parsed_reader)[0] == ("err", SchemaResolutionError)
    both(one, parsed, return_record_name=True)
    both(one, parsed, return_named_type=True)
    for cut in range(len(one)):
        both(one[:cut], parsed)


def test_memory_is_stable_across_repeated_decodes():
    schema, records, data = make_file(3, n_records=5000)
    tracemalloc.start()
    peaks = []
    for _ in range(5):
        tracemalloc.reset_peak()
        with_plan(True, lambda: list(fastavro.reader(BytesIO(data))))
        peaks.append(tracemalloc.get_traced_memory()[1])
    tracemalloc.stop()
    assert peaks[-1] <= peaks[1] * 1.05


def test_enum_error_message_matches_generic_reader():
    schema = fastavro.parse_schema(
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
    reader = fastavro.parse_schema(
        {
            "type": "record",
            "name": "R",
            "fields": [
                {
                    "name": "e",
                    "type": {"type": "enum", "name": "E", "symbols": ["A", "B"]},
                }
            ],
        }
    )
    messages = []
    for enabled in (True, False):
        with pytest.raises(SchemaResolutionError) as error:
            with_plan(
                enabled,
                lambda: fastavro.schemaless_reader(BytesIO(b"\x04"), schema, reader),
            )
        messages.append(str(error.value))
    assert (
        messages[0]
        == messages[1]
        == ("C not found in reader symbol list E, known symbols: ['A', 'B']")
    )


def test_enum_plan_size_does_not_depend_on_missing_symbols():
    # The "not found" message lists every reader symbol; built at compile time
    # for each missing writer symbol, a 5,000-symbol plan held over 50 MiB.
    writer = fastavro.parse_schema(
        {"type": "enum", "name": "E", "symbols": [f"S{i}" for i in range(5000)]}
    )
    reader = fastavro.parse_schema(
        {"type": "enum", "name": "E", "symbols": [f"S{i}" for i in range(0, 5000, 2)]}
    )
    tracemalloc.start()
    plan = _read.compile_read_plan(writer, {"writer": {}, "reader": {}}, reader, {})
    held, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert plan is not None
    assert held < 512 * 1024


@pytest.mark.parametrize(
    "reader_type,payload",
    [(["null", "int"], b"\x02\x02a"), ("int", b"\x00")],
    ids=["reader union", "reader not a union"],
)
def test_union_mismatch_message_matches_generic_reader(reader_type, payload):
    def record(field_type):
        return fastavro.parse_schema(
            {
                "type": "record",
                "name": "R",
                "fields": [{"name": "u", "type": field_type}],
            }
        )

    writer, reader = record(["null", "string"]), record(reader_type)
    messages = []
    for enabled in (True, False):
        with pytest.raises(SchemaResolutionError) as error:
            with_plan(
                enabled,
                lambda: fastavro.schemaless_reader(BytesIO(payload), writer, reader),
            )
        messages.append(str(error.value))
    assert messages[0] == messages[1]


def test_union_plan_size_does_not_grow_with_incompatible_branches():
    # Each incompatible branch used to get its own message repeating the whole
    # writer union: 500 record branches held almost 19 MiB.
    branches = [
        {"type": "record", "name": f"R{i}", "fields": [{"name": "x", "type": "int"}]}
        for i in range(500)
    ]

    def record(field_type):
        return fastavro.parse_schema(
            {
                "type": "record",
                "name": "Top",
                "fields": [{"name": "u", "type": field_type}],
            }
        )

    writer, reader = record(["null", *branches]), record(["null"])
    tracemalloc.start()
    message_reader = fastavro.read.MessageReader(writer, reader)
    held, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert message_reader.read(b"\x00") == {"u": None}
    assert held < 1024 * 1024


def defer_schemas(recursive):
    # Two writer fields aliased to one reader field: the generic reader raises
    # KeyError when it reads such a record, and only then.
    def schemas(last_fields):
        back = {
            "type": "record",
            "name": "B",
            "fields": [{"name": "back", "type": ["null", "A"]}],
        }
        first = [{"name": "x", "type": ["null", back]}] if recursive else []
        a = {"type": "record", "name": "A", "fields": first + last_fields}
        top = {
            "type": "record",
            "name": "Top",
            "fields": [{"name": "u", "type": ["null", a]}],
        }
        return fastavro.parse_schema(top)

    writer = schemas([{"name": "a", "type": "int"}, {"name": "b", "type": "int"}])
    reader = schemas([{"name": "c", "type": "int", "aliases": ["a", "b"]}])
    record = b"\x02" + (b"\x00" if recursive else b"") + b"\x02\x04"
    return writer, reader, record


@pytest.mark.parametrize("recursive", [False, True])
def test_compile_errors_are_raised_when_a_datum_reaches_them(recursive):
    writer, reader, record = defer_schemas(recursive)
    for payload in (b"\x00", record):
        plan, generic = (
            with_plan(
                enabled,
                lambda: outcome(
                    lambda: fastavro.schemaless_reader(BytesIO(payload), writer, reader)
                ),
            )
            for enabled in (True, False)
        )
        assert plan == generic
    assert generic == ("err", KeyError)
    message_reader = fastavro.read.MessageReader(writer, reader)
    assert message_reader.read(b"\x00") == {"u": None}
    errors = []
    for _ in range(2):
        with pytest.raises(KeyError) as error:
            message_reader.read(record)
        errors.append(error.value)
    assert errors[0] is not errors[1]


def test_stream_position_after_a_unicode_error_matches_generic_reader():
    schema = fastavro.parse_schema(
        {
            "type": "record",
            "name": "R",
            "fields": [{"name": "s", "type": "string"}, {"name": "n", "type": "int"}],
        }
    )
    positions = []
    for enabled in (True, False):
        fo = BytesIO(b"\x04\xff\xfe\x02")  # a 2-byte string that is not UTF-8
        with pytest.raises(UnicodeDecodeError):
            with_plan(enabled, lambda: fastavro.schemaless_reader(fo, schema))
        positions.append(fo.tell())
    assert positions == [3, 3]
