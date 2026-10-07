import os
import pickle
import random
import subprocess
import sys
import threading
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
    previous = _read.set_schemaless_plan_cache_size(0)  # start empty
    _read.set_schemaless_plan_cache_size(4)
    try:
        fields = [{"name": "x", "type": "int"}]
        schemas = [parsed(fields) for _ in range(6)]
        assert all(
            fastavro.schemaless_reader(BytesIO(b"\x02"), s) == {"x": 1} for s in schemas
        )
        assert _read.schemaless_plan_cache_info() == {"size": 4, "capacity": 4}
        # A cached entry keeps its schema alive, so evicted schemas have fewer
        # references than cached ones: the two oldest were evicted. (A for loop
        # above would leave its variable referencing the last schema.)
        counts = [sys.getrefcount(s) for s in schemas]
        assert counts[0] == counts[1] < counts[2] == counts[3] == counts[4] == counts[5]
        fastavro.schemaless_reader(
            BytesIO(b"\x02"), schemas[5], return_record_name=True
        )
        assert _read.schemaless_plan_cache_info()["size"] == 4
        assert sys.getrefcount(schemas[2]) == sys.getrefcount(schemas[0])
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


def test_compiled_plans_refuse_to_be_pickled():
    plan = _read.compile_read_plan(
        parsed([{"name": "x", "type": "int"}]), {"writer": {}, "reader": {}}, None, {}
    )
    with pytest.raises(TypeError):
        pickle.dumps(plan)


@pytest.mark.parametrize("capacity", [8, 2], ids=["cache never full", "cache full"])
def test_replaced_logical_readers_do_not_pile_up_in_the_cache(capacity):
    # Replacing a logical reader invalidates the cached plans that captured
    # it; their keys must not accumulate, whether or not the cache evicts.
    key = "int-cache-churn-probe"
    logical = {"type": "int", "logicalType": "cache-churn-probe"}
    schemas = [parsed([{"name": "x", "type": logical}]) for _ in range(3)]
    previous = _read.set_schemaless_plan_cache_size(0)  # start empty
    _read.set_schemaless_plan_cache_size(capacity)

    def churn(rounds):
        for _ in range(rounds):
            LOGICAL_READERS[key] = lambda data, w, r: data  # a new function
            for s in schemas:
                assert fastavro.schemaless_reader(BytesIO(b"\x02"), s) == {"x": 1}

    try:
        churn(200)
        tracemalloc.start()
        churn(5000)
        grown, _ = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert _read.schemaless_plan_cache_info()["size"] == min(capacity, 3)
        assert grown < 64 * 1024
    finally:
        LOGICAL_READERS.pop(key, None)
        _read.set_schemaless_plan_cache_size(previous)


def test_concurrent_reads_while_the_cache_evicts():
    # Meaningful on free-threaded builds (PYTHON_GIL=0), where threads use the
    # cache at the same time; with a GIL it is a smoke test.
    schemas = [parsed([{"name": "x", "type": "int"}]) for _ in range(32)]
    errors = []

    def work(seed):
        rnd = random.Random(seed)
        try:
            for _ in range(2000):
                s = rnd.choice(schemas)
                assert fastavro.schemaless_reader(BytesIO(b"\x02"), s) == {"x": 1}
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    previous = _read.set_schemaless_plan_cache_size(4)
    try:
        threads = [threading.Thread(target=work, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        _read.set_schemaless_plan_cache_size(previous)
    assert errors == []


@pytest.mark.parametrize("setting,capacity", [(None, 3072), ("100", 100)])
def test_cache_capacity_default_and_environment_variable(setting, capacity):
    # The variable is read when fastavro is imported: check in a new process.
    env = {k: v for k, v in os.environ.items() if k != "FASTAVRO_SCHEMALESS_PLAN_CACHE"}
    if setting is not None:
        env["FASTAVRO_SCHEMALESS_PLAN_CACHE"] = setting
    code = "from fastavro.read import _read; print(_read.schemaless_plan_cache_info()['capacity'])"
    out = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert int(out.stdout) == capacity
