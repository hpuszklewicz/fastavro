# cython: language_level=3

"""Python code for reading AVRO files"""

# This code is a modified version of the code at
# http://svn.apache.org/viewvc/avro/trunk/lang/py/src/avro/ which is under
# Apache 2.0 license (http://www.apache.org/licenses/LICENSE-2.0)

import bz2
import lzma
import os
import sys
import zlib
from collections import deque
from datetime import datetime, timezone
from decimal import Context
from functools import partial
from io import BytesIO
from warnings import warn

import json

from .logical_readers import LOGICAL_READERS
from ._schema import (
    extract_record_type,
    is_single_record_union,
    is_single_name_union,
    extract_logical_type,
    parse_schema,
)
from ._read_common import (
    SchemaResolutionError,
    MAGIC,
    SYNC_SIZE,
    HEADER_SCHEMA,
    missing_codec_lib,
)
from .const import NAMED_TYPES, AVRO_TYPES

CYTHON_MODULE = 1  # Tests check this to confirm whether using the Cython code.

# Compiled read plans (see "Fast decoding path" below) can be switched off,
# e.g. to compare against the generic reader: FASTAVRO_READ_PLAN=0 in the
# environment at import time, or set_read_plan_enabled(False) at runtime.
_READ_PLAN_ENABLED = os.environ.get("FASTAVRO_READ_PLAN", "1") != "0"


def set_read_plan_enabled(enabled):
    """Enable or disable the compiled read plans; returns the previous value."""
    global _READ_PLAN_ENABLED
    previous = _READ_PLAN_ENABLED
    _READ_PLAN_ENABLED = bool(enabled)
    return previous


def read_plan_enabled():
    return _READ_PLAN_ENABLED

decimal_context = Context()
epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
epoch_naive = datetime(1970, 1, 1)

cimport cython
from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.unicode cimport PyUnicode_DecodeUTF8
from cpython.list cimport PyList_GET_ITEM, PyList_GET_SIZE

ctypedef int int32
ctypedef unsigned int uint32
ctypedef unsigned long long ulong64
ctypedef long long long64


class ReadError(Exception):
    pass


cpdef _default_named_schemas():
    return {"writer": {}, "reader": {}}


cpdef match_types(writer_type, reader_type, named_schemas):
    if isinstance(writer_type, list) or isinstance(reader_type, list):
        return True
    if isinstance(writer_type, dict) or isinstance(reader_type, dict):
        matching_schema = match_schemas(
            writer_type, reader_type, named_schemas, raise_on_error=False
        )
        return matching_schema is not None
    if writer_type == reader_type:
        return True
    # promotion cases
    elif writer_type == "int" and reader_type in ["long", "float", "double"]:
        return True
    elif writer_type == "long" and reader_type in ["float", "double"]:
        return True
    elif writer_type == "float" and reader_type == "double":
        return True
    elif writer_type == "string" and reader_type == "bytes":
        return True
    elif writer_type == "bytes" and reader_type == "string":
        return True
    writer_schema = named_schemas["writer"].get(writer_type)
    reader_schema = named_schemas["reader"].get(reader_type)
    if writer_schema is not None and reader_schema is not None:
        return match_types(writer_schema, reader_schema, named_schemas)
    return False


cpdef match_schemas(w_schema, r_schema, named_schemas, raise_on_error=True):
    if isinstance(w_schema, list):
        # If the writer is a union, checks will happen in read_union after the
        # correct schema is known
        return r_schema
    elif isinstance(r_schema, list):
        # If the reader is a union, ensure one of the new schemas is the same
        # as the writer
        for schema in r_schema:
            if match_types(w_schema, schema, named_schemas):
                return schema
        else:
            if raise_on_error:
                raise SchemaResolutionError(
                    f"Schema mismatch: {w_schema} is not {r_schema}"
                )
            else:
                return None
    else:
        # Check for dicts as primitive types are just strings
        if isinstance(w_schema, dict):
            w_type = w_schema["type"]
        else:
            w_type = w_schema
        if isinstance(r_schema, dict):
            r_type = r_schema["type"]
        else:
            r_type = r_schema

        if w_type == r_type == "map":
            if match_types(w_schema["values"], r_schema["values"], named_schemas):
                return r_schema
        elif w_type == r_type == "array":
            if match_types(w_schema["items"], r_schema["items"], named_schemas):
                return r_schema
        elif w_type in NAMED_TYPES and r_type in NAMED_TYPES:
            if w_type == r_type == "fixed" and w_schema["size"] != r_schema["size"]:
                if raise_on_error:
                    raise SchemaResolutionError(
                        f"Schema mismatch: {w_schema} size is different than {r_schema} size"
                    )
                else:
                    return None

            w_unqual_name = w_schema["name"].split(".")[-1]
            r_unqual_name = r_schema["name"].split(".")[-1]
            r_aliases = r_schema.get("aliases", [])
            if w_unqual_name == r_unqual_name or w_schema["name"] in r_aliases or w_unqual_name in r_aliases:
                return r_schema
        elif w_type not in AVRO_TYPES and r_type in NAMED_TYPES:
            if match_types(w_type, r_schema["name"], named_schemas):
                return r_schema["name"]
        elif match_types(w_type, r_type, named_schemas):
            return r_schema
        if raise_on_error:
            raise SchemaResolutionError(
                f"Schema mismatch: {w_schema} is not {r_schema}"
            )
        else:
            return None


cpdef inline read_null(fo):
    """null is written as zero bytes."""
    return None


cpdef inline skip_null(fo):
    """null is written as zero bytes."""
    pass


cpdef inline read_boolean(fo):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true).
    """
    cdef unsigned char ch_temp
    cdef bytes bytes_temp = fo.read(1)
    if len(bytes_temp) == 1:
        # technically 0x01 == true and 0x00 == false, but many languages will
        # cast anything other than 0 to True and only 0 to False
        ch_temp = bytes_temp[0]
        return ch_temp != 0
    else:
        raise ReadError


cpdef inline skip_boolean(fo):
    """A boolean is written as a single byte whose value is either 0 (false) or
    1 (true).
    """
    fo.read(1)


cpdef long64 read_long(fo) except? -1:
    """int and long values are written using variable-length, zig-zag
    coding."""
    cdef ulong64 b
    cdef ulong64 n
    cdef int32 shift
    cdef bytes c = fo.read(1)

    # We do EOF checking only here, since most reader start here
    if not c:
        raise EOFError

    b = <unsigned char>(c[0])
    n = b & 0x7F
    shift = 7

    while (b & 0x80) != 0:
        c = fo.read(1)
        b = <unsigned char>(c[0])
        n |= (b & 0x7F) << shift
        shift += 7

    return (n >> 1) ^ -(n & 1)


cpdef skip_long(fo):
    """int and long values are written using variable-length, zig-zag
    coding."""
    cdef ulong64 b
    cdef bytes c = fo.read(1)

    b = <unsigned char>(c[0])

    while (b & 0x80) != 0:
        c = fo.read(1)
        b = <unsigned char>(c[0])


cpdef skip_int(fo):
    skip_long(fo)


cdef union float_uint32:
    float f
    uint32 n


cpdef read_float(fo):
    """A float is written as 4 bytes.

    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    cdef bytes data
    cdef unsigned char ch_data[4]
    cdef float_uint32 fi
    data = fo.read(4)
    if len(data) == 4:
        ch_data[:4] = data
        fi.n = (ch_data[0]
                | (ch_data[1] << 8)
                | (ch_data[2] << 16)
                | (ch_data[3] << 24))
        return fi.f
    else:
        raise ReadError


cpdef skip_float(fo):
    """A float is written as 4 bytes.

    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    fo.read(4)


cdef union double_ulong64:
    double d
    ulong64 n


cpdef read_double(fo):
    """A double is written as 8 bytes.

    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    cdef bytes data
    cdef unsigned char ch_data[8]
    cdef double_ulong64 dl
    data = fo.read(8)
    if len(data) == 8:
        ch_data[:8] = data
        dl.n = (ch_data[0]
                | (<ulong64>(ch_data[1]) << 8)
                | (<ulong64>(ch_data[2]) << 16)
                | (<ulong64>(ch_data[3]) << 24)
                | (<ulong64>(ch_data[4]) << 32)
                | (<ulong64>(ch_data[5]) << 40)
                | (<ulong64>(ch_data[6]) << 48)
                | (<ulong64>(ch_data[7]) << 56))
        return dl.d
    else:
        raise ReadError


cpdef skip_double(fo):
    """A double is written as 8 bytes.

    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    fo.read(8)


cpdef read_bytes(fo):
    """Bytes are encoded as a long followed by that many bytes of data."""
    cdef long64 size = read_long(fo)
    out =  fo.read(<long>size)
    if len(out) != size:
        raise EOFError(f"Expected {size} bytes, read {len(out)}")
    return out


cpdef skip_bytes(fo):
    """Bytes are encoded as a long followed by that many bytes of data."""
    cdef long64 size = read_long(fo)
    fo.read(<long>size)


cpdef unicode read_utf8(fo, handle_unicode_errors="strict"):
    """A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data.
    """
    return read_bytes(fo).decode(errors=handle_unicode_errors)


cpdef skip_utf8(fo):
    """A string is encoded as a long followed by that many bytes of UTF-8
    encoded character data.
    """
    skip_bytes(fo)


cpdef read_fixed(fo, writer_schema):
    """Fixed instances are encoded using the number of bytes declared in the
    schema."""
    out = fo.read(writer_schema["size"])
    if len(out) != writer_schema["size"]:
        raise EOFError(f"Expected {writer_schema['size']} bytes, read {len(out)}")
    return out


cpdef skip_fixed(fo, writer_schema):
    """Fixed instances are encoded using the number of bytes declared in the
    schema."""
    fo.read(writer_schema["size"])


cpdef read_enum(fo, writer_schema, reader_schema):
    """An enum is encoded by a int, representing the zero-based position of the
    symbol in the schema.
    """
    index = read_long(fo)
    symbol = writer_schema["symbols"][index]
    if reader_schema and symbol not in reader_schema["symbols"]:
        default = reader_schema.get("default")
        if default:
            return default
        else:
            symlist = reader_schema["symbols"]
            msg = f"{symbol} not found in reader symbol list {reader_schema['name']}, known symbols: {symlist}"
            raise SchemaResolutionError(msg)
    return symbol


cpdef skip_enum(fo):
    """An enum is encoded by a int, representing the zero-based position of the
    symbol in the schema.
    """
    read_long(fo)


cpdef read_array(
    fo,
    writer_schema,
    named_schemas,
    reader_schema=None,
    options={},
):
    """Arrays are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many array
    items.  A block with count zero indicates the end of the array.  Each item
    is encoded per the array's item schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.
    """
    cdef list read_items
    cdef long64 block_count
    cdef long64 i

    read_items = []

    block_count = read_long(fo)

    while block_count != 0:
        if block_count < 0:
            block_count = -block_count
            # Read block size, unused
            read_long(fo)

        if reader_schema:
            for i in range(block_count):
                read_items.append(_read_data(
                    fo,
                    writer_schema["items"],
                    named_schemas,
                    reader_schema["items"],
                    options,
                ))
        else:
            for i in range(block_count):
                read_items.append(_read_data(
                    fo,
                    writer_schema["items"],
                    named_schemas,
                    None,
                    options,
                ))
        block_count = read_long(fo)

    return read_items


cpdef skip_array(fo, writer_schema, named_schemas):
    """Arrays are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many array
    items.  A block with count zero indicates the end of the array.  Each item
    is encoded per the array's item schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.
    """
    cdef long64 block_count
    cdef long64 i

    block_count = read_long(fo)

    while block_count != 0:
        if block_count < 0:
            block_size = read_long(fo)
            fo.read(block_size)
        else:
            for i in range(block_count):
                _skip_data(fo, writer_schema["items"], named_schemas)

        block_count = read_long(fo)


cpdef read_map(
    fo,
    writer_schema,
    named_schemas,
    reader_schema=None,
    options={},
):
    """Maps are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many key/value
    pairs.  A block with count zero indicates the end of the map.  Each item is
    encoded per the map's value schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.
    """
    cdef dict read_items
    cdef long64 block_count
    cdef long64 i
    cdef unicode key

    read_items = {}
    block_count = read_long(fo)
    while block_count != 0:
        if block_count < 0:
            block_count = -block_count
            # Read block size, unused
            read_long(fo)

        if reader_schema:
            for i in range(block_count):
                key = read_utf8(fo, options.get("handle_unicode_errors", "strict"))
                read_items[key] = _read_data(
                    fo,
                    writer_schema["values"],
                    named_schemas,
                    reader_schema["values"],
                    options,
                )
        else:
            for i in range(block_count):
                key = read_utf8(fo, options.get("handle_unicode_errors", "strict"))
                read_items[key] = _read_data(
                    fo,
                    writer_schema["values"],
                    named_schemas,
                    None,
                    options,
                )
        block_count = read_long(fo)

    return read_items


cpdef skip_map(fo, writer_schema, named_schemas):
    """Maps are encoded as a series of blocks.

    Each block consists of a long count value, followed by that many key/value
    pairs.  A block with count zero indicates the end of the map.  Each item is
    encoded per the map's value schema.

    If a block's count is negative, then the count is followed immediately by a
    long block size, indicating the number of bytes in the block.  The actual
    count in this case is the absolute value of the count written.
    """
    cdef long64 block_count
    cdef long64 i

    block_count = read_long(fo)
    while block_count != 0:
        if block_count < 0:
            block_size = read_long(fo)
            fo.read(block_size)
        else:
            for i in range(block_count):
                skip_utf8(fo)
                _skip_data(fo, writer_schema["values"], named_schemas)

        block_count = read_long(fo)


cpdef read_union(
    fo,
    writer_schema,
    named_schemas,
    reader_schema=None,
    options={}
):
    """A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value.

    The value is then encoded per the indicated schema within the union.
    """
    # schema resolution
    index = read_long(fo)
    idx_schema = writer_schema[index]
    idx_reader_schema = None

    if reader_schema:
        # Handle case where the reader schema is just a single type (not union)
        if not isinstance(reader_schema, list):
            if match_types(idx_schema, reader_schema, named_schemas):
                result = _read_data(
                    fo,
                    idx_schema,
                    named_schemas,
                    reader_schema,
                    options,
                )
            else:
                raise SchemaResolutionError(
                    f"schema mismatch: {writer_schema} not found in {reader_schema}"
                )
        else:
            for schema in reader_schema:
                if match_types(idx_schema, schema, named_schemas):
                    idx_reader_schema = schema
                    result = _read_data(
                        fo,
                        idx_schema,
                        named_schemas,
                        schema,
                        options,
                    )
                    break
            else:
                raise SchemaResolutionError(
                    f"schema mismatch: {writer_schema} not found in {reader_schema}"
                )
    else:
        result = _read_data(fo, idx_schema, named_schemas, None, options)

    return_record_name_override = options.get("return_record_name_override")
    return_record_name = options.get("return_record_name")
    return_named_type_override = options.get("return_named_type_override")
    return_named_type = options.get("return_named_type")
    if return_named_type_override and is_single_name_union(writer_schema):
        return result
    elif return_named_type and extract_record_type(idx_schema) in NAMED_TYPES:
        schema_name = (
            idx_reader_schema["name"] if idx_reader_schema else idx_schema["name"]
        )
        return (schema_name, result)
    elif return_named_type and extract_record_type(idx_schema) not in AVRO_TYPES:
        # idx_schema is a named type
        schema_name = (
            named_schemas["reader"][idx_reader_schema]["name"]
            if idx_reader_schema
            else named_schemas["writer"][idx_schema]["name"]
        )
        return (schema_name, result)
    elif return_record_name_override and is_single_record_union(writer_schema):
        return result
    elif return_record_name and extract_record_type(idx_schema) == "record":
        schema_name = (
            idx_reader_schema["name"] if idx_reader_schema else idx_schema["name"]
        )
        return (schema_name, result)
    elif return_record_name and extract_record_type(idx_schema) not in AVRO_TYPES:
        # idx_schema is a named type
        schema_name = (
            named_schemas["reader"][idx_reader_schema]["name"]
            if idx_reader_schema
            else named_schemas["writer"][idx_schema]["name"]
        )
        return (schema_name, result)
    else:
        return result


cpdef skip_union(fo, writer_schema, named_schemas):
    """A union is encoded by first writing a long value indicating the
    zero-based position within the union of the schema of its value.

    The value is then encoded per the indicated schema within the union.
    """
    # schema resolution
    index = read_long(fo)
    _skip_data(fo, writer_schema[index], named_schemas)


cpdef read_record(
    fo,
    writer_schema,
    named_schemas,
    reader_schema=None,
    options={},
):
    """A record is encoded by encoding the values of its fields in the order
    that they are declared. In other words, a record is encoded as just the
    concatenation of the encodings of its fields.  Field values are encoded per
    their schema.

    Schema Resolution:
     * the ordering of fields may be different: fields are matched by name.
     * schemas for fields with the same name in both records are resolved
         recursively.
     * if the writer's record contains a field with a name not present in the
         reader's record, the writer's value for that field is ignored.
     * if the reader's record schema has a field that contains a default value,
         and writer's schema does not have a field with the same name, then the
         reader should use the default value from its field.
     * if the reader's record schema has a field with no default value, and
         writer's schema does not have a field with the same name, then the
         field's value is unset.
    """
    record = {}
    if reader_schema is None:
        for field in writer_schema["fields"]:
            record[field["name"]] = _read_data(
                fo,
                field["type"],
                named_schemas,
                None,
                options,
            )
    else:
        readers_field_dict = {}
        aliases_field_dict = {}
        for f in reader_schema["fields"]:
            readers_field_dict[f["name"]] = f
            for alias in f.get("aliases", []):
                aliases_field_dict[alias] = f

        for field in writer_schema["fields"]:
            readers_field = readers_field_dict.get(
                field["name"],
                aliases_field_dict.get(field["name"]),
            )
            if readers_field:
                readers_field_name = readers_field["name"]
                record[readers_field_name] = _read_data(
                    fo,
                    field["type"],
                    named_schemas,
                    readers_field["type"],
                    options,
                )
                del readers_field_dict[readers_field_name]
            else:
                _skip_data(fo, field["type"], named_schemas)

        # fill in default values
        for f_name, field in readers_field_dict.items():
            if "default" in field:
                record[field["name"]] = field["default"]
            else:
                msg = f"No default value for field {field['name']} in {reader_schema['name']}"
                raise SchemaResolutionError(msg)

    return record


cpdef skip_record(fo, writer_schema, named_schemas):
    for field in writer_schema["fields"]:
        _skip_data(fo, field["type"], named_schemas)


cpdef maybe_promote(data, writer_type, reader_type):
    if writer_type == "int":
        # No need to promote to long since they are the same type in Python
        if reader_type == "float" or reader_type == "double":
            return float(data)
    if writer_type == "long":
        if reader_type == "float" or reader_type == "double":
            return float(data)
    if writer_type == "string" and reader_type == "bytes":
        return data.encode()
    if writer_type == "bytes" and reader_type == "string":
        return data.decode()
    return data


cpdef _read_data(
    fo,
    writer_schema,
    named_schemas,
    reader_schema=None,
    options={},
):
    """Read data from file object according to schema."""

    record_type = extract_record_type(writer_schema)

    if reader_schema:
        reader_schema = match_schemas(
            writer_schema,
            reader_schema,
            named_schemas,
        )

    try:
        if record_type == "null":
            data = read_null(fo)
        elif record_type == "string":
            data = read_utf8(fo, options.get("handle_unicode_errors", "strict"))
        elif record_type == "int" or record_type == "long":
            data = read_long(fo)
        elif record_type == "float":
            data = read_float(fo)
        elif record_type == "double":
            data = read_double(fo)
        elif record_type == "boolean":
            data = read_boolean(fo)
        elif record_type == "bytes":
            data = read_bytes(fo)
        elif record_type == "fixed":
            data = read_fixed(fo, writer_schema)
        elif record_type == "enum":
            data = read_enum(fo, writer_schema, reader_schema)
        elif record_type == "array":
            data = read_array(
                fo,
                writer_schema,
                named_schemas,
                reader_schema,
                options,
            )
        elif record_type == "map":
            data = read_map(
                fo,
                writer_schema,
                named_schemas,
                reader_schema,
                options,
            )
        elif record_type == "union" or record_type == "error_union":
            data = read_union(
                fo,
                writer_schema,
                named_schemas,
                reader_schema,
                options,
            )
        elif record_type == "record" or record_type == "error":
            data = read_record(
                fo,
                writer_schema,
                named_schemas,
                reader_schema,
                options,
            )
        else:
            return _read_data(
                fo,
                named_schemas["writer"][record_type],
                named_schemas,
                named_schemas["reader"].get(reader_schema),
                options,
            )
    except ReadError:
        raise EOFError(f"cannot read {record_type} from {fo}")

    if "logicalType" in writer_schema:
        logical_type = extract_logical_type(writer_schema)
        fn = LOGICAL_READERS.get(logical_type)
        if fn:
            return fn(data, writer_schema, reader_schema)

    if reader_schema is not None:
        return maybe_promote(
            data,
            record_type,
            extract_record_type(reader_schema)
        )
    else:
        return data


cpdef _skip_data(
    fo,
    writer_schema,
    named_schemas,
):
    record_type = extract_record_type(writer_schema)

    if record_type == "null":
        skip_null(fo)
    elif record_type == "string":
        skip_utf8(fo)
    elif record_type == "int" or record_type == "long":
        skip_long(fo)
    elif record_type == "float":
        skip_float(fo)
    elif record_type == "double":
        skip_double(fo)
    elif record_type == "boolean":
        skip_boolean(fo)
    elif record_type == "bytes":
        skip_bytes(fo)
    elif record_type == "fixed":
        skip_fixed(fo, writer_schema)
    elif record_type == "enum":
        skip_enum(fo)
    elif record_type == "array":
        skip_array(fo, writer_schema, named_schemas)
    elif record_type == "map":
        skip_map(fo, writer_schema, named_schemas)
    elif record_type == "union" or record_type == "error_union":
        skip_union(fo, writer_schema, named_schemas)
    elif record_type == "record" or record_type == "error":
        skip_record(fo, writer_schema, named_schemas)
    else:
        _skip_data(fo, named_schemas["writer"][record_type], named_schemas)


cpdef skip_sync(fo, sync_marker):
    """Skip an expected sync marker, complaining if it doesn't match"""
    if fo.read(SYNC_SIZE) != sync_marker:
        raise ValueError("expected sync marker not found")


cpdef null_read_block(fo):
    """Read block in "null" codec."""
    return BytesIO(read_bytes(fo))


cpdef deflate_read_block(fo):
    """Read block in "deflate" codec."""
    data = read_bytes(fo)
    # -15 is the log of the window size; negative indicates "raw" (no
    # zlib headers) decompression.  See zlib.h.
    return BytesIO(zlib.decompressobj(-15).decompress(data))


cpdef bzip2_read_block(fo):
    """Read block in "bzip2" codec."""
    data = read_bytes(fo)
    return BytesIO(bz2.decompress(data))


cpdef xz_read_block(fo):
    length = read_long(fo)
    data = fo.read(length)
    return BytesIO(lzma.decompress(data))


BLOCK_READERS = {
    "null": null_read_block,
    "deflate": deflate_read_block,
    "bzip2": bzip2_read_block,
    "xz": xz_read_block,
}


cpdef snappy_read_block(fo):
    length = read_long(fo)
    data = fo.read(length - 4)
    fo.read(4)  # CRC
    return BytesIO(snappy_decompress(data))


try:
    from cramjam import snappy
    snappy_decompress = snappy.decompress_raw
except ImportError:
    try:
        import snappy
        snappy_decompress = snappy.decompress
        warn(
            "Snappy compression will use `cramjam` in the future. Please make sure you have `cramjam` installed",
            DeprecationWarning,
        )
    except ImportError:
        BLOCK_READERS["snappy"] = missing_codec_lib("snappy", "cramjam")
    else:
        BLOCK_READERS["snappy"] = snappy_read_block
else:
    BLOCK_READERS["snappy"] = snappy_read_block


cpdef zstandard_read_block(fo):
    length = read_long(fo)
    data = fo.read(length)
    return BytesIO(zstd.decompress(data))


try:
    if sys.version_info >= (3, 14):
        from compression import zstd
    else:
        from backports import zstd
except ImportError:
    BLOCK_READERS["zstandard"] = missing_codec_lib("zstandard", "backports.zstd")
else:
    BLOCK_READERS["zstandard"] = zstandard_read_block


cpdef lz4_read_block(fo):
    length = read_long(fo)
    data = fo.read(length)
    return BytesIO(lz4.block.decompress(data))


try:
    import lz4.block
except ImportError:
    BLOCK_READERS["lz4"] = missing_codec_lib("lz4", "lz4")
else:
    BLOCK_READERS["lz4"] = lz4_read_block


# ---------------------------------------------------------------------------
# Fast decoding path
#
# The generic readers above take a file-like object and dispatch on the schema
# for every datum.  The path below is used for container files (whose blocks
# are fully in memory once decompressed) and for schemaless_reader on a
# BytesIO: the schema is compiled once into a tree of ReadPlan nodes (all
# schema resolution, option handling and logical-type lookup happens there),
# and the data is decoded straight from a C pointer with a C switch per
# datum.  Behaviour, including which errors are raised and when, mirrors
# _read_data.
# ---------------------------------------------------------------------------


cdef enum:
    K_NULL = 0
    K_BOOLEAN = 1
    K_INT = 2
    K_LONG = 3
    K_FLOAT = 4
    K_DOUBLE = 5
    K_BYTES = 6
    K_STRING = 7
    K_FIXED = 8
    K_ENUM = 9
    K_ARRAY = 10
    K_MAP = 11
    K_UNION = 12
    K_RECORD = 13
    K_ERROR = 14

cdef enum:
    P_NONE = 0
    P_FLOAT = 1
    P_ENCODE = 2
    P_DECODE = 3


# Cursor and ReadPlan hold C pointers, which Cython's generated pickling
# would dereference: they are never pickled (see Block and MessageReader).
@cython.auto_pickle(False)
cdef class Cursor:
    """Read position inside an in-memory buffer.  ``keepalive`` holds the
    object that owns the memory."""
    cdef const unsigned char* buf
    cdef Py_ssize_t pos
    cdef Py_ssize_t end
    cdef object keepalive


@cython.auto_pickle(False)
cdef class ReadPlan:
    """One node of a compiled (writer schema, reader schema, options) triple."""
    cdef int kind
    cdef int promote
    cdef Py_ssize_t size                 # fixed
    cdef list enum_symbols               # enum: writer symbols
    cdef list enum_resolved              # enum with reader: per index symbol/default, None = error
    cdef ReadPlan child                  # array items / map values
    cdef list branches                   # union: ReadPlan per writer branch
    cdef list branch_names               # union: name to wrap with, or None
    cdef list field_names                # record: str, or None to skip the field
    cdef list field_plans                # record: ReadPlan per writer field
    cdef list default_names              # record: reader-only fields with defaults
    cdef list default_values
    cdef object missing_default_error    # record: message to raise, or None
    cdef object logical_fn
    cdef object writer_schema
    cdef object reader_schema
    cdef object errors_obj               # handle_unicode_errors
    cdef const char* errors              # NULL means "strict"
    cdef object error_message            # K_ERROR
    cdef object error_exception          # K_ERROR: a compile error, raised again



cdef object _ref_key(object schema):
    return id(schema) if schema is not None else 0


cdef ReadPlan _error_plan(message):
    cdef ReadPlan p = ReadPlan()
    p.kind = K_ERROR
    p.error_message = message
    return p


cdef ReadPlan _union_mismatch_plan(writer_schema, reader_schema):
    """Shared by every writer branch of a union that the reader cannot read.
    The message repeats both schemas, so it is built only when raised."""
    cdef ReadPlan p = ReadPlan()
    p.kind = K_ERROR
    p.writer_schema = writer_schema
    p.reader_schema = reader_schema
    return p


cdef ReadPlan _deferred_error_plan(exception):
    cdef ReadPlan p = ReadPlan()
    p.kind = K_ERROR
    p.error_exception = exception
    return p


cdef object _plan_error(ReadPlan p):
    """The exception an error node raises: a fresh copy of a deferred compile
    error, or a SchemaResolutionError."""
    if p.error_exception is not None:
        e = p.error_exception
        try:
            return type(e)(*e.args)
        except Exception:
            return e
    if p.error_message is not None:
        return SchemaResolutionError(p.error_message)
    return SchemaResolutionError(
        f"schema mismatch: {p.writer_schema} not found in {p.reader_schema}"
    )


cdef str _branch_name(idx_schema, idx_reader_schema, named_schemas, bint use_reader_lookup):
    if idx_reader_schema is not None:
        if isinstance(idx_reader_schema, dict):
            return idx_reader_schema["name"]
        return named_schemas["reader"][idx_reader_schema]["name"]
    if use_reader_lookup:
        return named_schemas["writer"][idx_schema]["name"]
    return idx_schema["name"]


cdef ReadPlan _build_plan(
    object writer_schema,
    object reader_schema,
    dict named_schemas,
    dict options,
    dict memo,
):
    """Compile one schema node. If that fails, the node raises the same error
    when a datum reaches it, which is when the generic reader raises it: a
    union branch that no datum takes never fails."""
    try:
        return _compile_node(writer_schema, reader_schema, named_schemas, options, memo)
    except Exception as e:
        return _deferred_error_plan(e)


cdef ReadPlan _compile_node(
    object writer_schema,
    object reader_schema,
    dict named_schemas,
    dict options,
    dict memo,
):
    cdef ReadPlan plan
    cdef ReadPlan sub
    cdef ReadPlan union_error
    cdef list branches, branch_names
    cdef list field_names, field_plans
    cdef list default_names, default_values

    record_type = extract_record_type(writer_schema)

    if reader_schema:
        try:
            reader_schema = match_schemas(writer_schema, reader_schema, named_schemas)
        except SchemaResolutionError as e:
            return _error_plan(str(e))

    if record_type in ("record", "error"):
        key = (id(writer_schema), _ref_key(reader_schema))
        plan = memo.get(key)
        if plan is not None:
            return plan
        plan = ReadPlan()
        memo[key] = plan
        plan.kind = K_RECORD
        try:
            field_names = []
            field_plans = []
            if reader_schema is None:
                for field in writer_schema["fields"]:
                    field_names.append(field["name"])
                    field_plans.append(_build_plan(field["type"], None, named_schemas, options, memo))
            else:
                readers_field_dict = {}
                aliases_field_dict = {}
                for f in reader_schema["fields"]:
                    readers_field_dict[f["name"]] = f
                    for alias in f.get("aliases", []):
                        aliases_field_dict[alias] = f
                for field in writer_schema["fields"]:
                    readers_field = readers_field_dict.get(
                        field["name"], aliases_field_dict.get(field["name"])
                    )
                    if readers_field:
                        field_names.append(readers_field["name"])
                        field_plans.append(_build_plan(
                            field["type"], readers_field["type"], named_schemas, options, memo
                        ))
                        del readers_field_dict[readers_field["name"]]
                    else:
                        field_names.append(None)
                        field_plans.append(_build_plan(field["type"], None, named_schemas, options, memo))
                default_names = []
                default_values = []
                for f_name, field in readers_field_dict.items():
                    if "default" in field:
                        default_names.append(field["name"])
                        default_values.append(field["default"])
                    else:
                        plan.missing_default_error = (
                            f"No default value for field {field['name']} in {reader_schema['name']}"
                        )
                        break
                if default_names:
                    plan.default_names = default_names
                    plan.default_values = default_values
            plan.field_names = field_names
            plan.field_plans = field_plans
        except Exception as e:
            # Recursive references compiled meanwhile already point at this
            # plan: turn it into the error node itself.
            plan.kind = K_ERROR
            plan.error_exception = e
            return plan

    elif record_type == "null":
        plan = ReadPlan()
        plan.kind = K_NULL
    elif record_type == "string":
        plan = ReadPlan()
        plan.kind = K_STRING
    elif record_type == "int":
        plan = ReadPlan()
        plan.kind = K_INT
    elif record_type == "long":
        plan = ReadPlan()
        plan.kind = K_LONG
    elif record_type == "float":
        plan = ReadPlan()
        plan.kind = K_FLOAT
    elif record_type == "double":
        plan = ReadPlan()
        plan.kind = K_DOUBLE
    elif record_type == "boolean":
        plan = ReadPlan()
        plan.kind = K_BOOLEAN
    elif record_type == "bytes":
        plan = ReadPlan()
        plan.kind = K_BYTES
    elif record_type == "fixed":
        plan = ReadPlan()
        plan.kind = K_FIXED
        plan.size = writer_schema["size"]
    elif record_type == "enum":
        plan = ReadPlan()
        plan.kind = K_ENUM
        plan.enum_symbols = list(writer_schema["symbols"])
        if reader_schema:
            resolved = []
            reader_symbol_set = set(reader_schema["symbols"])
            default = reader_schema.get("default")
            for symbol in plan.enum_symbols:
                if symbol in reader_symbol_set:
                    resolved.append(symbol)
                elif default:
                    resolved.append(default)
                else:
                    resolved.append(None)  # error, message built when raised
            plan.enum_resolved = resolved
    elif record_type == "array":
        plan = ReadPlan()
        plan.kind = K_ARRAY
        plan.child = _build_plan(
            writer_schema["items"],
            reader_schema["items"] if reader_schema else None,
            named_schemas, options, memo,
        )
    elif record_type == "map":
        plan = ReadPlan()
        plan.kind = K_MAP
        plan.child = _build_plan(
            writer_schema["values"],
            reader_schema["values"] if reader_schema else None,
            named_schemas, options, memo,
        )
    elif record_type in ("union", "error_union"):
        plan = ReadPlan()
        plan.kind = K_UNION
        branches = []
        branch_names = []
        rnn_override = options.get("return_record_name_override")
        rnn = options.get("return_record_name")
        rnt_override = options.get("return_named_type_override")
        rnt = options.get("return_named_type")
        single_name = is_single_name_union(writer_schema) if rnt_override else False
        single_record = is_single_record_union(writer_schema) if rnn_override else False
        union_error = None
        for idx_schema in writer_schema:
            idx_reader_schema = None
            if reader_schema:
                if not isinstance(reader_schema, list):
                    if match_types(idx_schema, reader_schema, named_schemas):
                        sub = _build_plan(idx_schema, reader_schema, named_schemas, options, memo)
                    else:
                        if union_error is None:
                            union_error = _union_mismatch_plan(writer_schema, reader_schema)
                        sub = union_error
                else:
                    for schema in reader_schema:
                        if match_types(idx_schema, schema, named_schemas):
                            idx_reader_schema = schema
                            sub = _build_plan(idx_schema, schema, named_schemas, options, memo)
                            break
                    else:
                        if union_error is None:
                            union_error = _union_mismatch_plan(writer_schema, reader_schema)
                        sub = union_error
            else:
                sub = _build_plan(idx_schema, None, named_schemas, options, memo)
            branches.append(sub)

            et = extract_record_type(idx_schema)
            name = None
            if rnt_override and single_name:
                name = None
            elif rnt and et in NAMED_TYPES:
                name = _branch_name(idx_schema, idx_reader_schema, named_schemas, False)
            elif rnt and et not in AVRO_TYPES:
                name = _branch_name(idx_schema, idx_reader_schema, named_schemas, True)
            elif rnn_override and single_record:
                name = None
            elif rnn and et == "record":
                name = _branch_name(idx_schema, idx_reader_schema, named_schemas, False)
            elif rnn and et not in AVRO_TYPES:
                name = _branch_name(idx_schema, idx_reader_schema, named_schemas, True)
            branch_names.append(name)
        plan.branches = branches
        plan.branch_names = branch_names
    else:
        # named type reference
        if reader_schema is not None and isinstance(reader_schema, dict):
            resolved_reader = reader_schema
        else:
            resolved_reader = named_schemas["reader"].get(reader_schema)
        return _build_plan(
            named_schemas["writer"][record_type],
            resolved_reader,
            named_schemas, options, memo,
        )

    plan.writer_schema = writer_schema
    plan.reader_schema = reader_schema
    errors_obj = options.get("handle_unicode_errors", "strict")
    if errors_obj is None or errors_obj == "strict":
        plan.errors = NULL
    else:
        plan.errors_obj = errors_obj.encode() if isinstance(errors_obj, str) else bytes(errors_obj)
        plan.errors = plan.errors_obj

    if isinstance(writer_schema, dict) and "logicalType" in writer_schema:
        fn = LOGICAL_READERS.get(extract_logical_type(writer_schema))
        if fn:
            plan.logical_fn = fn

    if reader_schema is not None:
        reader_type = extract_record_type(reader_schema)
        if record_type in ("int", "long") and reader_type in ("float", "double"):
            plan.promote = P_FLOAT
        elif record_type == "string" and reader_type == "bytes":
            plan.promote = P_ENCODE
        elif record_type == "bytes" and reader_type == "string":
            plan.promote = P_DECODE

    return plan


cpdef ReadPlan compile_read_plan(writer_schema, named_schemas, reader_schema, options):
    """Compile a (writer schema, reader schema, options) triple into a ReadPlan."""
    return _build_plan(writer_schema, reader_schema, named_schemas, dict(options), {})


cdef inline int _need(Cursor c, Py_ssize_t n) except -1:
    # A negative n comes from a corrupted length varint; the generic reader
    # reports that as EOFError as well.
    if n < 0 or c.end - c.pos < n:
        raise EOFError(f"Expected {n} bytes, read {c.end - c.pos}")
    return 0


cdef inline long64 _c_read_long(Cursor c) except? -1:
    cdef ulong64 b
    cdef ulong64 n
    cdef int32 shift
    cdef const unsigned char* buf = c.buf
    cdef Py_ssize_t pos = c.pos
    cdef Py_ssize_t end = c.end

    if pos >= end:
        raise EOFError
    b = buf[pos]
    pos += 1
    n = b & 0x7F
    shift = 7
    while (b & 0x80) != 0:
        if pos >= end:
            raise EOFError
        if shift > 63:
            # more than 10 continuation bytes cannot encode a 64-bit value
            raise ValueError("invalid varint: more than 10 bytes")
        b = buf[pos]
        pos += 1
        n |= (b & 0x7F) << shift
        shift += 7
    c.pos = pos
    return (n >> 1) ^ -(n & 1)


cdef inline unicode _c_read_utf8(Cursor c, const char* errors):
    cdef long64 size = _c_read_long(c)
    cdef Py_ssize_t start
    _need(c, size)
    # Past the bytes before decoding them, as the generic reader reads them
    # first: after a UnicodeDecodeError the stream is past the string too.
    start = c.pos
    c.pos += size
    return PyUnicode_DecodeUTF8(<const char*>(c.buf + start), size, errors)


cdef object _exec_plan(Cursor c, ReadPlan p):
    cdef int kind = p.kind
    cdef long64 n, i, block_count
    cdef Py_ssize_t nfields, idx
    cdef ReadPlan sub
    cdef dict record
    cdef list items
    cdef dict mapping
    cdef unsigned char ch_data[8]
    cdef float_uint32 fi
    cdef double_ulong64 dl
    cdef object data
    cdef object name

    if kind == K_STRING:
        data = _c_read_utf8(c, p.errors)
    elif kind == K_LONG or kind == K_INT:
        data = _c_read_long(c)
    elif kind == K_RECORD:
        record = {}
        nfields = PyList_GET_SIZE(p.field_plans)
        for idx in range(nfields):
            sub = <ReadPlan>PyList_GET_ITEM(p.field_plans, idx)
            name = <object>PyList_GET_ITEM(p.field_names, idx)
            if name is None:
                _skip_plan(c, sub)
            else:
                record[name] = _exec_plan(c, sub)
        if p.default_names is not None:
            nfields = PyList_GET_SIZE(p.default_names)
            for idx in range(nfields):
                record[<object>PyList_GET_ITEM(p.default_names, idx)] = (
                    <object>PyList_GET_ITEM(p.default_values, idx)
                )
        if p.missing_default_error is not None:
            raise SchemaResolutionError(p.missing_default_error)
        data = record
    elif kind == K_DOUBLE:
        _need(c, 8)
        dl.n = (c.buf[c.pos]
                | (<ulong64>(c.buf[c.pos + 1]) << 8)
                | (<ulong64>(c.buf[c.pos + 2]) << 16)
                | (<ulong64>(c.buf[c.pos + 3]) << 24)
                | (<ulong64>(c.buf[c.pos + 4]) << 32)
                | (<ulong64>(c.buf[c.pos + 5]) << 40)
                | (<ulong64>(c.buf[c.pos + 6]) << 48)
                | (<ulong64>(c.buf[c.pos + 7]) << 56))
        c.pos += 8
        data = dl.d
    elif kind == K_UNION:
        n = _c_read_long(c)
        if n < 0 or n >= PyList_GET_SIZE(p.branches):
            raise IndexError("list index out of range")
        sub = <ReadPlan>PyList_GET_ITEM(p.branches, n)
        data = _exec_plan(c, sub)
        name = <object>PyList_GET_ITEM(p.branch_names, n)
        if name is not None:
            data = (name, data)
    elif kind == K_NULL:
        data = None
    elif kind == K_BOOLEAN:
        _need(c, 1)
        data = c.buf[c.pos] != 0
        c.pos += 1
    elif kind == K_ARRAY:
        items = []
        sub = p.child
        block_count = _c_read_long(c)
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                _c_read_long(c)  # block size, unused
            for i in range(block_count):
                items.append(_exec_plan(c, sub))
            block_count = _c_read_long(c)
        data = items
    elif kind == K_MAP:
        mapping = {}
        sub = p.child
        block_count = _c_read_long(c)
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                _c_read_long(c)  # block size, unused
            for i in range(block_count):
                name = _c_read_utf8(c, p.errors)
                mapping[name] = _exec_plan(c, sub)
            block_count = _c_read_long(c)
        data = mapping
    elif kind == K_BYTES:
        n = _c_read_long(c)
        _need(c, n)
        data = PyBytes_FromStringAndSize(<const char*>(c.buf + c.pos), n)
        c.pos += n
    elif kind == K_FLOAT:
        _need(c, 4)
        fi.n = (c.buf[c.pos]
                | (c.buf[c.pos + 1] << 8)
                | (c.buf[c.pos + 2] << 16)
                | (c.buf[c.pos + 3] << 24))
        c.pos += 4
        data = fi.f
    elif kind == K_FIXED:
        _need(c, p.size)
        data = PyBytes_FromStringAndSize(<const char*>(c.buf + c.pos), p.size)
        c.pos += p.size
    elif kind == K_ENUM:
        n = _c_read_long(c)
        if n < 0 or n >= PyList_GET_SIZE(p.enum_symbols):
            raise IndexError("list index out of range")
        if p.enum_resolved is None:
            data = <object>PyList_GET_ITEM(p.enum_symbols, n)
        else:
            data = <object>PyList_GET_ITEM(p.enum_resolved, n)
            if data is None:
                symbol = <object>PyList_GET_ITEM(p.enum_symbols, n)
                raise SchemaResolutionError(
                    f"{symbol} not found in reader symbol list "
                    f"{p.reader_schema['name']}, known symbols: {p.reader_schema['symbols']}"
                )
    else:  # K_ERROR
        raise _plan_error(p)

    if p.logical_fn is not None:
        return p.logical_fn(data, p.writer_schema, p.reader_schema)
    if p.promote != P_NONE:
        if p.promote == P_FLOAT:
            return float(data)
        elif p.promote == P_ENCODE:
            return data.encode()
        else:
            return data.decode()
    return data


cdef int _skip_plan(Cursor c, ReadPlan p) except -1:
    cdef int kind = p.kind
    cdef long64 n, i, block_count
    cdef Py_ssize_t nfields, idx

    if kind == K_STRING or kind == K_BYTES:
        n = _c_read_long(c)
        _need(c, n)
        c.pos += n
    elif kind == K_LONG or kind == K_INT or kind == K_ENUM:
        _c_read_long(c)
    elif kind == K_RECORD:
        nfields = PyList_GET_SIZE(p.field_plans)
        for idx in range(nfields):
            _skip_plan(c, <ReadPlan>PyList_GET_ITEM(p.field_plans, idx))
    elif kind == K_DOUBLE:
        _need(c, 8)
        c.pos += 8
    elif kind == K_FLOAT:
        _need(c, 4)
        c.pos += 4
    elif kind == K_BOOLEAN:
        _need(c, 1)
        c.pos += 1
    elif kind == K_FIXED:
        _need(c, p.size)
        c.pos += p.size
    elif kind == K_UNION:
        n = _c_read_long(c)
        if n < 0 or n >= PyList_GET_SIZE(p.branches):
            raise IndexError("list index out of range")
        _skip_plan(c, <ReadPlan>PyList_GET_ITEM(p.branches, n))
    elif kind == K_ARRAY or kind == K_MAP:
        block_count = _c_read_long(c)
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                n = _c_read_long(c)
                _need(c, n)
                c.pos += n
            else:
                for i in range(block_count):
                    if kind == K_MAP:
                        n = _c_read_long(c)
                        _need(c, n)
                        c.pos += n
                    _skip_plan(c, p.child)
            block_count = _c_read_long(c)
    elif kind == K_ERROR:
        raise _plan_error(p)
    return 0


cdef Cursor _cursor_for_bytes(bytes data):
    cdef Cursor c = Cursor()
    c.keepalive = data
    c.buf = <const unsigned char*>data
    c.pos = 0
    c.end = len(data)
    return c


def _decode_block_records(bytes block_bytes, long64 count, ReadPlan plan):
    """Generator decoding ``count`` records from ``block_bytes``."""
    cdef Cursor c = _cursor_for_bytes(block_bytes)
    cdef long64 i
    for i in range(count):
        yield _exec_plan(c, plan)


cdef object _read_one_from_bytesio(fo, ReadPlan plan):
    """Decode one datum from a BytesIO at its current position and advance
    the position past it (or past the bytes consumed before an error).

    getvalue() shares the buffer of an unmodified BytesIO; getbuffer() would
    export a writable view and force a copy of the whole buffer first."""
    cdef bytes data = fo.getvalue()
    cdef Cursor c = _cursor_for_bytes(data)
    c.pos = fo.tell()
    try:
        return _exec_plan(c, plan)
    finally:
        fo.seek(c.pos)


# (writer_schema, reader_schema, options) -> plan, keyed by object identity.
# Entries keep references to the schema objects so an id cannot be reused by
# a different object while it is cached.  Parsed schemas are assumed not to
# change once used (the documentation says so): their contents are not
# checked again.  When full, the oldest entry is evicted.  Each entry also
# records the LOGICAL_READERS function (or None) of every logical type in the
# plan, so replacing, removing or registering a reader invalidates the entry
# on its next use, as the generic reader would see the change immediately.
#
# _SCHEMALESS_ORDER holds the keys in insertion order, so evicting the oldest
# entry is O(1): next(iter(dict)) walks past every slot already deleted from
# the front of the dict.  Keys removed by an invalidation stay in it until
# they reach the front, and it is rebuilt from the dict when it grows past
# twice the dict's size.  Each entry stores the key object it was inserted
# with, and eviction only removes an entry whose stored key *is* the popped
# one: a leftover key can equal a newer key (ids are reused once a schema is
# freed) and must not evict it.  The dict and the deque are only used while
# holding _SCHEMALESS_LOCK: without a GIL, a dict lookup returns a borrowed
# reference that a concurrent delete could free.
cdef dict _SCHEMALESS_PLANS = {}
cdef object _SCHEMALESS_ORDER = deque()
cdef cython.pymutex _SCHEMALESS_LOCK
cdef Py_ssize_t _SCHEMALESS_PLANS_MAX = int(
    os.environ.get("FASTAVRO_SCHEMALESS_PLAN_CACHE", "3072")
)


# Nothing that can run Python code of its own may happen under
# _SCHEMALESS_LOCK: freeing an entry can call a finalizer (of a logical
# reader, say), and allocating a container object can start a garbage
# collection that calls others; either may use this cache and would wait for
# the lock forever. Removed entries are therefore collected in a list and
# released after the lock, and nothing is allocated under it beyond the dict
# and deque storage.


cdef _evict_to(Py_ssize_t size, list removed):
    """Evict the oldest entries until at most `size` remain (lock held)."""
    while len(_SCHEMALESS_PLANS) > size and _SCHEMALESS_ORDER:
        key = _SCHEMALESS_ORDER.popleft()
        entry = _SCHEMALESS_PLANS.get(key)
        if entry is not None and entry[4] is key:
            removed.append(entry)
            del _SCHEMALESS_PLANS[key]


cdef _remember(key):
    """Record a newly inserted key (lock held)."""
    cdef Py_ssize_t i
    _SCHEMALESS_ORDER.append(key)
    if len(_SCHEMALESS_ORDER) > 2 * len(_SCHEMALESS_PLANS) + 64:
        # drop the keys left by invalidations, in place and in order
        for i in range(len(_SCHEMALESS_ORDER)):
            k = _SCHEMALESS_ORDER.popleft()
            entry = _SCHEMALESS_PLANS.get(k)
            if entry is not None and entry[4] is k:
                _SCHEMALESS_ORDER.append(k)


def set_schemaless_plan_cache_size(size):
    """Set the number of compiled schemaless plans kept; returns the old size."""
    global _SCHEMALESS_PLANS_MAX
    cdef list removed = []
    size = max(0, int(size))
    with _SCHEMALESS_LOCK:
        previous = _SCHEMALESS_PLANS_MAX
        _SCHEMALESS_PLANS_MAX = size
        _evict_to(size, removed)
    return previous


def schemaless_plan_cache_info():
    return {"size": len(_SCHEMALESS_PLANS), "capacity": _SCHEMALESS_PLANS_MAX}


cdef dict _logical_readers_used(ReadPlan plan, dict out, set seen):
    """{logical type: function, or None if none was registered} for every
    logical type in the plan tree, for cache validation: replacing, removing
    or registering a reader invalidates the plan."""
    if plan is None or id(plan) in seen:
        return out
    seen.add(id(plan))
    if isinstance(plan.writer_schema, dict) and "logicalType" in plan.writer_schema:
        out[extract_logical_type(plan.writer_schema)] = plan.logical_fn
    _logical_readers_used(plan.child, out, seen)
    for sub in plan.branches or ():
        _logical_readers_used(<ReadPlan>sub, out, seen)
    for sub in plan.field_plans or ():
        _logical_readers_used(<ReadPlan>sub, out, seen)
    return out


cdef ReadPlan _schemaless_plan(writer_schema, reader_schema, dict named_schemas, dict options):
    cdef ReadPlan plan
    key = (
        id(writer_schema),
        _ref_key(reader_schema),
        options["return_record_name"],
        options["return_record_name_override"],
        options["handle_unicode_errors"],
        options["return_named_type"],
        options["return_named_type_override"],
    )
    with _SCHEMALESS_LOCK:
        entry = _SCHEMALESS_PLANS.get(key)
        if entry is not None and entry[0] is writer_schema and entry[1] is reader_schema:
            for logical_key, fn in <list>entry[3]:
                if LOGICAL_READERS.get(logical_key) is not fn:
                    break
            else:
                return <ReadPlan>entry[2]
            del _SCHEMALESS_PLANS[key]  # `entry` keeps it alive past the lock
    plan = _build_plan(writer_schema, reader_schema, named_schemas, options, {})
    if _SCHEMALESS_PLANS_MAX <= 0:
        return plan
    used = list(_logical_readers_used(plan, {}, set()).items())
    entry = (writer_schema, reader_schema, plan, used, key)
    removed = []
    with _SCHEMALESS_LOCK:
        _evict_to(_SCHEMALESS_PLANS_MAX - 1, removed)
        replaced = _SCHEMALESS_PLANS.get(key)  # inserted by another thread meanwhile
        if replaced is not None:
            removed.append(replaced)
        _SCHEMALESS_PLANS[key] = entry
        _remember(key)
    return plan


def _iter_avro_records(
    fo,
    header,
    codec,
    writer_schema,
    named_schemas,
    reader_schema,
    options,
    read_plan=True,
):
    cdef int32 i

    sync_marker = header["sync"]

    read_block = BLOCK_READERS.get(codec)
    if not read_block:
        raise ValueError(f"Unrecognized codec: {codec}")

    # Compiled lazily so that schema resolution errors surface on the first
    # record, as they always have, rather than when the reader is created.
    cdef ReadPlan plan = None
    if _READ_PLAN_ENABLED and read_plan:
        plan = compile_read_plan(writer_schema, named_schemas, reader_schema, options)

    block_count = 0
    while True:
        try:
            block_count = read_long(fo)
        except EOFError:
            return

        block_fo = read_block(fo)

        if plan is not None:
            # BytesIO.getvalue() does not copy while the buffer is unmodified
            yield from _decode_block_records(block_fo.getvalue(), block_count, plan)
        else:
            for i in range(block_count):
                yield _read_data(
                    block_fo,
                    writer_schema,
                    named_schemas,
                    reader_schema,
                    options,
                )

        skip_sync(fo, sync_marker)


def _iter_avro_blocks(
    fo,
    header,
    codec,
    writer_schema,
    named_schemas,
    reader_schema,
    options,
    read_plan=True,
):
    sync_marker = header["sync"]

    read_block = BLOCK_READERS.get(codec)
    if not read_block:
        raise ValueError(f"Unrecognized codec: {codec}")

    # Shared, lazily filled plan cell: compiled by the first Block that is
    # actually iterated, so iterating blocks without decoding costs nothing.
    plan_cell = [None]

    while True:
        offset = fo.tell()
        try:
            num_block_records = read_long(fo)
        except EOFError:
            return

        block_bytes = read_block(fo)

        skip_sync(fo, sync_marker)

        size = fo.tell() - offset

        yield Block(
            block_bytes, num_block_records, codec, reader_schema,
            writer_schema, named_schemas, offset, size, options, plan_cell,
            read_plan,
        )


class Block:
    def __init__(
        self,
        bytes_,
        num_records,
        codec,
        reader_schema,
        writer_schema,
        named_schemas,
        offset,
        size,
        options,
        plan_cell=None,
        read_plan=True,
    ):
        self.bytes_ = bytes_
        self.num_records = num_records
        self.codec = codec
        self.reader_schema = reader_schema
        self.writer_schema = writer_schema
        self._named_schemas = named_schemas
        self.offset = offset
        self.size = size
        self.options = options
        self._plan_cell = plan_cell if plan_cell is not None else [None]
        self._read_plan = read_plan

    def __iter__(self):
        if not (_READ_PLAN_ENABLED and self._read_plan):
            return self._iter_generic()
        plan = self._plan_cell[0]
        if plan is None:
            plan = self._plan_cell[0] = compile_read_plan(
                self.writer_schema, self._named_schemas, self.reader_schema, self.options
            )
        # Decodes from the start of the block bytes, so a Block can be
        # iterated more than once.
        return _decode_block_records(self.bytes_.getvalue(), self.num_records, plan)

    def __getstate__(self):
        # The shared plan cell may hold a compiled plan; an unpickled Block
        # compiles its own when it is first iterated.
        state = self.__dict__.copy()
        state["_plan_cell"] = [None]
        return state

    def _iter_generic(self):
        for i in range(self.num_records):
            yield _read_data(
                self.bytes_,
                self.writer_schema,
                self._named_schemas,
                self.reader_schema,
                self.options,
            )

    def __str__(self):
        return (
            f"Avro block: {len(self.bytes_)} bytes, {self.num_records} records, "
            + f"codec: {self.codec}, position {self.offset}+{self.size}"
        )


class file_reader:
    def __init__(self, fo, reader_schema=None, options={}):
        self.fo = fo
        self.options = options
        try:
            self._header = _read_data(self.fo, HEADER_SCHEMA, {}, None, self.options)
        except EOFError:
            raise ValueError("cannot read header - is it an avro file?")

        # `meta` values are bytes. So, the actual decoding has to be external.
        self.metadata = {
            k: v.decode() for k, v in self._header["meta"].items()
        }

        self._schema = json.loads(self.metadata["avro.schema"])
        self.codec = self.metadata.get("avro.codec", "null")

        self._named_schemas = _default_named_schemas()
        if reader_schema:
            self.reader_schema = parse_schema(
                reader_schema, self._named_schemas["reader"], _write_hint=False
            )
            # Older avro files created before we were more strict about
            # defaults might have been writen with a bad default. Since we re-parse
            # the writer schema here, it will now fail. Therefore, if a user
            # provides a reader schema that passes parsing, we will ignore those
            # default errors
            ignore_default_error = True
        else:
            self.reader_schema = None
            ignore_default_error = False

        self.writer_schema = parse_schema(
            self._schema,
            self._named_schemas["writer"],
            _write_hint=False,
            _force=True,
            _ignore_default_error=ignore_default_error,
        )

        self._elems = None

    @property
    def schema(self):
        import warnings
        warnings.warn(
            "The 'schema' attribute is deprecated. Please use 'writer_schema'",
            DeprecationWarning,
        )
        return self._schema

    def __iter__(self):
        if not self._elems:
            raise NotImplementedError
        return self._elems

    def __next__(self):
        return next(self._elems)


class reader(file_reader):
    def __init__(
        self,
        fo,
        reader_schema=None,
        return_record_name=False,
        return_record_name_override=False,
        handle_unicode_errors="strict",
        return_named_type=False,
        return_named_type_override=False,
        read_plan=True,
    ):
        options = {
            "return_record_name": return_record_name,
            "return_record_name_override": return_record_name_override,
            "handle_unicode_errors": handle_unicode_errors,
            "return_named_type": return_named_type,
            "return_named_type_override": return_named_type_override,
        }
        super().__init__(fo, reader_schema, options)

        self._elems = _iter_avro_records(self.fo,
                                         self._header,
                                         self.codec,
                                         self.writer_schema,
                                         self._named_schemas,
                                         self.reader_schema,
                                         self.options,
                                         read_plan)


class block_reader(file_reader):
    def __init__(
        self,
        fo,
        reader_schema=None,
        return_record_name=False,
        return_record_name_override=False,
        handle_unicode_errors="strict",
        return_named_type=False,
        return_named_type_override=False,
        read_plan=True,
    ):
        options = {
            "return_record_name": return_record_name,
            "return_record_name_override": return_record_name_override,
            "handle_unicode_errors": handle_unicode_errors,
            "return_named_type": return_named_type,
            "return_named_type_override": return_named_type_override,
        }
        super().__init__(fo, reader_schema, options)

        self._elems = _iter_avro_blocks(self.fo,
                                        self._header,
                                        self.codec,
                                        self.writer_schema,
                                        self._named_schemas,
                                        self.reader_schema,
                                        self.options,
                                        read_plan)


cpdef schemaless_reader(
    fo,
    writer_schema,
    reader_schema=None,
    return_record_name=False,
    return_record_name_override=False,
    handle_unicode_errors="strict",
    return_named_type=False,
    return_named_type_override=False,
    bint read_plan=True,
):
    if writer_schema == reader_schema:
        # No need for the reader schema if they are the same
        reader_schema = None

    named_schemas = _default_named_schemas()
    parsed_writer_schema = parse_schema(writer_schema, named_schemas["writer"])

    parsed_reader_schema = None
    if reader_schema:
        parsed_reader_schema = parse_schema(reader_schema, named_schemas["reader"])

    options = {
        "return_record_name": return_record_name,
        "return_record_name_override": return_record_name_override,
        "handle_unicode_errors": handle_unicode_errors,
        "return_named_type": return_named_type,
        "return_named_type_override": return_named_type_override,
    }

    # Fast path: the input is in memory and the schemas were already parsed
    # (so the compiled plan can be cached by identity across calls).  Any
    # other input keeps the generic reader, including BytesIO subclasses,
    # whose read() may differ from the buffer it is read from here.
    if (
        _READ_PLAN_ENABLED
        and read_plan
        and type(fo) is BytesIO
        and parsed_writer_schema is writer_schema
        and (parsed_reader_schema is None or parsed_reader_schema is reader_schema)
    ):
        return _read_one_from_bytesio(
            fo,
            _schemaless_plan(
                parsed_writer_schema, parsed_reader_schema, named_schemas, options
            ),
        )

    writer_schema = parsed_writer_schema
    reader_schema = parsed_reader_schema
    return _read_data(
        fo,
        writer_schema,
        named_schemas,
        reader_schema,
        options,
    )


cdef class MessageReader:
    """Decoder for schemaless messages, prepared once and reused.

    Does the same work as ``schemaless_reader`` but the schema handling,
    option handling and plan compilation happen once in the constructor, and
    ``read`` decodes straight from ``bytes``::

        reader = MessageReader(parsed_writer_schema)
        record = reader.read(payload)

    Behaviour is identical to ``schemaless_reader(BytesIO(payload), ...)``
    with the same arguments: same values, same key order, same exceptions,
    raised at the same point (schema resolution errors on the first ``read``
    that hits them, not in the constructor).  Trailing bytes after the datum
    are ignored, as ``schemaless_reader`` ignores them.  An instance is
    immutable after construction and safe to share between threads.

    ``LOGICAL_READERS`` functions are bound when the instance is built; build
    a new instance to pick up a replacement.  If the compiled read plans are
    disabled (``FASTAVRO_READ_PLAN=0`` or ``set_read_plan_enabled(False)``),
    or the instance was built with ``read_plan=False``, ``read`` uses the
    generic reader, which looks them up on every call.
    """
    cdef ReadPlan plan
    cdef readonly bint read_plan
    cdef readonly object writer_schema
    cdef readonly object reader_schema
    cdef readonly dict options
    cdef dict named_schemas

    def __init__(
        self,
        writer_schema,
        reader_schema=None,
        *,
        return_record_name=False,
        return_record_name_override=False,
        handle_unicode_errors="strict",
        return_named_type=False,
        return_named_type_override=False,
        read_plan=True,
    ):
        if writer_schema == reader_schema:
            # No need for the reader schema if they are the same
            reader_schema = None
        self.read_plan = read_plan
        self.named_schemas = _default_named_schemas()
        self.writer_schema = parse_schema(writer_schema, self.named_schemas["writer"])
        self.reader_schema = None
        if reader_schema:
            self.reader_schema = parse_schema(reader_schema, self.named_schemas["reader"])
        self.options = {
            "return_record_name": return_record_name,
            "return_record_name_override": return_record_name_override,
            "handle_unicode_errors": handle_unicode_errors,
            "return_named_type": return_named_type,
            "return_named_type_override": return_named_type_override,
        }
        self.plan = None
        if read_plan:
            self.plan = compile_read_plan(
                self.writer_schema, self.named_schemas, self.reader_schema, self.options
            )

    def __reduce__(self):
        # Pickled as its arguments; the plan is compiled again when unpickled.
        return (
            partial(MessageReader, read_plan=self.read_plan, **self.options),
            (self.writer_schema, self.reader_schema),
        )

    def read(self, data):
        """Decode one datum from ``data`` (bytes, or anything supporting the
        buffer protocol, which is copied)."""
        cdef Cursor c
        if type(data) is not bytes:
            data = bytes(data)
        if not _READ_PLAN_ENABLED or self.plan is None:
            return _read_data(
                BytesIO(data),
                self.writer_schema,
                self.named_schemas,
                self.reader_schema,
                self.options,
            )
        c = _cursor_for_bytes(data)
        return _exec_plan(c, self.plan)


cpdef is_avro(path_or_buffer):
    if isinstance(path_or_buffer, str):
        fp = open(path_or_buffer, "rb")
        close = True
    else:
        fp = path_or_buffer
        close = False

    try:
        header = fp.read(len(MAGIC))
        return header == MAGIC
    finally:
        if close:
            fp.close()
