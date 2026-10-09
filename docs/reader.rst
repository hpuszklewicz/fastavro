fastavro.read
=============

.. autoclass:: fastavro._read_py.reader

.. autoclass:: fastavro._read_py.block_reader

.. autoclass:: fastavro._read_py.Block

.. autofunction:: fastavro._read_py.schemaless_reader

.. autoclass:: fastavro._read_py.MessageReader
   :members: read

.. autofunction:: fastavro._read_py.is_avro


Compiled read plans
-------------------

Without read plans, fastavro looks at the schema again for every value it
reads, to decide how to decode it. On CPython it now makes those decisions
once and keeps them as a *read plan*: which fields to read in which order,
what type each one is, and which defaults and type conversions a reader
schema needs. Decoding then just follows the plan over the bytes in C, like
running compiled code instead of interpreting it. This is usually several
times faster. It is used by:

* ``reader`` (``iter_avro``) and ``block_reader``: one plan per file, so files
  with only a few records gain little;
* ``MessageReader``: one plan, prepared when it is created, for any schema.

``schemaless_reader`` is unchanged and does not use read plans. To decode
schemaless messages with one, create a ``MessageReader`` once per schema, for
example where the parsed schemas are kept now, and call its ``read`` method::

    reader = MessageReader(writer_schema, reader_schema)  # once per schema
    record = reader.read(payload)  # payload: the message's bytes

For valid data, ``read`` returns exactly what ``schemaless_reader`` returns
with the same arguments.

For valid data, decoded results and errors are the same as before. For
corrupt or cut-off data, read plans always raise an error, where the previous
reader sometimes returned data (see the changelog).

To switch back to the previous reader, for example to compare results:

* set ``FASTAVRO_READ_PLAN=0`` (or ``false``, ``no``, ``off``) in the
  environment before ``fastavro`` is imported, or
* call ``fastavro.read.set_read_plan_enabled(False)``
  (``read_plan_enabled()`` reports the current state), or
* for a single reader, pass ``read_plan=False`` to ``reader``,
  ``block_reader`` or ``MessageReader``. Nothing global changes, so a
  threaded service can decode some messages each way, for example as the
  control group of an A/B test. The two settings above still turn read plans
  off everywhere.

Both do nothing on the pure-Python implementation (PyPy), which does not have
read plans.
