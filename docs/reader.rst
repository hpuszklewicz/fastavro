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
* ``schemaless_reader``: only when it is given a ``BytesIO`` and a record
  schema that went through ``parse_schema``. Parse the schema once and reuse
  it; parsing it on every call is slower than before. Plans are kept in a
  cache of 2048 entries (``FASTAVRO_SCHEMALESS_PLAN_CACHE`` changes the size);
* ``MessageReader``: one plan, prepared when it is created, for any schema.
  Keeping one ``MessageReader`` per schema is the fastest way to decode many
  messages.

For valid data, decoded results and errors are the same as before. For
corrupt or cut-off data, read plans always raise an error, where the previous
reader sometimes returned data (see the changelog).

To switch back to the previous reader, for example to compare results:

* set ``FASTAVRO_READ_PLAN=0`` in the environment before ``fastavro`` is
  imported, or
* call ``fastavro.read.set_read_plan_enabled(False)``
  (``read_plan_enabled()`` reports the current state).

Both do nothing on the pure-Python implementation (PyPy), which does not have
read plans.
