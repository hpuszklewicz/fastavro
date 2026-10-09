# fastavro
[![Build Status](https://github.com/fastavro/fastavro/workflows/Build/badge.svg)](https://github.com/fastavro/fastavro/actions)
[![Documentation Status](https://readthedocs.org/projects/fastavro/badge/?version=latest)](http://fastavro.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/fastavro/fastavro/branch/master/graph/badge.svg)](https://codecov.io/gh/fastavro/fastavro)

## This fork: faster reading (unofficial preview)

This fork adds *compiled read plans* to fastavro: on CPython, decoding is
usually 3-5x faster (see "Compiled read plans" in [docs/reader.rst](docs/reader.rst)).
The preview is version `1.13.1+readplan.3`: official fastavro 1.13.1 plus read
plans. It is not an official fastavro release.

Prebuilt wheels for CPython 3.11 to 3.15 on Linux, macOS and Windows are on the
[`readplan-preview-3` release](https://github.com/hpuszklewicz/fastavro/releases/tag/readplan-preview-3),
so there is nothing to compile.

### 1. Install

To try it without changing your project, run your code with:

```sh
uv run --with "fastavro==1.13.1+readplan.3" --find-links https://github.com/hpuszklewicz/fastavro/releases/expanded_assets/readplan-preview-3 python your_script.py
```

Or, to use it in a uv project, add this to its `pyproject.toml` and run
`uv sync`. Only `fastavro` comes from the preview; everything else still comes
from PyPI. To go back, delete these lines and run `uv sync` again.

```toml
[[tool.uv.index]]
name = "fastavro-preview"
url = "https://github.com/hpuszklewicz/fastavro/releases/expanded_assets/readplan-preview-3"
format = "flat"
explicit = true

[tool.uv.sources]
fastavro = { index = "fastavro-preview" }
```

`fastavro.__version__` is `1.13.1+readplan.3` when the preview is in use.

### 2. Decode messages with `MessageReader`

`schemaless_reader` is unchanged: it is the official reader. To decode
schemaless messages with a read plan, create a `MessageReader` once per schema
and call its `read` method:

```python
from io import BytesIO

import fastavro
from fastavro.read import MessageReader

schema = {
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
    ],
}

# An example message
buf = BytesIO()
fastavro.schemaless_writer(buf, fastavro.parse_schema(schema), {"id": 1, "name": "a"})
payload = buf.getvalue()

reader = MessageReader(schema)  # once per schema
record = reader.read(payload)  # was: fastavro.schemaless_reader(BytesIO(payload), schema)
```

With a reader schema, use `MessageReader(writer_schema, reader_schema)`; other
options of `schemaless_reader` go to the constructor too. If your application
already keeps parsed schemas by id, keep a `MessageReader` per id instead:

```python
readers = {}  # schema id -> MessageReader


def decode(schema_id, payload: bytes) -> dict:
    reader = readers.get(schema_id)
    if reader is None:
        reader = readers[schema_id] = MessageReader(writer_schema(schema_id), reader_schema(schema_id))
    return reader.read(payload)
```

Files read with `fastavro.reader` or `block_reader` use read plans
automatically.

### 3. Compare with the previous reader

For valid data, `MessageReader` returns exactly what `schemaless_reader`
returns: the same values, types and key order. Corrupt or cut-off messages
always raise an error, where `schemaless_reader` sometimes returns data, so
catch errors broadly (`except Exception`) where you handle bad messages.

```python
new = MessageReader(schema).read(payload)
old = fastavro.schemaless_reader(BytesIO(payload), schema)
assert new == old
```

`MessageReader(schema, read_plan=False)` uses the previous reader, for example
as the control group of an A/B test; `read_plan=False` also works with `reader`
and `block_reader`. To turn read plans off everywhere, set
`FASTAVRO_READ_PLAN=0`.

No wheel for your platform? This builds the preview from source instead (needs
a C compiler):

```sh
uv run --with "fastavro @ git+https://github.com/hpuszklewicz/fastavro@readplan-preview-3" python your_script.py
```

> [!IMPORTANT]
> Fastavro is currently in maintenance mode. Efforts will be made to try to
> update to the latest versions of Python, fix security issues, and merge
> simple bug fixes or features, but even those might be significantly delayed.

Because the Apache Python `avro` package is written in pure Python, it is
relatively slow. In one test case, it takes about 14 seconds to iterate through
a file of 10,000 records. By comparison, the JAVA `avro` SDK reads the same file in
1.9 seconds.

The `fastavro` library was written to offer performance comparable to the Java
library. With regular CPython, `fastavro` uses C extensions which allow it to
iterate the same 10,000 record file in 1.7 seconds. With PyPy, this drops to 1.5
seconds (to be fair, the JAVA benchmark is doing some extra JSON
encoding/decoding).

`fastavro` supports the following Python versions:

* Python 3.10
* Python 3.11
* Python 3.12
* Python 3.13
* Python 3.14
* Python 3.15
* PyPy3

## Supported Features

* File Writer
* File Reader (iterating via records or blocks)
* Schemaless Writer
* Schemaless Reader
* JSON Writer
* JSON Reader
* Codecs (Snappy, Deflate, Zstandard, Bzip2, LZ4, XZ)
* Schema resolution
* Aliases
* Logical Types
* Parsing schemas into the canonical form
* Schema fingerprinting

## Missing Features

* Anything involving Avro's RPC features

[Cython]: http://cython.org/

# Documentation

Documentation is available at http://fastavro.readthedocs.io/en/latest/

# Installing
`fastavro` is available both on [PyPI](http://pypi.python.org/pypi)

    pip install fastavro

and on [conda-forge](https://conda-forge.github.io) `conda` channel.

    conda install -c conda-forge fastavro

# Contributing

* Bugs and new feature requests typically start as GitHub issues where they can be discussed. I try to resolve these as time affords, but PRs are welcome from all.
* Get approval from discussing on the GitHub issue before opening the pull request
* Tests must be passing for pull request to be considered

Developer requirements can be installed with `pip install -r developer_requirements.txt`.
If those are installed, you can run the tests with `./run-tests.sh`. If you have trouble
installing those dependencies, you can run `docker build .` to run the tests inside
a Docker container. This won't test on all versions of Python or on PyPy, so it's possible
to still get CI failures after making a pull request, but we can work through those errors
if/when they happen. `.run-tests.sh` only covers the Cython tests. In order to test the
pure Python implementation, comment out `python setup.py build_ext --inplace`
and re-run.

NOTE: Some tests might fail when running the tests locally. An example of this
is this codec tests. If the supporting codec library is not available, the test
will fail. These failures can be ignored since the tests will on pull requests
and will be run in the correct environments with the correct dependencies set up.

### Releasing

We release both to [PyPI][pypi] and to [conda-forge][conda-forge].

We assume you have [twine][twine] installed and that you've created your own
fork of [fastavro-feedstock][feedstock].

* Make sure the tests pass
* Run `make tag`
* Wait for all artifacts to be built and published to the GitHub release.
* Run `make publish`
* The conda-forge PR should get created and merged automatically

[conda-forge]: https://conda-forge.org/
[feedstock]: https://github.com/conda-forge/fastavro-feedstock
[pypi]: https://pypi.python.org/pypi
[twine]: https://pypi.python.org/pypi/twine


# Changes

See the [ChangeLog]

[ChangeLog]: https://github.com/fastavro/fastavro/blob/master/ChangeLog

# Contact

[Project Home](https://github.com/fastavro/fastavro)
[Maintainer: Scott Belden](mailto:scottabelden@gmail.com)
For security related issues, please see the [security policy](https://github.com/fastavro/fastavro/security/policy)
