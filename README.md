# Welcome to CHiSEL...

CHiSEL is a high-level, user-oriented framework for schema evolution.

## Installation

You can either install quickly from the source repository using Python's `pip`
package manager, or you can clone the source first which has the advantage of 
getting the examples and tests.

### System Requirements

- Python 3.5+
- [setuptools](https://pypi.org/project/setuptools/)
- Dependencies listed in [setup.py](./setup.py)

### Quick install

Install the library directly from its source code repository. You will 
_not_ get the `examples` and `tests` with the quick install instructions.

```
$ pip install https://github.com/informatics-isi-edu/chisel.git
```

For system-wide installations, use `sudo` and execute the command without the 
`--user` option.

### Clone and install

This installation method gets a copy of the source and then installs it.

1. Clone the source repository
    ```sh
    $ git clone https://github.com/informatics-isi-edu/chisel.git
    ```
2. Install
    ```sh
    $ cd chisel
    $ pip install .
    ```
    You may need to use `sudo` for system-wide install or add the `--user` option 
    for current user only install.
3. Run the tests
    ```sh
    $ export DERIVA_PY_TEST_HOSTNAME=my-ermrest-host.example.org
    $ python -m unittest discover
    ```
    See [the notes below on setting environment variables for testing](#testing). 
    Note that there may be transient network errors during the running of the tests 
    but if the final status of the tests reads `OK` then the CHiSEL tests have run 
    successfully. The final lines of the output should look something like this, though 
    the total number of tests may change as we add new tests.
    ```sh
    ....................s....s.......................
    ----------------------------------------------------------------------
    Ran 102 tests in 36.071s
    
    OK (skipped=2)
    ```
    Some expensive tests are skipped by default but can be enabled by setting 
    additional environment variables.
4. See examples in the [`./examples` directory](./examples) of this repository.

### Testing

The package includes unit tests. They may be run without any configuration, 
however, certain test cases and suites will be skipped without the following
environment variables defined.

* `DERIVA_PY_TEST_HOSTNAME`:
  To run the ERMrest catalog test suite, set this variable to the hostname of
  a server running an ERMrest service. You will also need to establish valid
  user credentials (e.g., by using the Deriva-Auth client).
* `DERIVA_PY_TEST_CATALOG`:
  In addition, set this variable to reuse a catalog. This variable is typically
  only used during development activities that would motivate frequently
  repeated test runs.
* `CHISEL_TEST_ALL`:
  Set this variable (any value will do) to run all tests rather than skipping the
  most expensive tests.


## Usage

### Connect to a data source

A data source (e.g., your database) is represented as a `catalog` object.
You will likely need to establish a _user credential_ depending on the type of
data source you are using. For DERIVA catalogs, use the Authentication Agent
available in the [DERIVA Client](https://github.com/informatics-isi-edu/deriva-client) 
bundle. Note that you will need to establish your user credential (i.e., 
log in to the server) before performing operations on it that may mutate it.

```python
import chisel

# Connect to a data source
catalog = chisel.connect('https://example.org/ermrest/catalog/1')
```

### Reference a table in the catalog

Catalogs are organized by _schemas_ (a.k.a., namespaces). Within a schema are 
_tables_.

```python
catalog.schemas['a_schema'].tables['foo']
```

For readability, the catalog itself behaves as a collection of schemas, each 
schema object behaves as a collection of tables, and each table behaves as a
 collection of columns. The above expression could be rewritten like this.

```python
catalog['a_schema']['foo']
```

In general, you should _avoid assigning catalog model objects to local
variables_. The model objects may be invalidated following a catalog model 
mutation and, if so, future operations on them will raise an exception.

### Begin a catalog evolution context

Catalog model evolution operations _may_ be performed in a block of statements that
are only processed after exiting the block.

```python
with catalog.evolve():
    # catalog model mutation operations...
```

If any exception is raised and not caught, when the block exits, the pending 
operations will be aborted. Pending operations may be aborted explicitly by 
using the catalog mutation context manager.

```python
with catalog.evolve() as ctx:
    # catalog model mutation operations...
    if something_went_wrong:
        ctx.abort()
    # all operations before and after the abort() are cancelled
    # and the block is immediately exited
```

If you want to do a test run, set the `dry_run` flag in the `evolve()` method.

```python
with catalog.evolve(dry_run=True):
    # catalog model mutation operations...
```

This will dump diagnostic information to the standard output stream.

### When not using a catalog evolution context

If not using the above `catalog.evolve()` context, catalog mutation will be processed 
immediately (somewhat akin to "autocommit" mode in a database). The catalog object
has properties `allow_alter_default` and `allow_drop_default` (defaults `True`) that
are passed to an internal catalog `evolve(...)` method when operations are performed 
without first establishing an explicit evolve block. These defaults may be changed in
order to prevent `alter` and `drop` operations from being performed on the catalog 
model objects.

### Perform operations

Chisel operations begin by calling a method of an existing table (e.g., `foo`)
or one of its columns. The methods return objects called "computed relations" 
-- essentially a new table that will be _computed_ from the operations over 
the initial, extant table. These operations can be _chained_ to form more 
complex operations. The operations will be evaluated and executed only when 
the evolve block is existed.

In this example, a new unique "domain" of terms is created from the `bar`
column of the `foo` table.

```python
catalog['a_schema']['foo']['bar'].to_domain()
```

This `to_domain` method, when executed, will select the values of 
column `bar` from table `foo`. It will also _deduplicate_ the values using a 
string similarity comparison of them. Then it will generate a new relation
(i.e., table) to store just those deduplicated values of the column `bar`.

### Assign to catalog

Up to this point, we have only expressed schema evolution operations, but none
have been executed nor has the catalog been altered in any way.

To make our changes permanent, we first need to assign this new relation 
(i.e., table) to the catalog.

```python
catalog['a_schema']['foobar'] = catalog['a_schema']['foo']['bar'].to_domain()
```

### More examples

For more details, see the brief [usage guide](./docs/usage.md). In addition, 
the [examples](./examples) directory incudes several scripts to illustrate
usage of chisel. To run the example scripts, you must set your 
`CHISEL_EXAMPLE_CATALOG_URL` environment variable to an ERMrest catalog URL.

## API

Chisel is under active development and its API is subject to change. Currently,
the best way to learn about the API is to use Python's built-in `help(...)` 
method on tables and columns. Such as `help(catalog['a_schema']['foo'])` or 
`help(catalog['a_schema']['foo']['bar'])` to get a description of the object 
and listing of its methods. This also works on functions, like 
`help(catalog['a_schema']['foo']['bar'].to_domain)`.
