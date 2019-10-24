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
$ pip install git+https://github.com/robes/chisel.git
```

For system-wide installations, use `sudo` and execute the command without the 
`--user` option.

### Clone and install

This installation method gets a copy of the source and then installs it.

1. Clone the source repository
    ```sh
    $ git clone git+https://github.com/robes/chisel.git
    ```
2. Install
    ```sh
    $ cd chisel
    $ pip install .
    ```
    You may need to use `sudo` for system-wide install or add the `--user` option for current user only install.
3. Run the tests
    ```sh
    $ export CHISEL_TEST_ERMREST_HOST=my-ermrest-host.example.org
    $ python -m unittest discover
    ```
    See [the notes below on setting environment variables for testing](#testing).
4. See examples in the [`./examples` directory](./examples) of this repository.

### Testing

The package includes unit tests. They may be run without any configuration, 
however, certain test cases and suites will be skipped without the following
environment variables defined.

* `CHISEL_TEST_ERMREST_HOST`
  To run the ERMrest catalog test suite, set this variable to the hostname of
  a server running an ERMrest service. You will also need to establish valid
  user credentials (e.g., by using the Deriva-Auth client).
* `CHISEL_TEST_ERMREST_CATALOG`
  In addition, set this variable to reuse a catalog. This variable is typically
  only used during development activities that would motivate frequently
  repeated test runs.


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
variables_. The model objects will not be updated following a catalog model 
mutation and future operations on them are likely to fail.

### Begin a catalog evolution block

Catalog model evolution operations are performed in a block of statements that
are only performed after exiting the block.

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
