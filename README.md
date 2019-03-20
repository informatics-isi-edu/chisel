# Welcome to CHiSEL...

CHiSEL is a high-level, user-oriented framework for schema evolution.

## Installation

You can either install quickly from the source repository using Python's `pip`
package manager, or you can clone the source first which has the advantage of 
getting the examples and tests.

### System Requirements

- Python 2.7 or 3.6+
- [Deriva-Py](https://github.com/informatics-isi-edu/deriva-py) library
- Dependencies listed in [setup.py](./setup.py)

### Quick install

Install the library directly from its source code repository. You will 
_not_ get the `examples` and `tests` with the quick install instructions.

```
$ pip install --user git+https://github.com/robes/chisel.git
```

For system-wide installations, use `sudo` and execute the command without the 
`--user` option.

### Clone and install

This installation method gets a copy of the source and then installs it.

1. Clone the source repository
    ```sh
    $ git clone git+https://github.com/robes/chisel.git
    ```
2. Run the tests
    ```sh
    $ cd chisel
    $ python -m unittest discover
    ```
3. Install
    ```sh
    $ pip install --user .
    ```
    Run with `sudo` and without `--user` for a system-wide install.
4. See examples in the `./examples` directory.

## Usage

### Connect to a data source

A data source (e.g., your database) is represented as a `catalog` object.
You will likely need to establish a _user credential_ depending on the type of
data source you are using. For DERIVA catalogs, use the `get_credential` 
function from the `deriva-py` library to get a handle to your user
credential. Note that you will need to establish your user credential (i.e., 
log in to the server) before getting a handle to it.

```python
from deriva.core import get_credential
import chisel

# Get user credential
hostname = 'example.org'
credentail = get_credential(hostname)

# Connect to a DERIVA data source
catalog = chisel.connect('https://example.org/ermrest/catalog/1', credentail)
```

### Get a table in the catalog

Catalogs are organized by _schemas_ (a.k.a., namespaces). Within a schema 
are _tables_. While you do not need to assign table objects to local variables
in your script in order to use them, it can help make your code more readable.

```python
foo = catalog.schemas['a_schema'].tables['foo']
```

For readability, the catalog object aliases `schemas` as `s`, `tables` as 
`t`, and `columns` as `c`, so that the above statement could be rewritten like this.

```python
foo = catalog.s['a_schema'].t['foo']
```

### Perform operations

Chisel operations begin by calling a method of an existing table (e.g., `foo`)
or one of its columns. The methods return objects called "computed relations" 
-- essentially a new table that will be _computed_ from the operations over 
the initial, extant table. These operations can be _chained_ to form more 
complex operations. The operations will be evaluated and executed only when 
they are finally committed to the catalog, and therefore to the underlying 
data source.

In this example, a new unique "domain" of terms is created from the `bar`
column of the `foo` table.

```python
foobar = foo.c['bar'].to_domain()
```

This `to_domain` method, when executed, will select the values of 
column `bar` from table `foo`. It will also _deduplicate_ the values using a 
string similarity comparison of them. Then it will generate a new relation
(i.e., table) to store just those deduplicated values of the column `bar`.

### Assign to catalog and commit

Up to this point, we have only expressed schema evolution operations, but none
have been executed nor has the catalog been altered in any way.

To make our changes permanent, we first need to assign this new relation 
(i.e., table) to the catalog and then `commit` the pending operation.

```python
catalog.s['a_schema'].t['foobar'] = foobar
catalog.commit()
```

Or if you want to do a test run, replace the last line with this.

```python
catalog.commit(dry_run=True)
```

This will dump some diagnostic information to the console about the 
schema evolution operations.

### More examples

See the [examples](./examples) directory of the source, for more examples on 
chisel's usage. To run the examples, you must set your 
`CHISEL_EXAMPLE_CATALOG_URL` environment variable to an ERMrest catalog URL.

## API

Chisel is under active development and its API is subject to change. 
Currently, the best way to learn about the API is to use Python's built-in
`help(...)` method on tables and columns. Such as `help(foo)` or 
`help(foo.c['bar'])` to get a description of the object and listing of its 
methods. This also works on functions, like `help(foo.c['bar'].to_domain)`.
