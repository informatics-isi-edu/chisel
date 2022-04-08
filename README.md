# Welcome to CHiSEL

CHiSEL is a high-level, user-oriented framework for schema evolution and model management in the [DERIVA](http://docs.derivacloud.org) platform.

Features:
  * Compatible with [DERIVA](http://docs.derivacloud.org)'s [deriva-py catalog model management API](http://docs.derivacloud.org/users-guide/project-tutorial.html#your-first-catalog);
  * Support for SQL-like `CREATE TABLE AS` schema evolution expressions;
  * Several built-in functions to reduce effort of writing complicated expressions;
  * Ability to view the output of expressions before materializing;
  * Schema evolution expressions that update schema annotations too;
  * Bulk operation execution to increase efficiency;
  * (_NEW_) Model management operations to `find`, `prune`, and `replace` column, key, and foreign key symbols in DERIVA schema annotations;
  * (_NEW_) Integrated schema modification and model management operations for column, key, and foreign key symbols operations for `alter` (rename) and `drop`;
  * (_NEW_) Convenient `cascade`ing `drop` operations on schema, table, column, key, and foreign key symbols model element.
  * (_NEW_) Associate operation the converts a 1:N relationship into an M:N association table (a.k.a., join table).

A brief example:

```python
from deriva.core import DerivaServer, get_credential
from deriva.chisel import Model

hostname = 'tutorial.derivacloud.org'
model = Model.from_catalog(
   DerivaServer('https', hostname, get_credential(hostname)).connect_ermrest('1')
)

public = model.schemas['public']
foo = public.tables['foo']

public.create_table_as('bar', foo.columns['bar'].to_vocabulary())
```

## Requirements

You will need Python **3.7+** and `pip` for installation.

**OPTIONAL**: To use chisel's `graph(...)` method, you will also need to have the graphviz executables installed for your operating system. For information about how to download and install graphviz, see https://graphviz.org/.

## Install

To install from the PyPI repository:

```sh
$ pip install deriva-chisel
```

To install the latest development branch you will also need `git`.

```sh
$ git clone https://github.com/informatics-isi-edu/chisel.git
$ cd chisel
$ pip install -e .
```

For more details, see the [Installation](./docs/install.md) guide.

## Get Started

Connect to a DERIVA catalog and create the `Model` management interface.

```python
from deriva.core import DerivaServer, get_credential
from deriva.chisel import Model

hostname = 'tutorial.derivacloud.org'
model = Model.from_catalog(
   DerivaServer('https', hostname, get_credential(hostname)).connect_ermrest('1')
)
```
**Note**: use the 
[DERIVA Authentication Agent](http://docs.derivacloud.org/users-guide/managing-data.html) 
to login to the server _before_ creating the `DerivaServer` object.

### Schema Definition

The deriva-py `Model` interface implemented by `chisel` follows a pattern:

1. **Define**: `define` class methods on `Schema`, `Table`, `Column`, `Key`, and 
   `ForeignKey` classes to define the respective parts of the catalog model.
2. **Create**: `create_schema`, `create_table`, `create_column`, etc. instance 
   methods on `Model`, `Schema`, and `Table` objects, respectively, that 
   accept their respective definitions (returned by their `define` method) and 
   issue requests to the DERIVA server to create that part of the catalog model.
3. **Alter**: `alter` instance methods on model objects for altering aspects of 
   their definitions.
4. **Drop**: `drop` instance methods on model object for dropping them from the 
   catalog model.
5. **Apply**: `apply` model "annotation" changes performed explicitly or 
   implicitly by the `alter` and `drop` methods.

```python
from deriva.core import DerivaServer, get_credential
from deriva.chisel import Model, Schema, Table, Column, Key, ForeignKey, builtin_types

# connect to catalog
hostname = 'tutorial.derivacloud.org'
model = Model.from_catalog(
   DerivaServer('https', hostname, get_credential(hostname)).connect_ermrest('1')
)

# create a schema
acme = model.create_schema(Schema.define('acme'))

# create a table
foo = acme.create_table(Table.define(
   'foo',
   column_defs=[
      Column.define('bar', builtin_types.int8, nullok=False),
      Column.define('baz', builtin_types.text),
      Column.define('qux', builtin_types.timestamptz),
      Column.define('xyzzy', builtin_types.text)
   ],
   key_defs=[
      Key.define(...)
   ],
   fkey_defs=[
      ForeignKey.define(...)
   ]
))

# rename column
foo.columns['xyzzy'].alter(name='zzyzx')

# drop column
foo.columns['baz'].drop()

# apply model "annotation" changes (this only affects "annotation" changes)
model.apply()
```

For more details, see the [deriva-py tutorial](http://docs.derivacloud.org/users-guide/project-tutorial.html#your-first-catalog).

### Schema Evolution Expressions

In addition to schema definition, chisel supports table creation from 
schema evolution _expressions_. If you are familiar with SQL, these are akin
to the `CREATE TABLE <name> AS <expr>` statement.

```python
acme.create_table_as(
   'bar',  # table name
   foo.where(foo.columns['qux'] == '2008').select('bar')  # expression
)
```

Chisel comes with several builtin expression builders to reduce the difficulty
of expressing some complicated transformations. 

In this example, a new unique "domain" of terms is created from the `zzyzx`
column of the `foo` table.

```python
acme.create_table_as(
   'zzyzx_terms',  # table name
   foo.columns['zzyzx'].to_domain()  # expression
)
```

The `to_domain` method, when executed, will select the values of column `zzyzx`. 
It will also _deduplicate_ the values using a string similarity comparison. Then 
it will generate a new relation (i.e., table) to store just those deduplicated 
values of the column `zzyzx`.

For more details, see the [usage examples](./examples) and the [usage guide](./docs/usage.md).
