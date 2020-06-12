# Usage Guide

This guide covers usage examples.

## Catalog `evolve` block

Operations must be performed within a `with catalog.evolve(): ...` block.
The actual schema evolution is executed after the block exits successfully. If
an exception is raised (and not caught), the evolution is aborted and the
catalog model is restored to its original state.

### Testing with `dry_run` flag

In order to do a "dry run," call the evolve method with `dry_run=True` and at 
the exit of the evolve block the plan and sample data of computed relations
will be dumped to standard output. No changes to the catalog will be executed
and the catalog model will be restored to its original state.

### Guarding with `allow_alter` and `allow_drop`

In order to guard against accidental table alteration or destruction, the 
`evolve` method accepts `allow_alter` and `allow_drop` Boolean parameters. 
By default, these parameters are `False` and the evolve block will prevent
table alter or drop operations, respectively.

## Simple operations

The simple operators are generally equivalent to operations available in SQL 
DDL (data definition language).

### Create a table

```python
import chisel
from chisel import Table, Column, Key, ForeignKey
catalog = chisel.connect(...)

with catalog.evolve():
    # define table and assign to a schema in order to create it in the catalog
    catalog['public'].tables['foo'] = Table.define(
        'foo',
        column_defs=[Column.define(...), ...],
        key_defs=[Key.define(...), ...],
        fkey_defs=[ForeignKey.define(...), ...],
        ...)

# get the newly created table in order to use it in operations
foo = catalog['public'].tables['foo']

# perform operations on new table 'foo'...
list(foo.select().fetch())
```

### Drop a table

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve(allow_drop=True):
    del catalog['public'].tables['foo']
```

### Rename a table

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():
    table = catalog['public'].tables['foo']
    table.name = 'bar'
```

### Clone a table

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():
    table = catalog['public'].tables['foo']
    catalog['public'].tables['bar'] = table.clone()
```

### Move a table to a different schema

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():
    table = catalog.schemas['public'].tables['foo']
    table.schema = catalog.schemas['bar']
```

### Alter table add a column

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():
    table = catalog['public'].tables['foo']
    table.columns['baz'] = chisel.Column.define('baz', chisel.data_types.text, ...)
```

### Alter table drop a column

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve(allow_alter=True):
    table = catalog['public'].tables['foo']
    del table.columns['baz']
```

### Alter table rename a column

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve(allow_alter=True):
    table = catalog['public'].tables['foo']
    column = table.columns['baz']
    column.name = 'qux'
```

### Join relations

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():
    catalog['public']['foo'].join(catalog['public']['bar']).where(...)
```

### Union of relations
```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():
    catalog['public']['foo'].union(catalog['public']['bar'])
    # or... foo + bar
```

### Link tables

**Not Implemented Yet**

This operation adds a foreign key reference from the source table (`foo`) to 
the destination table (`bar`).

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():  # TODO
    foo = catalog['public'].tables['foo']
    bar = catalog['public'].tables['bar']
    foo.link(bar)
```

### Associate tables

**Not Implemented Yet**

This operation adds an association table (`foo_bar`) with foreign key 
references between two tables (`foo` and `bar`).

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():  # TODO
    foo = catalog['public'].tables['foo']
    bar = catalog['public'].tables['bar']
    foo.associate(bar)
```

## Complex operations

The complex operations cover chisel features that go beyond SQL DDL types of 
operations.

### Create table as domain from existing column

Table `bar` will have a `name` column from the deduplicated values of `foo.bar`.

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():
    foo = catalog['public'].tables['foo']
    catalog['public'].tables['bar'] = foo.columns['bar'].to_domain()
```

### Create table as vocabulary from existing column

Table `bar` will have a `name` column from the deduplicated values of `foo.bar`
_and_ a `synonyms` column that will have all of the remaining values for `name`
that were not selected as canonical.

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():
    foo = catalog['public'].tables['foo']
    catalog['public'].tables['bar'] = foo.columns['bar'].to_vocabulary()
```

### Create table from atomizied values from existing column 

Table `bar` will have a column `bar` containing the unnested values of 
`foo.bar` and also a foriegn key to `foo`.

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():
    foo = catalog['public'].tables['foo']
    catalog['public'].tables['bar'] = foo.columns['bar'].to_atoms()
```

### Create a table by reifying a concept embedded in another table

In `reify`, the first set of columns are used as the `key` of the new table, 
and the second set of columns used as the non-key columns of the new table.

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():
    foo = catalog['public'].tables['foo']
    catalog['public'].tables['barbaz'] = foo.reify(
        {foo.columns['id']}, 
        {foo.columns['bar'], foo.columns['baz']}
    )
```

### Create a table by reifying a sub-concept embedded in another table

In addition to the columns explicitly given in `reifysub(...)`, table `bar` 
will also have a foriegn key to the source table `foo` based on the 
introspected key of `foo`.

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():
    foo = catalog['public'].tables['foo']
    catalog['public'].tables['bar'] = foo.reify_sub(foo.columns['bar'])
```

### Create a table by aligning a column with a vocabulary or domain table

Given a vocabulary table `vocab.bar`, `foo_fixed` is the result of aligning
its column `bar` with the terms in `vobar.bar`. Columns can be aligned 
against a "vocabulary" with `name` and `synonyms` or against a simpler
"domain" with only a `name` column. Table `foo_fixed` is `foo` with the
target column (`bar` in the example here) replaced with a foreign key to
the vocabulary or domain table (`vocab.bar` in the example here).

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():
    foo = catalog['public'].tables['foo']
    bar_terms = catalog['vocab'].tables['bar']
    catalog['public'].tables['foo_fixed'] = foo.columns['bar'].align(bar_terms)
```

### Create a table by unnesting and aligning a column with a vocabulary or domain

In the following example, a table `foo` contains a column `bars` with a 
denormalized, delimited list of values. The values of `bars` are unnested
into atomic values, which are then aligned against a vocabulary `vocab.bar`.
The output relation assigned to `foo_bar` contains not only the normalized 
column `bars` (which can be renamed per the basic usage above) but also a
foreign key to `foo` from where it came.

```python
import chisel
catalog = chisel.connect(...)
with catalog.evolve():
    foo = catalog['public'].tables['foo']
    bar_terms = catalog['vocab'].tables['bar']
    catalog['public'].tables['foo_bar'] = foo.columns['bars'].to_tags(bar_terms)
```
