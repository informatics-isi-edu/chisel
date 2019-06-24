# Usage Guide

This guide covers usage examples.

## Basic usage

Basic usage operators are generally equivalent to operations available in SQL 
DDL (data definition language).

These operations need to be performed in _isolation_ and therefore cannot be 
used within the `with catalog.evolve(): ...` blocks.

### Create a table

```python
import chisel
catalog = chisel.connect(...)
catalog['public'].tables['foo'] = chisel.Table.define('foo')
```

### Drop a table

```python
import chisel
catalog = chisel.connect(...)
del catalog['public'].tables['foo']
```

### Rename a table

```python
import chisel
catalog = chisel.connect(...)
table = catalog['public'].tables['foo']
table.name = 'bar'
```

### Copy a table

```python
import chisel
catalog = chisel.connect(...)
table = catalog['public'].tables['foo']
catalog['public'].tables['bar'] = table.select()
```

### Move a table to a different schema

```python
import chisel
catalog = chisel.connect(...)
table = catalog['public'].tables['foo']
table.sname = 'bar'  # where 'bar' is a different schema in the catalog
```

### Alter table add a column

```python
import chisel
catalog = chisel.connect(...)
table = catalog['public'].tables['foo']
table.columns['baz'] = chisel.Column.define('baz', chisel.data_types.text)
```

### Alter table drop a column

```python
import chisel
catalog = chisel.connect(...)
table = catalog['public'].tables['foo']
del table.columns['baz']
```

### Alter table rename a column

```python
import chisel
catalog = chisel.connect(...)
table = catalog['public'].tables['foo']
column = table.columns['baz']
column.name = 'qux'
```

## Advanced usage

Here are more advanced usage examples that cover chisel features that go beyond
SQL DDL types of operations.

These operations must be performed within a `with catalog.evolve(): ...` block.
The actualy schema evolution is executed after the block exits successfully. If
an exception is raised (and not caught), the evolution is aborted and the
catalog model is restored to its original state.

In order to do a "dry run," call the evolve method with `dry_run=True` and at 
the exit of the evolve block the plan and sample data of computed relations
will be dumped to standard output. No changes to the catalog will be executed
and the catalog model will be restored to its original state.

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
    {foo.columns['id']}, {foo.columns['bar'], foo.columns['baz']})
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
