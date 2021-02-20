# Usage Guide

This guide covers usage examples.

## Simple operations

The simple operators are generally equivalent to operations available in SQL 
DDL (data definition language).

### Create a table

To create a table, use the `.define()` method for the Table, Column, etc. classes
of the class hierarchy. The exact signatures of the `define` methods are currently
identical to those of `deriva-py`. For further detail see [deriva-py docs](https://github.com/informatics-isi-edu/deriva-py/tree/master/docs).

```python
import chisel
from chisel import Table, Column, Key, ForeignKey, data_types as typ

catalog = chisel.connect(...)

# define table and assign to a schema in order to create it in the catalog
catalog['public'].tables['foo'] = Table.define(
    'foo',
    column_defs=[
        Column.define('Col1', typ.int8), 
        Column.define('Col2', typ.text), 
        ...
    ],
    key_defs=[
        Key.define(['Col1']),
        ...
    ],
    fkey_defs=[
        ForeignKey.define(['Col2'], 'Other Schema', 'Other Table', ['Other Col2']),
        ...
    ],
    ...)

# get the newly created table in order to use it in operations
foo = catalog['public'].tables['foo']

# perform operations on new table 'foo'...
list(foo.select().fetch())
```

### Drop a table

Drop a table using the `del` statement on the table's container.

```python
del catalog['public'].tables['foo']
```

### Rename a table

Rename a table by changing its `.name` property.

```python
table = catalog['public'].tables['foo']
table.name = 'bar'
```

### Clone a table

Make an exact clone of a table by assigning the results of its `.clone()` 
method to an unused table name.

```python
table = catalog['public'].tables['foo']
catalog['public'].tables['bar'] = table.clone()
```

### Move a table to a different schema

"Move" a table by reassigning its `.schema` property to the desired 
schema.

```python
table = catalog.schemas['public'].tables['foo']
table.schema = catalog.schemas['bar']
```

### Alter table add a column

Add a new column to a table by assigning a `Column` definition to an unused 
column name of the table.

```python
table = catalog['public'].tables['foo']
table.columns['baz'] = Column.define('baz', typ.text)
```

### Alter table drop a column

Drop a column from a table by deleting it from the table's `.columns` container.

```python
table = catalog['public'].tables['foo']
del table.columns['baz']
```

### Alter table rename a column

Rename a column by setting its `.name` property.

```python
table = catalog['public'].tables['foo']
column = table.columns['baz']
column.name = 'qux'
```

### Join relations

JOINs are very limited in chisel at this time. The `.join(rel)` method will
produce an unfiltered cross-join of two relations, which may be filtered
with an also very limited WHERE clause, using the `.where(...)` method. These
operations do not directly mutate the catalog, but the resultant relation can
be assigned to an unused table name of a schema's `tables` container to 
create a new table from the relation.

```python
catalog['public']['foo'].join(catalog['public']['bar']).where(...)
```

### Union of relations

Tables (or any relation) may be unioned with the `.union(rel)` method or the
`+` operator. Like JOINs, UNION does not directly mutate the catalog. 

```python
catalog['public']['foo'].union(catalog['public']['bar'])
# or... foo + bar
```

## Complex operations

The complex operations cover chisel features that go beyond SQL DDL types of 
operations.

### Create table as domain from existing column

Table `bar` will have a `name` column from the deduplicated values of `foo.bar`.

```python
foo = catalog['public'].tables['foo']
catalog['public'].tables['bar'] = foo.columns['bar'].to_domain()
```

### Create table as vocabulary from existing column

Table `bar` will have a `name` column from the deduplicated values of `foo.bar`
_and_ a `synonyms` column that will have all of the remaining values for `name`
that were not selected as canonical.

```python
foo = catalog['public'].tables['foo']
catalog['public'].tables['bar'] = foo.columns['bar'].to_vocabulary()
```

### Create table from atomizied values from existing column 

Table `bar` will have a column `bar` containing the unnested values of 
`foo.bar` and also a foriegn key to `foo`.

```python
foo = catalog['public'].tables['foo']
catalog['public'].tables['bar'] = foo.columns['bar'].to_atoms()
```

### Create a table by reifying a concept embedded in another table

In `reify`, the first set of columns are used as the `key` of the new table, 
and the second set of columns used as the non-key columns of the new table.

```python
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
foo = catalog['public'].tables['foo']
bar_terms = catalog['vocab'].tables['bar']
catalog['public'].tables['foo_bar'] = foo.columns['bars'].to_tags(bar_terms)
```
