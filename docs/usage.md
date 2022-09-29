# Usage Guide

This guide covers usage examples.

## Schema Definition

These operations are generally equivalent to operations available in SQL's
Data Definition Language (DDL).

### Create Table

To create a table, use the `define` method of the `Table` class and pass its 
output to the `create_table` method of a `Schema` instance. For further details, 
see [deriva-py docs](https://github.com/informatics-isi-edu/deriva-py/tree/master/docs).

```python
from deriva.core import DerivaServer
from deriva.chisel import Model, Schema, Table, Column, Key, ForeignKey, builtin_types

# connect to ermrest catalog and get model management interface
model = Model.from_catalog(
    DerivaServer('https', 'demo.derivacloud.org').connect_ermrest('1')
)

# define table and assign to a schema in order to create it in the catalog
foo = model.schemas['public'].create_table(Table.define(
    'Foo',
    column_defs=[
        Column.define('Col1', builtin_types.int8),
        Column.define('Col2', builtin_types.text),
        ...
    ],
    key_defs=[
        Key.define(
            ['Col1']  # list of column names of the key
        ),
        ...
    ],
    fkey_defs=[
        ForeignKey.define(
            ['Col2'],  # list of column names of the foreign key
            'Other Schema', 'Other Table',  # referenced schema and table
            ['Other Col2']  # list of column names of the referenced key
        ),
        ...
    ],
    ...)
)
```

### Drop Table

Drop a table using the `drop` method on the table instance.

```python
foo.drop()
```

### Rename Table

Rename a table using the `alter` method of a table instance.

```python
foo.alter(table_name='bar')
```

### Move Table

"Move" a table to a different schema in the catalog model using the `alter` method
of a table instance.

```python
foo.alter(schema_name='acme')
```

### Create an Association

An "associative table" (also known as "association table", "join table", and by other names) is a table that exists for the sole purpose of establishing a many-to-many relationship between tables. Typically, these are "binary" associations that relate just two tables, though greater arity is possible.

For a simplified method of creating a (binary) association between two tables, consider using the `create_association` method of the `Schema` class. The method will create a table consisting of two foreign key references based on DERIVA's standard primary key column `RID`. The resulting association table will be named `table1_table2`.

```python
a = model.schemas['public'].create_association(foo, bar)
```

The above method will result in a table named `foo_bar` with columns named `foo` and `bar` of type `text` and foreign keys to tables `foo` and `bar`. It will also have a key `(foo, bar)` and both `foo` and `bar` will be non-nullable. Note that the tables `foo` and `bar` themselves are unchanged.

### Alter Table -- Add Column

Add a column to an existing table by calling the `define` method of the `Column`
class and passing the result to the `create_column` method of a `Table` instance.

```python
foo.create_column(Column.define('qux', builtin_types.text))
```

### Alter Table -- Drop Column

Drop a column from a table by calling the `drop` method of a `Column` instance.

```python
foo.columns['qux'].drop()
```

### Alter Table -- Rename Column

Rename a column by calling the `alter` method of a `Column` instance.

```python
foo.columns['qux'].alter(name='quux')
```

### Alter Table -- Key and ForeignKey Definition

Analogous operations exist for creating, dropping, and altering `Key` and `ForeignKey`
instances of a `Table`. They follow the same pattern of `define` and `create_key` or
`create_fkey`.

### Alter Table -- Add Reference

For a simplified method of adding a foreign key reference, consider using `add_reference`. This method of the `Table` class adds a foreign key reference from `self` to a given `table` object based on DERIVA's standard primary key column `RID`. The resulting foreign key column in `self` will be named `table.name`.

```python
foo.add_reference(model.schemas['public'].tables['bar'])
```

The above method will result in a column in table `foo` named `bar` of type `text`, and it will participate in a foreign key that references table `bar` column `RID`.

## Schema Evolution Expressions

In addition to the schema definition interfaces, chisel supports schema evolution
_expressions_ similar to the SQL `CREATE TABLE AS` statement.

```python
acme = model.schemas['acme']
acme.create_table_as(
    'bar',  # table name
    foo.where(foo.columns['Col1'] == 42).select(foo.columns['Col2']) # expression
)
```

Above a new table named `bar` in schema `acme` is created from the _expression_ on
the table `Foo` (referenced by Python variable `foo`) where `Col1` is equal to
`42` and then selects only the `Col2` column out of the relation. 

The example also demostrates that an expression _is_ also a relation like the 
source table, and therefore you can continue to chain operations off of it like 
the `where` followed by the `select` in the example.

In order to materialize the new relation, it must be executed by the 
`create_table_as` method. Unlike the [schema definition](#schema-definition) 
methods defined above, the [schema evolution expressions](#schema-evolution-expressions) must be passed to the `create_table_as` method of
a `Schema` instance in order to be materialized in the catalog.

Chisel comes with several pre-defined expressions to reduce the effort required for
composing some common but complicated transformations.

### SQL vs CHiSEL (SMO) Expressions

The key distinction between SQL and CHiSEL expressions is that CHiSEL expressions
are translated into Schema Modification Operators (SMOs). A CHiSEL SMO not only
computes the attributes (i.e., column names) and tuples (i.e., rows) of the new
relation, it also preserves the column definitions and translates the constraints, 
schema annotations, and ACLs per the expression.

### Where

Use the `where` method on a `Table` instance to filter the rows of the source
relation. In relational theory, this operation is actually called a _select_ or
_restrict_. Currently, the where-clause may consist of a `COLUMN OP LITERAL` 
comparison or a conjunction ("and" using the 'bitwise-and' operator `&`) or disjunction ("or" using the 'bitwise-or' operator `|`) of these simple comparisons.

```python
# relation with just tuples that satisfy "Col1 == 42"
acme.create_table_as(
    'bar',
    foo.where(foo.columns['Col1'] == 42)
)

# conjunction of above and "Col2 == hello"
acme.create_table_as(
    'bar',
    foo.where((foo.columns['Col1'] == 42) & (foo.columns['Col1'] == 'hello'))
)
```
**NOTE**: if you are new to Python, be aware that `&` and `|` are actually bitwise
operators, but have been overloaded as logical operators here. Be careful, not to 
mistake these for python keywords for logical operators `and` and `or`.

### Select

Use the `select` method on a `Table` instance to subset the columns of the 
source relation. The arguments may be column names (`str`), column objects
(`Column` objects), and aliases or negations on columns. If no columns are given, 
then all columns are selected.

```python
# relation with just 2 columns from the source relation
acme.create_table_as(
    'bar',
    foo.select('Col1', foo.columns['Col2'])
)

# relation with 2 columns but renamed
acme.create_table_as(
    'bar',
    foo.select(
        foo.columns['Col1'].alias('ColONE'), 
        foo.columns['Col2'].alias('ColTWO')
    )
)

# all except 1 column
acme.create_table_as(
    'bar',
    foo.select(~foo.columns['Col2'])
)

# all columns
acme.create_table_as(
    'bar',
    foo.select()
)
```
**NOTE**: the `~` is a bitwise operator but overloaded here to drop a column from
a projection list.

### Join

The expression returned by the `join` method on a `Table` instance is equivalent
to a `CROSS JOIN` (a.k.a., cartesian product) in SQL. Obvoiusly, it is 
therefore a very expensive operation to perform. The relation produced by a
Join will include all columns from both relations, in some cases qualified with
the source table's name to resolve name collisions.

```python
acme.create_table_as(
    'bar',
    foo.join(model.schemas['acme'].tables['baz'])
)
```

### Union

The expression returned by the `union` method on a `Table` instance produces a 
relation that has the same table definition as the source table but combines the
rows from the source table and the input table. The schemas of the source and input
tables _must_ match.

```python
acme.create_table_as(
    'bar',
    foo.union(model.schemas['acme'].tables['baz'])
)

# or use the plus operator
acme.create_table_as(
    'bar',
    foo + model.schemas['acme'].tables['baz']
)
```

### Clone

The `clone` method on a `Table` instance returns an expression that clones the
source table. In fact, `clone` is nothing more than `select()`; i.e., selecting
all of the columns of the source relation.

```python
acme.create_table_as(
    'bar',
    foo.clone()
)
```

### To Domain

The expression returned by the `to_domain` method on a `Column` instance produces 
a relation from the deduplicated values of the source column. In other words, it 
takes an unconstrained text column and returns a deduplicated set of terms that can
be used as a custom domain of values.

```python
acme.create_table_as(
    'bar',
    foo.columns['Col2'].to_domain()
)
```

### To Vocabulary

The expression returned by the `to_vocabulary` method on a `Column` instance is
similar to the `to_domain`, except that it returns not only the term column but 
also a column of `synonyms` of the remaining values of the input column.

```python
acme.create_table_as(
    'bar',
    foo.columns['Col2'].to_vocabulary()
)
```

### To Atoms

The expression returned by the `to_atoms` method on a `Column` instance produces 
a relation that _unnests_ the source columns values. Examples of nested values 
include comma-separated or other delimiter-separated values. These values will be
unnested and each individual value (i.e., atom) returned in a separate row. In 
addition, the relation will include a foreign key to the source relation.

```python
acme.create_table_as(
    'bar',
    foo.columns['Col2'].to_atoms()
)
```

### Reify

The `reify` method on a `Table` instance will return an expression for reifying
a concept embedded in the source table. The first argument (`unique_columns`) must
be a collection of columns (or column names) to be used as the key of the new 
relation. They need not be unique in the source relation. The remaining arguments 
must be columns (or column names) that will form the non-key columns of the new 
relation.

```python
acme.create_table_as(
    'bar',
    foo.reify(['Col1'], 'Col2', 'Col8')
)
```

### Reify Subconcept

The `reify_sub` method on a `Table` instance will return an expression for 
reifying a sub-concept embedded in the source table. In addition to the columns
explicitly passed `reify_sub`, the resulting relation will also have an
inferred foreign key to the source table based on introspection of the table 
definition.

```python
acme.create_table_as(
    'bar',
    foo.reify_sub('Col1', 'Col2')
)
```

### Align

The `align` method on a `Column` instance returns an expression for aligning it
with a vocabulary or domain table. Columns can be aligned against a "vocabulary" with `name` and `synonyms` or against a simpler "domain" with only a `name` column.

```python
terms = model.schemas['acme'].tables['Terminology']
acme.create_table_as(
    'bar',
    foo.columns['Col2'].align(terms)
)
```

### To Tags

The `to_tags` method on a `Column` instance returns an expression for unnesting 
and aligning a column with a vocabulary or domain. In addition to the aligned
values, the relation will also include an inferred foreign key to the source 
relation. The relation produced by `to_tags` can therefore be used as an 
associative relation between the source table and the domain or vocabulary table.

```python
terms = model.schemas['acme'].tables['Terminology']
acme.create_table_as(
    'bar',
    foo.columns['bars'].to_tags(terms)
)
```

### Associate

The `associate` method on a `Table` instance returns an expression for converting a 1:N foreign key reference into a M:N association table (a.k.a., join table).

```python
foo = model.schemas['acme'].tables['foo']
acme.create_table_as(
    'foo_bar',
    foo.associate(foo.columns['bar'])
)
```

The above example assumes that `bar` is a table and `foo.bar` is a column that is used in a foreign key reference to from `foo` to `bar`.

## Model Management

The `alter` and `drop` methods above are integrated with model management operations for 
`prune`ing and `replace`ing the renamed or dropped objects in DERIVA's schema annotations. 
While the schema evolution `alter` and `drop` are executed immediately the changes to the
annotations are only local until `apply()` is called explicitly.

```python
# schema changes
acme.tables['foo'].drop(cascade=True)
# ...executed on the server, table 'foo' no longer exists now

# ..."apply" is not needed to affect server state of schema changes
# ...however, "annotation" changes are only locally modified by the API to allow review before commit

# now, "apply" the annotation changes to the remote database
# ...this can be called last after numerous schema change operations above
model.apply()
```

## Session Manager

Schema evolution expressions can be performed in a `with` block and materialized
only at the exit of the block. To learn more, read about the 
[Model Evolution Session Manager](./context.md).