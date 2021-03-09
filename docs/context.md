# Model Evolution Session Manager

Catalog model evolution operations _may_ be performed in a block of statements that
are only processed after exiting the block.

```python
with model.begin() as session:
    # model evolution operations
    session.create_table_as(
        'acme',  # schema name
        'foo',  # table name
        bar.columns['foo'].to_domain()  # expression
    )
```

Note that here `create_table_as` takes a `schema_name` as its first parameter, 
because it is bound to a `Model`-wide _session_ rather than to a `Schema` instance.

### Rollback

If any exception is raised and not caught, when the block exits, the pending 
operations will be rolled back. Pending operations may be rolled back explicitly.

```python
with model.begin() as session:
    # catalog model mutation operations...
    if something_went_wrong:
        session.rollback()
    # all operations before and after the abort() are cancelled
    # and the block is immediately exited
```

### Dry Run

To do a dry run, set `dry_run=True` (default `False`) and the relations will not be
materialized to the catalog. Instead, at the exit of the block the plan and sample 
data from computed relations will be dumped to the `debug` logger.

```python
with model.begin(dry_run=True) as session:
    # catalog model mutation operations...
    ...
```

### Work Sharing

To search for and reuse common subexpressions in a schema evolution session, set
`enable_work_sharing=True` (default `False`). For example, if selecting the
rows of a large source table, and creating multiple tables based on expressions that
build on that initial expression, then chisel will reuse the results from the initial
select expression in memory.

```python
with model.begin(enable_work_sharing=True) as session:
    foo = model.schemas['acme'].tables['lot_of_rows'].where(...)
    
    session.create_table_as(
        'acme', 'bar',
        foo.columns['bar'].to_domain()
    )
    
    session.create_table_as(
        'acme', 'baz',
        foo.columns['baz'].to_atoms()
    )
```
