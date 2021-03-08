# Schema Evolution Session Manager

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
because it is bound to a model-wide _session_ object rather than to a schema 
object.

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

In order to do a "dry run," call the `begin` method with `dry_run=True` and at 
the exit of the block the plan and sample data of computed relations will be 
dumped to the `debug` logger. No changes to the model will be executed
and the catalog model will be unaltered.

```python
with model.begin(dry_run=True):
    # catalog model mutation operations...
    ...
```
