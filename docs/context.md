# Schema Evolution Context Manager
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

### When not using a catalog evolution context

If not using the above `catalog.evolve()` context, catalog mutation will be processed 
immediately (somewhat akin to "autocommit" mode in a database). The catalog object
has properties `allow_alter_default` and `allow_drop_default` (defaults `True`) that
are passed to an internal catalog `evolve(...)` method when operations are performed 
without first establishing an explicit evolve block. These defaults may be changed in
order to prevent `alter` and `drop` operations from being performed on the catalog 
model objects.

### Testing with `dry_run` flag

In order to do a "dry run," call the evolve method with `dry_run=True` and at 
the exit of the evolve block the plan and sample data of computed relations
will be dumped to standard output. No changes to the catalog will be executed
and the catalog model will be restored to its original state.

```python
with catalog.evolve(dry_run=True):
    # catalog model mutation operations...
```

This will dump diagnostic information to the standard output stream.

### Guarding with `allow_alter` and `allow_drop`

In order to guard against accidental table alteration or destruction, the 
`evolve` method accepts `allow_alter` and `allow_drop` Boolean parameters. 
By default, these parameters are `False` and the evolve block will prevent
table alter or drop operations, respectively.



