## PLANNED

These operations are not implemented yet.

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
