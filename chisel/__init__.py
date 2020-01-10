"""CHiSEL: A high-level, user-oriented framework for schema evolution.

This package provides the following top-level interfaces:
  connect, shred, csv_reader, json_reader

Most schema evolution scripts will begin with `chisel.connect(...)`. To learn
more about any of these functions use the builtin Python `help` function. For
example:

>>> help(chisel.connect)
"""

from .catalog import connect, shred, CatalogMutationError, data_types, Schema, Table, Column, Key, ForeignKey
from .catalog.semistructured import csv_reader, json_reader

__version__ = "0.0.6"
