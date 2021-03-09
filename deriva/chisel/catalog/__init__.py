"""Catalog model package.
"""
from deriva.core.ermrest_model import Type, builtin_types
from .model import Schema, Table, Column, Key, ForeignKey
from .ext import Model
