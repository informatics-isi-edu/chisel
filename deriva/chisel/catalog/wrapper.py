"""Wrappers for containers and model objects.
"""
from collections.abc import Iterator, Mapping, Sequence


class IteratorWrapper (Iterator):
    """Provides wrapped objects from an underlying iterator.
    """
    def __init__(self, item_wrapper, iterator):
        """Initializes the wrapped iterator.

        :param item_wrapper: wrapper to apply to objects of the mapping
        :param iterator: original iterator
        """
        self._item_wrapper = item_wrapper
        self._iterator = iterator

    def __next__(self):
        return self._item_wrapper(next(self._iterator))


class MappingWrapper (Mapping):
    """Provides wrapped objects from an underlying mapping.
    """
    def __init__(self, item_wrapper, mapping):
        """Initializes the wrapped mapping.

        :param item_wrapper: wrapper to apply to objects of the mapping
        :param mapping: original mapping
        """
        self._item_wrapper = item_wrapper
        self._mapping = mapping

    def __getitem__(self, key):
        return self._item_wrapper(self._mapping[key])

    def __iter__(self):
        return iter(self._mapping)

    def __len__(self):
        return len(self._mapping)

    def __eq__(self, other):
        return self._mapping == other._mapping if isinstance(other, MappingWrapper) else False


class SequenceWrapper (Sequence):
    """Provides wrapped objects from an underlying sequence.
    """
    def __init__(self, item_wrapper, sequence):
        """Initializes the wrapped sequence.

        :param item_wrapper: wrapper to apply to objects of the sequence
        :param sequence: original sequence
        """
        self._item_wrapper = item_wrapper
        self._sequence = sequence

    def __getitem__(self, key):
        """Get element by key or by list index or slice."""
        if isinstance(key, slice):
            return SequenceWrapper(self._item_wrapper, self._sequence[key])
        elif isinstance(key, tuple):
            return self._item_wrapper(self._sequence[tuple(
                elem._wrapped_obj if isinstance(elem, ModelObjectWrapper) else elem
                for elem in key
            )])
        else:
            return self._item_wrapper(self._sequence[key])

    def __len__(self):
        return len(self._sequence)

    def __eq__(self, other):
        return self._sequence == other._sequence if isinstance(other, SequenceWrapper) else False

    def __contains__(self, item):
        if isinstance(item, ModelObjectWrapper):
            item = item._wrapped_obj
        return item in self._sequence


class ModelObjectWrapper (object):
    """Generic wrapper for an ermrest_model object.
    """
    def __init__(self, obj):
        """Initializes the wrapper.

        :param obj: the underlying ermrest_model object instance.
        """
        super(ModelObjectWrapper, self).__init__()
        self._wrapped_obj = obj

        # patch this wrapper object with attributes from the wrapped object
        for attr_name in ['acls', 'acl_bindings', 'annotations', 'alter', 'apply', 'clear', 'drop', 'prejson', 'names', 'constraint_name']:
            if not hasattr(self, attr_name) and hasattr(obj, attr_name):
                setattr(self, attr_name, getattr(obj, attr_name))

    def __repr__(self):
        return super(ModelObjectWrapper, self).__repr__() + f' named "{self.name}"'

    def __eq__(self, other):
        return self._wrapped_obj == other._wrapped_obj if isinstance(other, ModelObjectWrapper) else False

    @property
    def name(self):
        return self._wrapped_obj.name

    @property
    def comment(self):
        return self._wrapped_obj.comment

    @comment.setter
    def comment(self, value):
        self._wrapped_obj.comment = value
