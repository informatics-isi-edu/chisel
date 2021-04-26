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

    def __getitem__(self, item):
        return self._item_wrapper(self._mapping[item])

    def __iter__(self):
        return iter(self._mapping)

    def __len__(self):
        return len(self._mapping)


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

    def __getitem__(self, item):
        return self._item_wrapper(self._sequence[item])

    def __len__(self):
        return len(self._sequence)


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

    @property
    def name(self):
        return self._wrapped_obj.name

    @property
    def comment(self):
        return self._wrapped_obj.comment

    @comment.setter
    def comment(self, value):
        self._wrapped_obj.comment = value
