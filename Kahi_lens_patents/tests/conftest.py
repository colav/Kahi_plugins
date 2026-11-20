import functools

import mongomock.collection


def _drop_sort(method):
    """pymongo 4.x passes ``sort`` to bulk updates; mongomock does not accept it."""
    @functools.wraps(method)
    def wrapper(self, *args, sort=None, **kwargs):
        return method(self, *args, **kwargs)
    return wrapper


mongomock.collection.BulkOperationBuilder.add_update = _drop_sort(
    mongomock.collection.BulkOperationBuilder.add_update
)
