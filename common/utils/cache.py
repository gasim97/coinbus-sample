from typing import Any
from common.type.lrucache import LRUCache


def lru_cache(capacity: int = 10):
    cache = LRUCache[Any, Any](capacity=capacity)
    def decorator(func):
        def wrapper(*args, **kwargs):
            key = (*args, *kwargs)
            if cache.contains(key=key):
                return cache.get(key=key)
            return cache.put(key=key, value=func(*args, **kwargs))
        return wrapper
    return decorator