# Cache

A type-safe, dynamic, two-layer caching system for storing schema instances (Pydantic models or dataclasses) in Tadween.

## Concepts

- **Schema**: A Pydantic model defining the structure of cached data.
- **Bucket**: A cached instance of a schema, accessible by a unique key.
- **BucketProxy**: A runtime proxy that mimics the schema type while managing metadata and eviction.
- **CachePolicy**: Defines size limits, TTL, and eviction strategies.

## Eviction Strategies

Supports various strategies to manage cache size:
- **LRU (Least Recently Used)**: Evict the least recently accessed bucket or entry.
- **LFU (Least Frequently Used)**: Evict the least frequently accessed bucket or entry.
- **FIFO (First-In, First-Out)**: Evict the oldest bucket or entry.
- **Read Quota**: Evict buckets or entries after they have been read a certain number of times.

## Usage Example

```python
from pydantic import BaseModel
from tadween_core.cache import Cache, CachePolicy

# 1. Define your schema
class AudioMetadata(BaseModel):
    duration: float
    format: str

# 2. Initialize the cache
policy = CachePolicy(max_buckets=100, entry_ttl=3600)
cache = Cache(AudioMetadata, policy=policy)

# 3. Store and retrieve buckets
metadata = AudioMetadata(duration=120.5, format="wav")
cache.set_bucket("audio-1", metadata)

# 4. Use the proxy (fully type-hinted)
proxy = cache.get_bucket("audio-1")
print(proxy.duration)  # Accesses the cached field

# 5. Atomic field updates
proxy.duration = 125.0  # Updates only the duration field in the cache
```

## The Proxy & "Mirage Effect"

The cache returns `BucketProxy[MySchema] | MySchema`. This serves as a warning that you are holding a **proxy**, not a direct instance.

- **Ergonomics**: Provides full type-hints for schema fields and proxy methods.
- **Lazy Loading**: Accessing a field via the proxy tracks its metadata and read counts.
- **Honesty**: If you need a true instance (e.g., for Pydantic validation), call `proxy.to_instance()`.

---
*For more examples, see [examples/cache.py](../../../examples/cache.py) (if available).*
