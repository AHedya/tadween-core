# Cache

A type-safe, dynamic, two-layer caching system for storing schema instances (Pydantic models or dataclasses) in Tadween.
First layer caching: key -> bucket (or object) layer. Returned type from this layer is a `BucketProxy` that ensures having data fields of original type.
Second layer caching: attribute -> CacheEntry.

***Data integrity Warning***
Pydantic models are validated by fields not as a model. This is because partial schema fields (CacheEntry) might be evicted due to policy.

## Concepts

- **Schema**: A data model defining the structure of cached data.
- **Adapters**: Adapter to transform schema into cached model. Supports `pydantic.BaseModel`, and `dataclasses.dataclass`
- **Bucket**: A cached instance of a schema, accessible by a unique key.
- **BucketProxy**: A runtime proxy that mimics the schema type while managing metadata and eviction.
- **CacheEntry**: A runtime schema fields wrapper. It encapsulates actual field/attribute data with some caching related metadata (number of read, remaining reads, size), and functionalities (touch, update, evict).
- **CachePolicy**: Defines size limits, TTL, and eviction strategies.

## Anatomy

```
└── cache
    ├── __init__.py 
    ├── adapter.py  => Defines `SchemaAdapter` contract, implements `DataclassAdapter`, `PydanticAdapter` and adapter factory
    ├── base.py     => Defines `BaseCache` interface and `CacheEntry` for `Cache` implementation
    ├── cache.py    => Implements the generic `Cache`, and eviction logic
    ├── policy.py   => Defines policy data model. Not the actual policy behavior
    ├── proxy.py    => Defines `BucketProxy`
    ├── README.md
    └── simple_cache.py  => Implements `SimpleCache` - thread-safe, no-eviction cache
```

## Usage Example

*see [examples/cache.py](../../../examples/cache/README.md).*

## The Proxy & "Mirage Effect"

The cache returns `BucketProxy[MySchema] | MySchema`. This serves as a warning that you are holding a **proxy**, not a direct instance.

- **Ergonomics**: Provides full type-hints for schema fields and proxy methods.
- **Lazy Loading**: Accessing a field via the proxy tracks its metadata and read counts.
- **Manual Eviction**: You can immediately free memory by calling `del proxy.field` or `proxy.evict("field_name")`.
- **Honesty**: If you need a true instance (e.g., for Pydantic validation), call `proxy.to_instance()`.

## Manual Eviction & Deletion

The system supports immediate, on-demand eviction to free memory without waiting for policy triggers:
- **Bucket Level**: `cache.delete_bucket(key)` or `cache.evict_bucket(key)`.
- **Entry Level (via Cache)**: `cache.evict_entry(key, "field_name")`.
- **Entry Level (via Proxy)**: `del proxy.field_name` or `proxy.evict("field_name")`.

## Eviction Strategies

Supports various strategies to manage cache size:
- **LRU (Least Recently Used)**: Evict the least recently accessed bucket or entry.
- **LFU (Least Frequently Used)**: Evict the least frequently accessed bucket or entry.
- **FIFO (First-In, First-Out)**: Evict the oldest bucket or entry.
- **Read Quota**: Evict buckets or entries after they have been read a certain number of times. 
  ***Immediate Eviction***: When the read quota reaches zero, the memory is freed *during* that final read attempt. Subsequent reads return `None`.

## Thread-Safety Warning

**The `Cache` class is not thread-safe.** Concurrent access from multiple threads can lead to race conditions, corrupted state, and undefined behavior.

Limitations:
- Eviction is unsafe under concurrent access
- `CacheEntry.touch()` modifies state on every read without synchronization
- `_bucket_sizes` and `_bucket_last_accessed` are shared across all buckets and not protected

Recommendations:
- Use `SimpleCache` for thread-safe scenarios (recommended)
- Avoid setting size limits (`max_buckets`, `max_bucket_size`) in concurrent workflows
- Design workflows to have **one active `BucketProxy` at a time** per cache
- If sharing cache across threads, ensure exclusive bucket access (different keys per thread)


## SimpleCache

A lightweight, thread-safe alternative to `Cache` for scenarios that don't need eviction, policies, or metadata tracking.

**Key Differences from Cache:**

| Feature | Cache | SimpleCache |
|---------|-------|-------------|
| Thread-safe | No | Yes (Lock) |
| Eviction | auto if configured | manual |
| partial bucket | Yes | No |
| Metadata tracking | Yes (read counts, TTL, access times) | None |
| Return type | `BucketProxy` (with tracking) | Raw schema instance |
| Defensive copying | Yes (returns copies) | No (returns stored instance) |
| Policies | Yes | No |

**When to use SimpleCache:**
- Thread-safe access is required (For now)
- No eviction or size limits needed
- Simpler key-value storage with type safety
- Want to avoid proxy overhead

**When to use Cache:**
- Bucket sharding where partial model with field scoped validation is required.
- Need field-level metadata (read counts, access times)
- Need size limits per entry/bucket
- Need eviction strategies (LRU, LFU, etc.)


**Mutability Warning:**
SimpleCache returns the stored instance directly. Mutating the returned instance affects the cached data:
```python
bucket = cache.get_bucket("key1")
bucket.name = "modified"  # This modifies the cached instance
```
