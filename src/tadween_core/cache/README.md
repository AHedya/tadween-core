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
- **CacheEntry**: A runtime schema fields wrapper. It encapsulates actual field/attribute data with some caching related metadata (number of read, remaining reads, size), and functionalities (touch, update)
- **CachePolicy**: Defines size limits, TTL, and eviction strategies.

## Anatomy

```
└── cache
    ├── __init__.py 
    ├── adapter.py  => Defines `SchemaAdapter` contract, implements `DataclassAdapter`, `PydanticAdapter` and adapter factory
    ├── base.py     => Intended for shared types. Only defines `CacheEntry`
    ├── cache.py    => Implements the generic `Cache`, and eviction logic
    ├── policy.py   => Defies policy data model. Not the actual policy behavior
    ├── proxy.py    => Defines `BucketProxy`
    └── README.md
```

## The Proxy & "Mirage Effect"

The cache returns `BucketProxy[MySchema] | MySchema`. This serves as a warning that you are holding a **proxy**, not a direct instance.

- **Ergonomics**: Provides full type-hints for schema fields and proxy methods.
- **Lazy Loading**: Accessing a field via the proxy tracks its metadata and read counts.
- **Honesty**: If you need a true instance (e.g., for Pydantic validation), call `proxy.to_instance()`.


## Usage Example

*For more examples, see [examples/cache.py](../../../examples/cache/README.md).*

## Eviction Strategies

Supports various strategies to manage cache size:
- **LRU (Least Recently Used)**: Evict the least recently accessed bucket or entry.
- **LFU (Least Frequently Used)**: Evict the least frequently accessed bucket or entry.
- **FIFO (First-In, First-Out)**: Evict the oldest bucket or entry.
- **Read Quota**: Evict buckets or entries after they have been read a certain number of times.