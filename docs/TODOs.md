# Todos

## Cache
- [ ] Create test suite covers/triggers thread-safety weak points.
- [ ] Fix `Cache` thread-safety weak points:
  - [ ] `CacheEntry.touch`: Requires atomic update.
  - [ ] `Cache._bucket_sizes`: Requires atomic update.
  - [ ] Eviction while iterating: inconsistent stage if deletion happens during looking for eviction candidate
  - [ ] Proxy stale reference: If Cache clears a bucket while the proxy still holds the reference to internal `Cache._store`, this leads to in-consistent clear (no GC due to reference is still hold).

## Task Queue
- [x] Add synchronization to callback test cases. No-GIL builds (3.14t) make callback side-effects tests flaky.

## Library validation & early quit.
- [ ] Add early validation for library component to get better error resolution if happened. For example:
  - [ ] policy decorators being decorating incompatible events

## Retry mechanism
- [ ] design
- [ ] implement
- [ ] test