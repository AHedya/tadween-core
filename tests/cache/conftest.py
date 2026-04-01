import pytest

from tadween_core.cache.cache import Cache
from tadween_core.cache.policy import CachePolicy
from tadween_core.cache.simple_cache import SimpleCache

from .shared import SimpleSchema


@pytest.fixture
def cache():
    c = Cache(SimpleSchema)
    yield c
    c.clear()


@pytest.fixture
def sized_cache():
    policy = CachePolicy(max_buckets=5, eviction_strategy="lru")
    c = Cache(SimpleSchema, policy=policy)
    yield c
    c.clear()


@pytest.fixture
def bucket_sized_cache():
    policy = CachePolicy(max_bucket_size=500, eviction_strategy="lru")
    c = Cache(SimpleSchema, policy=policy)
    yield c
    c.clear()


@pytest.fixture
def simple_cache():
    c = SimpleCache(SimpleSchema)
    yield c
    c.clear()
