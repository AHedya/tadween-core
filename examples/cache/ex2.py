from dataclasses import dataclass
from pprint import pprint  # noqa

from tadween_core.cache import Cache, CachePolicy


@dataclass
class User:
    name: str
    age: int


cache = Cache(User, policy=CachePolicy())

if cache.set_bucket(key="id-1", bucket=User(name="Abdulrahman Hedya", age=24), quota=2):
    print("stored successfully")
else:
    print("something went wrong")
    exit()

user1 = cache.get_bucket("id-1")
print(user1)
assert user1.age == 24
assert user1.age == 24
# evicted after two reads; on the third one. N+1
assert user1.age is None

# check internals
pprint(cache.get_raw_internal("id-1"))
