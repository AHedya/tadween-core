from typing import reveal_type  # noqa

from pydantic import BaseModel, ValidationError  # noqa
from dataclasses import dataclass  # noqa
from tadween_core.cache import Cache
from pprint import pprint  # noqa


# define your schema
class User(BaseModel):
    name: str
    age: int


## or use dataclass
# @dataclass
# class User:
#     name: str
#     age: int


# create your cache instance, or use singleton interface
cache = Cache(User)

# None, as cache is fresh
user = cache.get_bucket("id")
assert user is None

# You can create empty bucket and fill it as you want.
# This can skip the overhead of creating full instance if your schema doesn't allow optional fields.
user = cache.get_or_create("id-1")
user.age = 20
print(user)

# Watch our converting partial bucket back to an instance:
try:
    original_user = user.to_instance()
    original_user.model_dump()
except (ValidationError, AttributeError):
    print("your model bucket isn't complete.")

# complete your schema
user.name = "Mohammed"
original_user = user.to_instance()
print(original_user.model_dump())

# now let's test fields access
user.age  # noqa
user.age  # noqa
user.age  # noqa

user.name  # noqa

user_internals = cache.get_raw_internal("id-1")
pprint(user_internals)
