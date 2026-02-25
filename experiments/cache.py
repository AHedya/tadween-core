from dataclasses import dataclass

from pydantic import BaseModel  # noqa

from tadween_core.cache import Cache


@dataclass
class SimpleSchema:
    field1: str | None = None
    field2: int | None = None


# class SimpleSchema(BaseModel):
#     field1: str | None = None
#     field2: int | None = None


def main():
    cache = Cache(SimpleSchema)
    cache.set_bucket("my_key", SimpleSchema(field1="hello"), 1)
    print(cache.get_raw_internal("my_key"))
    bkt = cache.get_bucket("my_key")
    print(bkt.model_dump())

    instance = bkt.to_instance()
    print(instance)
    print(isinstance(bkt, SimpleSchema))
    print(isinstance(instance, SimpleSchema))
    print(instance.model_dump())


if __name__ == "__main__":
    main()
