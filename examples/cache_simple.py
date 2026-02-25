from pydantic import BaseModel

from tadween_core.cache import Cache, CachePolicy


# Define a simple schema for cached data
class AudioMetadata(BaseModel):
    id: str
    duration: float
    format: str


def main():
    # Initialize the cache with a policy (TTL of 60 seconds)
    policy = CachePolicy(max_buckets=10, entry_ttl=60)
    cache = Cache(AudioMetadata, policy=policy)

    # Store a bucket in the cache
    print("[Cache] Setting a new bucket for 'audio-1'...")
    metadata = AudioMetadata(id="audio-1", duration=120.5, format="mp3")
    cache.set_bucket("audio-1", metadata)

    # Retrieve the bucket via a proxy (type-hinted)
    print("[Cache] Getting bucket 'audio-1'...")
    proxy = cache.get_bucket("audio-1")
    if proxy:
        print("\tRetrieved Metadata (Proxy):")
        print(f"\tID: {proxy.id}")
        print(f"\tDuration: {proxy.duration}")
        print(f"\tFormat: {proxy.format}")

    #  Atomic updates via the proxy
    print("[Cache] Updating duration to 130.0 via the proxy...")
    proxy.duration = 130.0

    # Verify the update (direct cache access)
    updated_proxy = cache.get_bucket("audio-1")
    print(f"\tUpdated Duration: {updated_proxy.duration}")

    #  Accessing a non-existent bucket
    print("[Cache] Getting non-existent bucket 'audio-2'...")
    not_found = cache.get_bucket("audio-2")
    if not_found is None:
        print("\tBucket 'audio-2' not found in cache.")


if __name__ == "__main__":
    main()
