import sys

from my_artifact import (
    AudioArtifact,
    part_names,
    run_example,
)

from tadween_core.exceptions import S3InitError


def setup():
    import boto3
    from botocore.client import Config

    from tadween_core.repo.s3 import S3Repo

    # override with any S3 provider config
    config = {
        "endpoint_url": "http://localhost:9000",
        "aws_access_key_id": "rustfsadmin",
        "aws_secret_access_key": "rustfsadmin",
        "region_name": "us-east-1",
        "config": Config(signature_version="s3v4"),
    }
    client = boto3.client("s3", **config)
    BUCKET_NAME = "example-bucket"
    BATCH = "my-batch2"
    client.create_bucket(Bucket=BUCKET_NAME)

    repo = S3Repo[AudioArtifact, part_names](
        artifact_type=AudioArtifact,
        bucket_id=BUCKET_NAME,
        prefix=BATCH,
        boto_client=client,
    )

    return repo


def main():
    try:
        repo = setup()
    except ImportError:
        print("tadween-core[s3] isn't installed. Quit")
        sys.exit(0)
    except S3InitError as e:
        print(f"Failed initializing S3 client: {e}.")
        sys.exit(0)

    run_example(repo)


if __name__ == "__main__":
    main()
