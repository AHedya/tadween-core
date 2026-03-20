import random
import shutil
import tempfile
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from tadween_core.repo import SqliteRepo
from tadween_core.repo.fs import FsRepo
from tadween_core.repo.json import FsJsonRepo
from tadween_core.repo.s3 import S3Repo

from .shared_types import (
    ArtifactRoot,
    ArtifactTest,
    ArtifactTestMetadata,
    ArtifactTestPart,
    AudioPart,
    part_names,
)

S3_TEST_BUCKET = "test-bucket"
S3_TEST_PREFIX = "test-prefix"


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=S3_TEST_BUCKET)
        yield client


@pytest.fixture
def s3_repo(s3_client):
    return S3Repo[ArtifactTest, part_names](
        artifact_type=ArtifactTest,
        bucket_id=S3_TEST_BUCKET,
        prefix=S3_TEST_PREFIX,
        boto_client=s3_client,
    )


@pytest.fixture
def json_repo():
    temp_dir = tempfile.mkdtemp()
    store = FsJsonRepo[ArtifactTest, part_names](
        Path(temp_dir), artifact_type=ArtifactTest
    )
    yield store
    shutil.rmtree(temp_dir)


@pytest.fixture
def fs_repo():
    temp_dir = tempfile.mkdtemp()
    store = FsRepo[ArtifactTest, part_names](Path(temp_dir), artifact_type=ArtifactTest)
    yield store
    shutil.rmtree(temp_dir)


@pytest.fixture
def sqlite_repo(tmp_path):
    db_file = tmp_path / "test_sqlite_repo.db"
    return SqliteRepo[ArtifactTest, part_names](db_file, artifact_type=ArtifactTest)


@pytest.fixture
def artifact_metadata() -> ArtifactTestMetadata:
    return ArtifactTestMetadata(
        checksum="random checksum",
        file_path=Path("/home/random_path"),
        duration=random.randint(0, 20) + random.random(),
    )


@pytest.fixture
def artifact_root() -> ArtifactRoot:
    return ArtifactRoot(
        stage=f"stage-{random.randint(1, 5)}",
    )


@pytest.fixture
def partial_artifact() -> ArtifactTest:
    return ArtifactTest(
        root=ArtifactRoot(
            stage=f"stage-{random.randint(1, 5)}",
        ),
        metadata=ArtifactTestMetadata(
            checksum="partial artifact metadata",
            file_path=Path("/home/random_path"),
            duration=random.randint(0, 20) + random.random(),
        ),
        part_a=ArtifactTestPart(),
    )


@pytest.fixture
def full_artifact() -> ArtifactTest:
    return ArtifactTest(
        root=ArtifactRoot(
            stage=f"stage-{random.randint(1, 5)}",
        ),
        metadata=ArtifactTestMetadata(
            checksum="full artifact metadata",
            file_path=Path("/home/random_path"),
            duration=random.randint(0, 20) + random.random(),
        ),
        part_a=ArtifactTestPart(),
        part_b=ArtifactTestPart(),
        audio=AudioPart(),
    )
