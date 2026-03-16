import pytest

from .test_contract import RepoContract


class TestS3Repo(RepoContract):
    @pytest.fixture
    def repo(self, s3_repo):
        return s3_repo
