import pytest

from .test_contract import RepoContract


@pytest.mark.xfail(
    reason="moto.s3.urls *sometimes* fails in free-threaded build", raises=ImportError
)
class TestS3Repo(RepoContract):
    @pytest.fixture
    def repo(self, s3_repo):
        return s3_repo

    def test_concurrent_writes(self):
        pytest.skip("moto is not thread-safe.")
