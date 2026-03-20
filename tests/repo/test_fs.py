import pytest

from .test_contract import RepoContract


class TestFsRepo(RepoContract):
    @pytest.fixture
    def repo(self, fs_repo):
        return fs_repo
