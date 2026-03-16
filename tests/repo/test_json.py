import pytest

from .test_contract import RepoContract


class TestFsJsonRepo(RepoContract):
    @pytest.fixture
    def repo(self, json_repo):
        return json_repo
