import pytest

from .test_contract import RepoContract


class TestSqliteRepo(RepoContract):
    @pytest.fixture
    def repo(self, sqlite_repo):
        return sqlite_repo
