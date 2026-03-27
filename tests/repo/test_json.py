import pytest

from .test_contract import RepoContract


class TestFsJsonRepo(RepoContract):
    @pytest.fixture
    def repo(self, json_repo):
        return json_repo

    @pytest.fixture
    def full_pickle_artifact(self):
        pytest.skip(
            "FsJsonRepo requires JSON-serializable parts; PicklePart uses binary"
        )
