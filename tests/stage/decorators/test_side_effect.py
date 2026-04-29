import logging
from unittest.mock import MagicMock

from tadween_core.stage.decorators import side_effect
from tadween_core.stage.policy import DefaultStagePolicy


def test_side_effect_suppresses_exception(caplog):
    class TestPolicy(DefaultStagePolicy):
        def __init__(self):
            self.logger = logging.getLogger("test_policy")

        @side_effect
        def on_success(self, *args, **kwargs):
            raise RuntimeError("Failing side effect")

    policy = TestPolicy()

    with caplog.at_level(logging.WARNING):
        # This should not raise RuntimeError
        result = policy.on_success(MagicMock(), MagicMock(), MagicMock())

    assert result is None
    assert (
        "Non-critical side-effect failed in on_success: Failing side effect"
        in caplog.text
    )


def test_side_effect_returns_value_on_success():
    class TestPolicy(DefaultStagePolicy):
        @side_effect
        def some_method(self):
            return "success"

    policy = TestPolicy()
    assert policy.some_method() == "success"


def test_side_effect_uses_default_logger(caplog):

    class NoLoggerPolicy:
        @side_effect
        def failing_method(self):
            raise ValueError("No logger error")

    policy = NoLoggerPolicy()
    with caplog.at_level(logging.WARNING):
        policy.failing_method()

    assert "tadween.stage.decorators" in caplog.text
    assert "No logger error" in caplog.text
