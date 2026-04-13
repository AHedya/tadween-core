import logging
import logging.handlers
import multiprocessing
import tempfile
from pathlib import Path

from tadween_core.logger.formatters import JsonFormatter
from tadween_core.logger.process_queue import ProcessQueueLogger


class TestProcessQueueLogger:
    def _reset_tadween_logger(self):
        logger = logging.getLogger("tadween")
        logger.handlers.clear()
        logger.addHandler(logging.NullHandler())

    def test_default_creates_queue_handler(self):
        pql = ProcessQueueLogger()
        logger = pql.logger
        assert logger.name == "tadween"
        queue_handlers = [
            h for h in logger.handlers if isinstance(h, logging.handlers.QueueHandler)
        ]
        assert len(queue_handlers) == 1
        pql.close()

    def test_custom_level(self):
        with ProcessQueueLogger(level=logging.DEBUG) as pql:
            assert pql.logger.level == logging.DEBUG

    def test_file_handler_in_listener(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "pql.log")
            with ProcessQueueLogger(log_path=log_path) as pql:
                pql.logger.info("from process queue logger")
            content = Path(log_path).read_text()
            assert "from process queue logger" in content

    def test_custom_file_formatter(self):
        json_fmt = JsonFormatter()
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "pql.json.log")
            with ProcessQueueLogger(log_path=log_path, file_formatter=json_fmt) as pql:
                pql.logger.info("json process msg")
            content = Path(log_path).read_text()
            assert '"message": "json process msg"' in content

    def test_custom_console_formatter(self):
        custom_fmt = logging.Formatter("CUSTOM: %(message)s")
        with ProcessQueueLogger(console_formatter=custom_fmt) as pql:
            _ = pql.logger

    def test_log_queue_property_returns_multiprocessing_queue(self):
        with ProcessQueueLogger() as pql:
            q = pql.log_queue
            assert isinstance(q, multiprocessing.queues.Queue)

    def test_custom_log_queue(self):
        mp_q = multiprocessing.Queue()
        with ProcessQueueLogger(log_queue=mp_q) as pql:
            assert pql.log_queue is mp_q

    def test_mp_context_creates_queue(self):
        ctx = multiprocessing.get_context("spawn")
        with ProcessQueueLogger(mp_context=ctx) as pql:
            q = pql.log_queue
            assert isinstance(q, multiprocessing.queues.Queue)

    def test_mp_context_and_log_queue_mutually_exclusive(self):
        import pytest

        mp_q = multiprocessing.Queue()
        ctx = multiprocessing.get_context("spawn")
        with pytest.raises(ValueError, match="mutually exclusive"):
            ProcessQueueLogger(mp_context=ctx, log_queue=mp_q)

    def test_context_manager_closes_listener(self):
        pql = ProcessQueueLogger()
        _ = pql.logger
        assert pql._listener is not None
        pql.__exit__(None, None, None)
        assert pql._closed is True

    def test_close_is_idempotent(self):
        pql = ProcessQueueLogger()
        _ = pql.logger
        pql.close()
        pql.close()
        assert pql._closed is True

    def test_close_without_configure_is_noop(self):
        pql = ProcessQueueLogger()
        pql.close()
        assert pql._closed is True

    def test_creates_parent_dirs_for_log_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "subdir" / "nested" / "pql.log")
            with ProcessQueueLogger(log_path=log_path) as pql:
                pql.logger.info("nested path test")
            assert Path(log_path).parent.exists()

    def test_logger_property_idempotent(self):
        with ProcessQueueLogger() as pql:
            logger1 = pql.logger
            logger2 = pql.logger
            assert logger1 is logger2

    def test_reconfigure_clears_old_handlers(self):
        with ProcessQueueLogger() as pql1:
            _ = pql1.logger
        self._reset_tadween_logger()
        with ProcessQueueLogger() as pql2:
            logger = pql2.logger
            queue_handlers = [
                h
                for h in logger.handlers
                if isinstance(h, logging.handlers.QueueHandler)
            ]
            assert len(queue_handlers) == 1

    def test_default_file_formatter_is_standard(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "test_default.log")
            with ProcessQueueLogger(log_path=log_path) as pql:
                pql.logger.info("default formatter test")
            content = Path(log_path).read_text()
            assert "default formatter test" in content
            assert content[0].isdigit() or content.startswith("2")
