import logging
import logging.handlers
import queue
import tempfile
from pathlib import Path

from tadween_core.logger.formatters import JsonFormatter
from tadween_core.logger.queue import QueueLogger


class TestQueueLogger:
    def _reset_tadween_logger(self):
        logger = logging.getLogger("tadween")
        logger.handlers.clear()
        logger.addHandler(logging.NullHandler())

    def test_default_creates_queue_handler(self):
        ql = QueueLogger()
        logger = ql.logger
        assert logger.name == "tadween"
        queue_handlers = [
            h for h in logger.handlers if isinstance(h, logging.handlers.QueueHandler)
        ]
        assert len(queue_handlers) == 1

    def test_custom_level(self):
        with QueueLogger(level=logging.DEBUG) as ql:
            assert ql.logger.level == logging.DEBUG

    def test_file_handler_in_listener(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "queue.log")
            with QueueLogger(log_path=log_path) as ql:
                ql.logger.info("from queue logger")
            content = Path(log_path).read_text()
            assert "from queue logger" in content

    def test_default_console_formatter_is_no_stack_trace(self):
        with QueueLogger() as ql:
            _ = ql.logger

    def test_custom_file_formatter(self):
        json_fmt = JsonFormatter()
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "queue.json.log")
            with QueueLogger(log_path=log_path, file_formatter=json_fmt) as ql:
                ql.logger.info("json queue msg")
            content = Path(log_path).read_text()
            assert '"message": "json queue msg"' in content

    def test_custom_console_formatter(self):
        custom_fmt = logging.Formatter("CUSTOM: %(message)s")
        with QueueLogger(console_formatter=custom_fmt) as ql:
            _ = ql.logger

    def test_log_queue_property_returns_queue(self):
        with QueueLogger() as ql:
            assert isinstance(ql.log_queue, queue.Queue)

    def test_custom_queue(self):
        q = queue.Queue()
        with QueueLogger(log_queue=q) as ql:
            assert ql.log_queue is q

    def test_context_manager_closes_listener(self):
        ql = QueueLogger()
        _ = ql.logger
        assert ql._listener is not None
        ql.__exit__(None, None, None)
        assert ql._closed is True

    def test_close_is_idempotent(self):
        ql = QueueLogger()
        _ = ql.logger
        ql.close()
        ql.close()
        assert ql._closed is True

    def test_close_without_configure_is_noop(self):
        ql = QueueLogger()
        ql.close()
        assert ql._closed is True

    def test_creates_parent_dirs_for_log_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "subdir" / "nested" / "q.log")
            with QueueLogger(log_path=log_path) as ql:
                ql.logger.info("nested path test")
            assert Path(log_path).parent.exists()

    def test_logger_property_idempotent(self):
        with QueueLogger() as ql:
            logger1 = ql.logger
            logger2 = ql.logger
            assert logger1 is logger2

    def test_reconfigure_clears_old_handlers(self):
        with QueueLogger() as ql1:
            _ = ql1.logger
        self._reset_tadween_logger()
        with QueueLogger() as ql2:
            logger = ql2.logger
            queue_handlers = [
                h
                for h in logger.handlers
                if isinstance(h, logging.handlers.QueueHandler)
            ]
            assert len(queue_handlers) == 1

    def test_console_output_does_not_suppress_tracebacks(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "traceback.log")
            with QueueLogger(log_path=log_path) as ql:
                try:
                    raise ValueError("test error")
                except ValueError:
                    ql.logger.exception("error occurred")
            content = Path(log_path).read_text()
            assert "ValueError" in content
            assert "test error" in content
