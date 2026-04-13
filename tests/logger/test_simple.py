import logging
import tempfile
from pathlib import Path

from tadween_core.logger.formatters import JsonFormatter, NoStackTraceFormatter
from tadween_core.logger.simple import StandardLogger, set_logger


class TestStandardLogger:
    def _reset_tadween_logger(self):
        logger = logging.getLogger("tadween")
        logger.handlers.clear()
        logger.addHandler(logging.NullHandler())

    def test_default_creates_console_handler(self):
        sl = StandardLogger()
        logger = sl.logger
        assert logger.name == "tadween"
        assert logger.level == logging.INFO
        stream_handlers = [
            h for h in logger.handlers if isinstance(h, logging.StreamHandler)
        ]
        assert len(stream_handlers) >= 1

    def test_custom_level(self):
        sl = StandardLogger(level=logging.DEBUG)
        logger = sl.logger
        assert logger.level == logging.DEBUG

    def test_file_handler_created(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "test.log")
            sl = StandardLogger(log_path=log_path)
            logger = sl.logger
            file_handlers = [
                h for h in logger.handlers if isinstance(h, logging.FileHandler)
            ]
            assert len(file_handlers) == 1

    def test_default_console_formatter_is_no_stack_trace(self):
        sl = StandardLogger()
        _ = sl.logger
        stream_handlers = [
            h for h in sl.logger.handlers if isinstance(h, logging.StreamHandler)
        ]
        assert any(
            isinstance(h.formatter, NoStackTraceFormatter) for h in stream_handlers
        )

    def test_custom_console_formatter(self):
        custom_fmt = logging.Formatter("CUSTOM: %(message)s")
        sl = StandardLogger(console_formatter=custom_fmt)
        _ = sl.logger
        stream_handlers = [
            h for h in sl.logger.handlers if isinstance(h, logging.StreamHandler)
        ]
        assert any(h.formatter is custom_fmt for h in stream_handlers)

    def test_custom_file_formatter(self):
        json_fmt = JsonFormatter()
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "test.json.log")
            sl = StandardLogger(log_path=log_path, file_formatter=json_fmt)
            sl.logger.info("json test")
            for handler in sl.logger.handlers:
                if isinstance(handler, logging.FileHandler):
                    handler.flush()
            content = Path(log_path).read_text()
            assert '"message": "json test"' in content

    def test_default_file_formatter_is_standard(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "test.log")
            sl = StandardLogger(log_path=log_path)
            _ = sl.logger
            file_handlers = [
                h for h in sl.logger.handlers if isinstance(h, logging.FileHandler)
            ]
            assert len(file_handlers) == 1
            assert isinstance(file_handlers[0].formatter, logging.Formatter)
            assert not isinstance(file_handlers[0].formatter, JsonFormatter)

    def test_logger_property_is_idempotent(self):
        sl = StandardLogger()
        logger1 = sl.logger
        logger2 = sl.logger
        assert logger1 is logger2

    def test_context_manager(self):
        sl = StandardLogger()
        with sl:
            assert sl.logger.name == "tadween"

    def test_reconfigure_clears_old_handlers(self):
        sl1 = StandardLogger()
        _ = sl1.logger
        sl2 = StandardLogger()
        logger = sl2.logger
        stream_handlers = [
            h for h in logger.handlers if isinstance(h, logging.StreamHandler)
        ]
        assert len(stream_handlers) == 1

    def test_creates_parent_dirs_for_log_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "subdir" / "nested" / "test.log")
            sl = StandardLogger(log_path=log_path)
            _ = sl.logger
            assert Path(log_path).parent.exists()

    def test_writes_to_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "test.log")
            sl = StandardLogger(log_path=log_path)
            sl.logger.info("hello from standard")
            for handler in sl.logger.handlers:
                if isinstance(handler, logging.FileHandler):
                    handler.flush()
            content = Path(log_path).read_text()
            assert "hello from standard" in content


class TestSetLoggerBackwardCompat:
    def _reset_tadween_logger(self):
        logger = logging.getLogger("tadween")
        logger.handlers.clear()
        logger.addHandler(logging.NullHandler())

    def test_returns_logger(self):
        logger = set_logger()
        assert isinstance(logger, logging.Logger)
        assert logger.name == "tadween"

    def test_set_logger_with_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "compat.log")
            logger = set_logger(log_path=log_path)
            assert isinstance(logger, logging.Logger)
            file_handlers = [
                h for h in logger.handlers if isinstance(h, logging.FileHandler)
            ]
            assert len(file_handlers) == 1

    def test_set_logger_default_level(self):
        logger = set_logger()
        assert logger.level == logging.INFO

    def test_set_logger_with_formatters(self):
        json_fmt = JsonFormatter()
        with tempfile.TemporaryDirectory() as tmp:
            log_path = str(Path(tmp) / "compat.json.log")
            logger = set_logger(log_path=log_path, file_formatter=json_fmt)
            file_handlers = [
                h for h in logger.handlers if isinstance(h, logging.FileHandler)
            ]
            assert len(file_handlers) == 1
            assert isinstance(file_handlers[0].formatter, JsonFormatter)
