import json
import logging
import sys

from tadween_core.logger.formatters import JsonFormatter, NoStackTraceFormatter


class TestNoStackTraceFormatter:
    def test_suppresses_exception_traceback(self):
        formatter = NoStackTraceFormatter()
        try:
            raise ValueError("boom")
        except ValueError:
            record = logging.LogRecord("test", logging.ERROR, "", 0, "msg", [], None)
            output = formatter.formatException(record.exc_info)
            assert output == ""

    def test_format_stack_returns_empty(self):
        formatter = NoStackTraceFormatter()
        assert formatter.formatStack("some stack info") == ""

    def test_normal_formatting_still_works(self):
        formatter = NoStackTraceFormatter("%(message)s")
        record = logging.LogRecord("test", logging.INFO, "", 0, "hello", None, None)
        assert formatter.format(record) == "hello"


class TestJsonFormatter:
    def test_basic_json_output(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            "test.logger", logging.WARNING, "", 0, "watch out", None, None
        )
        output = formatter.format(record)
        data = json.loads(output)
        assert data["message"] == "watch out"
        assert data["level"] == "WARNING"
        assert data["logger"] == "test.logger"
        assert "timestamp" in data

    def test_custom_datefmt(self):
        formatter = JsonFormatter(datefmt="%Y")
        record = logging.LogRecord("test", logging.INFO, "", 0, "msg", None, None)
        output = formatter.format(record)
        data = json.loads(output)
        assert len(data["timestamp"]) == 4

    def test_exc_info_included(self):
        formatter = JsonFormatter()
        try:
            raise RuntimeError("oops")
        except RuntimeError:
            ei = sys.exc_info()
            record = logging.LogRecord("test", logging.ERROR, "", 0, "err", [], None)
            record.exc_info = ei
            output = formatter.format(record)
            data = json.loads(output)
            assert "exc_info" in data
            assert "RuntimeError" in data["exc_info"]

    def test_stack_info_included(self):
        formatter = JsonFormatter()
        record = logging.LogRecord("test", logging.INFO, "", 0, "msg", None, None)
        record.stack_info = "some stack trace"
        output = formatter.format(record)
        data = json.loads(output)
        assert "stack_info" in data
        assert data["stack_info"] == "some stack trace"

    def test_no_exc_info_key_when_none(self):
        formatter = JsonFormatter()
        record = logging.LogRecord("test", logging.INFO, "", 0, "msg", None, None)
        output = formatter.format(record)
        data = json.loads(output)
        assert "exc_info" not in data
        assert "stack_info" not in data
