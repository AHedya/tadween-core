import json as json_module
import logging


class NoStackTraceFormatter(logging.Formatter):
    def formatException(self, ei):
        """Return an empty string to suppress the stack trace."""
        return ""

    def formatStack(self, stack_info):
        """Also suppress stack info if present."""
        return ""


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": self.formatTime(record, self.datefmt or "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            log_entry["stack_info"] = self.formatStack(record.stack_info)
        return json_module.dumps(log_entry, default=str)
