"""Logging for the migration: one console stream and one log file per run.

Rules: every line carries the run id, table and window; passwords are masked if they ever reach a
message; ROW VALUES ARE NEVER LOGGED (employee_information holds passwords) - log counts and names only.
"""
import contextvars
import logging
import os
import re
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent      # the project folder
LOG_DIR = Path(os.getenv("PG_MIGRATION_LOG_DIR", BASE_DIR / "logs"))

LOGGER_NAME = "migration"

_run_id = contextvars.ContextVar("run_id", default="-")
_table = contextvars.ContextVar("table", default="-")
_window = contextvars.ContextVar("window", default="-")

# A braced value may contain '}}' (an escaped closing brace), so match brace-pairs inside it.
_SECRET = re.compile(r"(?i)\b(pwd|password|passwd|upassword)\b(\s*[=:]\s*)(\{(?:[^}]|\}\})*\}|\S+)")


class ContextFilter(logging.Filter):
    """Adds run id, table and window to every record, and masks anything that looks like a password."""

    def filter(self, record : logging.LogRecord) -> bool:
        record.run_id = _run_id.get()
        record.table = _table.get()
        record.window = _window.get()
        # The same record goes through the console and the file handler: mask it only once,
        # otherwise the second pass would treat the first mask and the text after it as one password.
        if not getattr(record, "_masked", False):
            record.msg = _SECRET.sub(r"\1\2***", str(record.getMessage()))
            record.args = ()
            record._masked = True
        return True


FORMAT = "%(asctime)s %(levelname)-7s run=%(run_id)s table=%(table)s window=%(window)s %(message)s"


def setup_logging(run_id : str , log_dir : Path = LOG_DIR , console_level : int = logging.INFO) -> logging.Logger:
    """Create the console and file handlers for one run. Safe to call again: old handlers are replaced."""
    _run_id.set(run_id)

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter(FORMAT)

    console = logging.StreamHandler()
    console.setLevel(console_level)
    console.setFormatter(formatter)
    console.addFilter(ContextFilter())
    logger.addHandler(console)

    log_dir.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(log_dir / f"run_{run_id}.log", maxBytes=20_000_000,
                                       backupCount=5, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(ContextFilter())
    logger.addHandler(file_handler)

    return logger


def get_logger(name : str = "") -> logging.Logger:
    return logging.getLogger(f"{LOGGER_NAME}.{name}" if name else LOGGER_NAME)


@contextmanager
def log_context(table : str | None = None , window : str | None = None):
    """Everything logged inside the block is tagged with this table and window."""
    tokens = []
    if table is not None:
        tokens.append((_table, _table.set(table)))
    if window is not None:
        tokens.append((_window, _window.set(window)))
    try:
        yield
    finally:
        for var, token in reversed(tokens):
            var.reset(token)


def log_file_for(run_id : str , log_dir : Path = LOG_DIR) -> Path:
    return log_dir / f"run_{run_id}.log"
