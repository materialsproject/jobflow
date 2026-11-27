"""Tools for logging."""

from __future__ import annotations

import logging


def initialize_logger(
    level: int = logging.INFO,
    fmt: str = "",
    handlers: list[logging.Handler] | None = None,
) -> logging.Logger:
    """Initialize the default logger.

    Parameters
    ----------
    level
        The log level.
    fmt
        Custom format for the generated stdout handler. Defaults to
        JobflowSettings.LOG_FORMAT.
        Common format codes:
        - %(message)s - The logged message
        - %(asctime)s - Human-readable time
        - %(levelname)s - DEBUG, INFO, WARNING, ERROR, or CRITICAL
        See Python logging documentation for more format codes.
    handlers
        Supplied handlers replace the package handlers. Their levels and formatters
        remain unchanged, and they remain installed after this function returns.
        An empty list removes package handlers. Logging configuration is process-wide,
        so records can still propagate to root handlers.

    Returns
    -------
    Logger
        A logging instance with customized formatter and handlers.
    """
    import sys

    from jobflow import SETTINGS

    log = logging.getLogger("jobflow")
    log.setLevel(level)
    log.handlers = []  # reset logging handlers if they already exist

    if handlers is None:
        formatter = logging.Formatter(fmt or SETTINGS.LOG_FORMAT)

        screen_handler = logging.StreamHandler(stream=sys.stdout)
        screen_handler.setFormatter(formatter)
        log.addHandler(screen_handler)
    else:
        for handler in handlers:
            log.addHandler(handler)
    return log
