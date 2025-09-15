"""Tools for logging."""

from __future__ import annotations

import logging


def initialize_logger(level: int = logging.INFO, fmt: str = "") -> logging.Logger:
    """Initialize the default logger.

    Parameters
    ----------
    level
        The log level.
    fmt
        Custom logging format string. Defaults to JobflowSettings.LOG_FORMAT.
        Common format codes:
        - %(message)s - The logged message
        - %(asctime)s - Human-readable time
        - %(levelname)s - DEBUG, INFO, WARNING, ERROR, or CRITICAL
        See Python logging documentation for more format codes.

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

    formatter = logging.Formatter(fmt or SETTINGS.LOG_FORMAT)

    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    log.addHandler(screen_handler)
    return log
