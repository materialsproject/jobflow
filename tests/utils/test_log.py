import logging

import pytest


@pytest.fixture
def preserve_jobflow_logger():
    logger = logging.getLogger("jobflow")
    handlers = logger.handlers[:]
    level = logger.level
    propagate = logger.propagate
    logger.handlers = []
    logger.setLevel(logging.INFO)
    logger.propagate = True
    yield logger
    logger.handlers = handlers
    logger.setLevel(level)
    logger.propagate = propagate


def test_initialize_logger(capsys, preserve_jobflow_logger):
    from jobflow import initialize_logger

    logger = preserve_jobflow_logger
    # initialize logger with default values
    initialize_logger()
    logger.info("123")
    logger.debug("ABC")

    stdout, stderr = capsys.readouterr()
    assert stdout.endswith("INFO 123\n")
    assert stdout.count("DEBUG") == 0
    assert stderr == ""

    # initialize logger with debug level
    initialize_logger(level=logging.DEBUG)
    logger.info("123")
    stdout, stderr = capsys.readouterr()
    assert stdout.endswith("INFO 123\n")

    logger.debug("ABC")
    stdout, stderr = capsys.readouterr()
    assert stdout.endswith("DEBUG ABC\n")
    assert stderr == ""

    # test with custom format string
    custom_fmt = "%(levelname)s - %(message)s"
    initialize_logger(fmt=custom_fmt)
    logger.info("custom format")

    stdout, stderr = capsys.readouterr()
    assert stdout == "INFO - custom format\n"
    assert stderr == ""


def test_initialize_logger_custom_handlers(preserve_jobflow_logger):
    from jobflow import initialize_logger

    logger = preserve_jobflow_logger
    first = logging.NullHandler()
    second = logging.NullHandler()
    third = logging.NullHandler()
    first_formatter = logging.Formatter("first %(message)s")
    second_formatter = logging.Formatter("second %(message)s")
    first.setFormatter(first_formatter)
    second.setFormatter(second_formatter)

    initialize_logger(fmt="ignored %(message)s", handlers=[first, second, third])

    assert logger.handlers == [first, second, third]
    assert first.formatter is first_formatter
    assert second.formatter is second_formatter
    assert third.formatter is None


def test_initialize_logger_empty_handlers(preserve_jobflow_logger):
    from jobflow import initialize_logger

    logger = preserve_jobflow_logger
    initialize_logger(handlers=[])

    assert logger.handlers == []
