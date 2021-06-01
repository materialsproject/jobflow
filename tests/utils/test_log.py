def test_initialize_logger(capsys):
    import logging

    from jobflow import initialize_logger

    logger = logging.getLogger("jobflow")

    # initialize logger with default values
    initialize_logger()
    logger.info("123")
    logger.debug("ABC")

    captured = capsys.readouterr()
    assert "INFO 123" in captured.out
    assert "DEBUG" not in captured.out

    # initialize logger with debug level
    initialize_logger(level=logging.DEBUG)
    logger.info("123")
    logger.debug("ABC")

    captured = capsys.readouterr()
    assert "INFO 123" in captured.out
    assert "DEBUG ABC" in captured.out
