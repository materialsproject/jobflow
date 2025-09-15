def test_initialize_logger(capsys):
    import logging

    from jobflow import initialize_logger

    logger = logging.getLogger("jobflow")

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
