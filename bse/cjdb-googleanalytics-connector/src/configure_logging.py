import logging
import colorlog


def configure_logging(logger_name="", level=logging.INFO):
    """
    Configure the log process

    """
    # format logging
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(levelname)s: %(asctime)s: %(lineno)d: %(module)s: %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        reset=True,
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red",
        },
        secondary_log_colors={},
        style="%",
    )

    logger = logging.getLogger(logger_name)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)

    return logger
