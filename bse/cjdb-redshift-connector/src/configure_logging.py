import logging
import colorlog


def configure_logging(logger_name="", level=logging.INFO, handler=True):
    """
    Configure logging

    Parameters
    ----------
    logger_name: string
        The name of the logger.

    level: logging level

    handler: bool
        Choose whether we attach current logger to streamhandler (In other words whether we show it in the console)

    Returns
    -------
    A logger

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

    """
    If handler is true, the logger is then enabled to stream the logging info to the console. Therefore we should set it 
    to False for all side packages and set it to true in the main script to avoid duplicated logging info. 
    
    """
    if handler is True:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)

    return logger
