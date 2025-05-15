"""Logging utility."""

import logging
import os
from datetime import datetime
from config import APP_NAME

GREY = "\x1b[38;20m"
YELLOW = "\x1b[33;20m"
BOLD_YELLOW = "\x1b[33;1m"
RED = "\x1b[31;20m"
BOLD_RED = "\x1b[31;1m"
GREEN = "\x1b[32;20m"
BOLD_GREEN = "\x1b[32;1m"
MAGENTA = "\x1b[35;20m"
BOLD_MAGENTA = "\x1b[35;1m"


class CustomFormatter(logging.Formatter):

    def __init__(self):
        super().__init__()
        self.to_format = {
            logging.DEBUG: self._colored_fmt(YELLOW),
            logging.INFO: self._colored_fmt(GREEN),
            logging.WARNING: self._colored_fmt(BOLD_YELLOW),
            logging.ERROR: self._colored_fmt(RED),
            logging.CRITICAL: self._colored_fmt(BOLD_RED),
        }
        self.old_format = self._default_fmt()

    def _colored_fmt(self, color: str) -> str:
        """retorna el color indicado... """
        reset = "\x1b[0m"
        return f"{color}%(asctime)s - %(name)s - %(filename)s - %(levelname)s{reset} - %(message)s"

    def _default_fmt(self) -> str:
        """Retorna el formato.."""
        return "%(asctime)s - %(name)s - %(filename)s - %(levelname)s - %(message)s"

    def _get_header_length(self, record):
        """el Header del record del log... """
        formatter = logging.Formatter(self.old_format)
        return len(
            formatter.format(
                logging.LogRecord(
                    name=record.name,
                    level=record.levelno,
                    pathname=record.pathname,
                    lineno=record.lineno,
                    msg="",
                    args=(),
                    exc_info=None,
                )
            )
        )

    def format(self, record):
        log_fmt = self.to_format.get(record.levelno)
        indent = " " * self._get_header_length(record)
        formatter = logging.Formatter(log_fmt)
        head, *trailing = formatter.format(record).splitlines(True)
        return head + "".join(indent + line for line in trailing)


def get_logger(app_name: str = APP_NAME, to_file: bool = False):
    """
        Esto crea un logger para que se pueda observar por terminal... 
    """

    logger = logging.getLogger(app_name)

    if not logger.hasHandlers():

        handlers = []

        logger = logging.getLogger(app_name)
        logger.propagate = False
        logger.setLevel(logging.DEBUG)

        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(CustomFormatter())
        handlers.append(handler)

        if to_file:
            root_dir = os.path.dirname(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            )
            now = datetime.now().strftime(r"%Y-%m-%d_%H:%M")
            f_handler = logging.FileHandler(
                os.path.join(root_dir, "logs", f"log_{now}.log")
            )
            f_handler.setLevel(logging.DEBUG)
            f_handler.setFormatter(CustomFormatter())
            handlers.append(f_handler)

        for handler in handlers:
            logger.addHandler(handler)

    return logger
