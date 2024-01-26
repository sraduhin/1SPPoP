import sys

from core.config import logger


def _giveup(e):
    logger.error("Giving up after multiple failures. X/")
    sys.exit(-1)


def _backoff(details):
    logger.warning("Failed connection. Reconnecting...")
