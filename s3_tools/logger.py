import logging
from logging.handlers import RotatingFileHandler


def init_logger(log_level=None, log_file=None):
    formatter = '[%(levelname)s] %(asctime)s: %(message)s'
    if not log_level:
        log_level = 'INFO'
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    if log_file:
        handler = RotatingFileHandler(log_file, maxBytes=5000000, backupCount=10)
        handler.setFormatter(logging.Formatter(formatter))
        handler.setLevel(log_level)
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(formatter))
        handler.setLevel(log_level)
    root_logger.addHandler(handler)
