import logging


def get_logger(verbose=False):
    log_format = '%(asctime)s %(levelname)s [%(module)s] %(message)s'
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format=log_format, datefmt='%H:%M:%S')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    return logger
