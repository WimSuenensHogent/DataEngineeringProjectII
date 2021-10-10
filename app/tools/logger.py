"""[summary]

Returns:
    [type]: [description]
"""
import os
import logging
import logging.config
import yaml

from settings import BASE_DIR

# class Logger(logging.Handler):

    

def get_logger(name):
    """[summary]

    Args:
        name ([type]): [description]

    Returns:
        [type]: [description]
    """

    # logging.basicConfig(level=
    #   logging.DEBUG,
    #   format=
    #     """
    #         \n--- %(levelname)s ---\n
    #         Name = %(name)s | Process = %(process)d | %(asctime)s
    #         \n%(message)s\n
    #     """
    # )
    # logging.debug('This is a debug message')
    # logging.info('This is an info message')
    # logging.warning('This is a warning message')
    # logging.error('This is an error message')
    # logging.critical('This is a critical message')

    with open(os.path.join(BASE_DIR, 'settings.yaml'), mode='r', encoding="UTF-8") as file:
        config = yaml.safe_load(file.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(name)

    return logger
