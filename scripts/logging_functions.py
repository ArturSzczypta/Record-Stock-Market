''''''

import logging
from logging import config
import re
import json


# Get logging_file_name from main script
LOG_FILE_NAME = 'placeholder.log'


def get_log_file_name(new_log_file_name):
    '''
    Get logging_file_name from main script

    :new_log_file_name: main script name with ending '_log.log'
    '''
    # Makes file name global for all other functions
    global LOG_FILE_NAME
    LOG_FILE_NAME = new_log_file_name


def configure_logging():
    '''
    Configure logging to using JSON file
    '''
    with open('stocks.json', 'r', encoding='utf-8') as f:
        log_conf = json.load(f)

    for handler in log_conf.get('handlers', {}).values():
        if handler.get('class') == 'logging.FileHandler':
            handler['filename'] = LOG_FILE_NAME

    logging.config.dictConfig(log_conf)


def log_exception(hierarchy_str, written_string=' '):
    '''
    Save exception as single line in logger

    :hierarchy_str: name of function
    :written_string: Additional info
    '''

    # Change traceback into single string
    exc_message = traceback.format_exc()
    #  Remove ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    exc_message = re.sub(r'\^+', '', exc_message)
    #  Remove ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    exc_message = re.sub(r'\~+', '', exc_message)

    exc_list = exc_message.split('\n')[:-1]
    exc_list = [x.strip(' ') for x in exc_list]
    exc_message = ' - '.join(exc_list)

    configure_logging()
    logger = logging.getLogger(f'main.{hierarchy_str}')
    logger.error(written_string + ' - ' + exc_message)
    logger.debug('')