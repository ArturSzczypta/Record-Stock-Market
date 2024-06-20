'''Functions for logging configuration and formatting exceptions'''

import logging
from logging import config
import re
import json

def configure_logging():
    '''Configure logging using JSON file'''
    with open('logging_conf.json', 'r', encoding='utf-8') as f:
        log_conf = json.load(f)
    config.dictConfig(log_conf)


def format_exception(exc_info):
    '''Format to single line without special characters'''
    #  Remove multiple ^ and ~
    exc_info = re.sub(r'(\^+|\~+)', '', exc_info)
    # Remove multiple spaces
    exc_info = re.sub(r'\s+', ' ', exc_info)
    #replace new lines with |
    exc_info = re.sub(r'\n', ' | ', exc_info)
    return exc_info