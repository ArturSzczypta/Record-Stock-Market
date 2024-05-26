import requests
import logging

def check_internet():
    '''
    Check internet connection
    Loggs error if cannot connect to Google
    '''
    try:
        requests.head("http://www.google.com/", timeout=2)
        # logger.debug('Internet connection active')
    except requests.ConnectionError:
        logging.error(' - Cannot connect to internet')
        