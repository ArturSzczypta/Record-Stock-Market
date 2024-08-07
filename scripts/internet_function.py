'''Check internet connection'''
import requests
import logging

def check_internet_connection():
    '''
    Check internet connection
    Loggs error if cannot connect to Google
    '''
    try:
        requests.head("http://www.google.com/", timeout=2)
        logging.debug('Internet connection active')
    except requests.ConnectionError:
        logging.critical('Cannot connect to the internet')
