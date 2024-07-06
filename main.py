'''Web scrape stock markets, comodieites and indexes data'''
import os
import csv
from pathlib import Path
from dotenv import load_dotenv
import logging
import pandas as pd
from datetime import datetime
from scripts import logging_functions as lf


load_dotenv('../.env')

current_file_name = Path(__file__).stem
log_file_name = f'{current_file_name}_log.log'

BASE_DIR = Path(__file__).parent
LOGGING_FILE = BASE_DIR / 'logging_files' / log_file_name
LOGGING_JSON = BASE_DIR / 'logging_files' / 'logging_config.json'

lf.configure_logging(LOGGING_JSON, LOGGING_FILE)

COMODITIES_DIR = BASE_DIR / 'comodities'
INDEXES_DIR = BASE_DIR / 'indexes'

# Check if there are any comodities files
comodities_files = Path(COMODITIES_DIR).iterdir()
comodities_files_exist = any(file.is_file() for file in comodities_files)

# Check if there are any indexes files
indexes_files = Path(INDEXES_DIR).iterdir()
indexes_files_exist = any(file.is_file() for file in indexes_files)



