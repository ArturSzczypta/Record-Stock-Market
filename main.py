'''Web scrape stock markets, comodieites and indexes data'''
import os
import csv
from pathlib import Path
from dotenv import load_dotenv
import logging
import pandas as pd
from datetime import datetime
from scripts import logging_functions as l_f
from scripts import internet_function as i_f
from scripts import db_connector as db_c
from scripts import scraping_functions as s_f
from scripts import stock_db_functions as s_d

''' Preparation of logging, connections and directories and saved date'''

load_dotenv('../.env')

current_file_name = Path(__file__).stem
log_file_name = f'{current_file_name}_log.log'

BASE_DIR = Path(__file__).parent
LOGGING_FILE = BASE_DIR / 'logging_files' / log_file_name
LOGGING_JSON = BASE_DIR / 'logging_files' / 'logging_config.json'

l_f.configure_logging(LOGGING_JSON, LOGGING_FILE)

COMODITIES_DIR = BASE_DIR / 'comodities'
INDEXES_DIR = BASE_DIR / 'indexes'

date_saved_file = BASE_DIR / 'txt_files' / 'date_saved.txt'
with open(date_saved_file, 'r', encoding='utf-8') as file:
    date_str = file.readline().lstrip('\ufeff').strip()
    date_saved = datetime.strptime(date_str, '%Y-%m-%d')
    logging.debug(f'Date saved: {date_saved}')

comodities_files = Path(COMODITIES_DIR).iterdir()
comodities_files_exist = any(file.is_file() for file in comodities_files)
logging.debug(f'comodities files?: {comodities_files_exist}') 

indexes_files = Path(INDEXES_DIR).iterdir()
indexes_files_exist = any(file.is_file() for file in indexes_files)
logging.debug(f'indexes files?: {indexes_files_exist}')

i_f.check_internet_connection()

'''Scraping data from polish stock exchange'''
pl_stocks = db_c.get_db_credentials('../.env', 'DB_STOCK_EXCHANGE_PL')

conn = db_c.db_connector(pl_stocks['host'], pl_stocks['user'],
                    pl_stocks['password'], pl_stocks['db_name'])

db_c.check_db_connection(conn)
stock_df = s_f.get_pl_stock_results(date_saved)
if stock_df:
    stock_df = s_f.clean_dataframe_names(stock_df)

missing_indexes = s_d.add_missing_indexes(conn, stock_df)
missing_stock_tables = s_d.add_missing_stock_tables(conn, stock_df)

s_d.add_missing_indexes(conn, missing_indexes)
for stock in missing_stock_tables:
    s_d.create_stock_table(conn, stock)

s_d.write_to_db(conn, stock_df, date_saved)