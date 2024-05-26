'''Web scrape Polish stock market data'''
import os
from dotenv import load_dotenv
import logging
import pandas as pd
from datetime import datetime

# Load PostgreSQL data from .env
load_dotenv('../.env')

# Get PostgreSQL credentials for Polish Stock Exchange database
DB_HOST = os.getenv('DB_STOCK_EXCHANGE_PL')
DB_PORT = os.getenv('DB_STOCK_EXCHANGE_PL_PORT')
DB_NAME = os.getenv('DB_STOCK_EXCHANGE_PL_NAME')
DB_USER = os.getenv('DB_STOCK_EXCHANGE_PL_USER')
DB_PASSWORD = os.getenv('DB_STOCK_EXCHANGE_PL_PASSWORD')

print(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)


