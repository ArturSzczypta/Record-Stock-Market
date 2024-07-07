'''Connectors to PostgreSQL database'''

import os
from dotenv import load_dotenv
import psycopg2
import logging

import db_credentials


def get_db_credentials(env_location: str, db_name) -> dict:
    '''Get database credentials from .env file'''
    load_dotenv(env_location)
    db_credentials = {
        'host': os.getenv(f'{db_name}_HOST'),
        'port': os.getenv(f'{db_name}_PORT'),
        'db_name': os.getenv(f'{db_name}_NAME'),
        'user': os.getenv(f'{db_name}_USER'),
        'password': os.getenv(f'{db_name}_PASSWORD')}
    return db_credentials

def db_connector(host: str, user: str, password: str,
                         database: str) -> psycopg2.connect:
    '''Connect to PostgreSQL database'''
    conn = psycopg2.connect(host=host, user=user, password=password,
                            database=database)
    return conn


def check_db_connection(conn: psycopg2.connect) -> None:
    ''' Check connection to database'''
    try:
        conn.admin.command('ping')
        logging.debug("You successfully connected to db")
    except Exception as e:
        logging.critical(f'Connection to db failed: {e}')


if __name__ == '__main__':
    '''Check connection to database'''
    
    enviroment_loc = '../../.env.txt'
    pl_stocks = get_db_credentials(enviroment_loc, 'DB_STOCK_EXCHANGE_PL')

    conn = db_connector(pl_stocks['host'], pl_stocks['user'],
                        pl_stocks['password'], pl_stocks['db_name'])
    check_db_connection(conn)
    conn.close()
