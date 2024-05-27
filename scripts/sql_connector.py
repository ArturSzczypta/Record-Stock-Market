'''Connectors to different SQL databases'''

import os
from dotenv import load_dotenv
import psycopg2
import logging

# Load environment variables from .env file
load_dotenv('../.env.txt')


def postgresql_connector(host: str, user: str, password: str,
                         database: str) -> psycopg2.connect:
    '''Connect to PostgreSQL database'''

    conn = psycopg2.connect(host=host, user=user, password=password,
                            database=database)
    return conn


def check_connection(client: psycopg2.connect) -> None:
    ''' Check connection to database'''
    try:
        client.admin.command('ping')
        logging.debug("You successfully connected to db")
    except Exception as e:
        print(f'Connection to db failed: {e}')


if __name__ == '__main__':
    '''Check connection to database'''
    

    conn = postgresql_connector(host='localhost', user='postgres',
                                password='postgres', database='postgres')
    check_connection(conn)
    conn.close()
