'''Connectors to different SQL databases'''

import os
from dotenv import load_dotenv
import psycopg2

# Load environment variables from .env file
load_dotenv('../.env.txt')


def postgresql_connector(host, user, password, database):
    conn = psycopg2.connect(host=host, user=user, password=password, database=database)
    return conn


def check_connection(client):
    ''' Check connection to database'''
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(f'connection to db failed: {e}')


if __name__ == '__main__':
    '''Check connection to database'''
    localhost = os.getenv('POSTGRESQL_LOCALHOST')
    user = os.getenv('POSTGRESQL_USER')
    password = os.getenv('POSTGRESQL_PASSWORD')
    database = os.getenv('POSTGRESQL_DATABASE')

    conn = postgresql_connector(host='localhost', user='postgres', password='postgres', database='postgres')
    check_connection(conn)