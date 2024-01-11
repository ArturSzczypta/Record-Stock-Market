'''Connectors to different SQL databases'''

import os
from dotenv import load_dotenv
import psycopg2

# Load environment variables from .env file
load_dotenv('../.env.txt')


#PostgreSQL
def postgresql_connector(host, user, password, database):
    conn = psycopg2.connect(host=host, user=user, password=password, database=database)
    return conn

# Check connection to database
def check_connection(client):
    ''' Check connection to database'''
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except:
        print('Unable to connect with database')

if __name__ == '__main__':
    '''Check connection to database'''
    localhost = os.getenv('POSTGRESQL_LOCALHOST')
    user = os.getenv('POSTGRESQL_USER')
    password = os.getenv('POSTGRESQL_PASSWORD')
    database = os.getenv('POSTGRESQL_DATABASE')

    conn = postgresql_connector(host='localhost', user='postgres', password='postgres', database='postgres')
    check_connection(conn)





















# Get the database login credentials from main script
DB_USERNAME = ''
DB_PASSWORD = ''
DB_NAME = ''


def get_db_credentials(username, password, name):
    '''
    Get the database login credentials from main script
    :username: enviroment username
    :password: enviroment password
    :name: enviroment name of database
    '''
    # Makes credentials global for all other functions
    global DB_USERNAME
    global DB_PASSWORD
    global DB_NAME
    DB_USERNAME = username
    DB_PASSWORD = password
    DB_NAME = name
    print(type(DB_USERNAME), type(DB_PASSWORD), type(DB_NAME))




def database_connector(host="localhost", port=3306,
                       user=None, password=None, database=None):
    '''
    Create database connector
    :host: by default "localhost"
    :port: by default 3306
    :user: database user
    :password: user's password
    :database: name of database
    '''
    #  Use the global variables in case values are not provided
    user = user or DB_USERNAME
    password = password or DB_PASSWORD
    database = database or DB_NAME
    try:
        conn = mariadb.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database)
        return conn
    except:
        log_exception('database_connector',
                      ' - Cannot create database connector')


def check_database(given_df):
    '''
    Checks are there new or obsolete stocks

    :given_df: pandas dataframe, layout as with polish stock exchange GPW

    DESCRIBE stocks_recorded;
    +--------------+-------------+------+-----+---------+-------+
    | Field        | Type        | Null | Key | Default | Extra |
    +--------------+-------------+------+-----+---------+-------+
    | name         | varchar(15) | NO   | PRI | NULL    |       |
    | code_isin    | varchar(15) | YES  |     | NULL    |       |
    | listed       | date        | YES  |     | NULL    |       |
    | delisted     | date        | YES  |     | NULL    |       |
    | relisted     | tinyint(1)  | YES  |     | NULL    |       |
    | relist_count | smallint(6) | YES  |     | NULL    |       |
    +--------------+-------------+------+-----+---------+-------+

    stocks_recorded columns:
    name         - name of each stock, UPPER
    code_isin    - international code ISIN
    listed       - date first listed
    delisted     - date delisted in database
    relisted     - if now relisted, then TRUE
    relist_count - counts how many times stocks appear to be relisted

    '''
    # Instantiate MariaDB cursor
    conn = database_connector()
    mycursor = conn.cursor()

    # Extract current stocks
    mycursor.execute("SELECT name FROM stocks_recorded "
                     "WHERE delisted IS NULL")

    current = set()
    for row in mycursor:
        current.add(row[0])

    # Extract delisted stocks
    mycursor.execute("SELECT name FROM stocks_recorded "
                     "WHERE delisted IS NOT NULL")
    delisted = set()
    for row in mycursor:
        delisted.add(row[0])

    # Extract already relisted stocks
    mycursor.execute("SELECT name FROM stocks_recorded "
                     "WHERE relisted IS NOT NULL")
    already_relisted = set()
    for row in mycursor:
        already_relisted.add(row[0])

    # Extract given's stocks names as type set
    given = set(given_df['name'])

    # Compare given's stocks to current and delisted.
    new = given - current - delisted
    obsolete = current - given
    relisted = delisted - given - already_relisted

    # Create hierarchy of loggers
    logger = logging.getLogger('check_database')
    logger.info(f'new:\n {new}')
    logger.info(f'obsolete:\n {obsolete}')
    logger.info(f'relisted:\n {relisted}')

    mycursor.close()
    return new, obsolete, relisted



def main():
    ''' Performs basic logging set up, if script is runned directly'''

    # Get the database login credentials from environment variables
    db_username = os.environ.get('MYSQL_USERNAME')
    db_password = os.environ.get('MYSQL_PASSWORD')
    db_name = os.environ.get('MYSQL_DB_NAME')
    #  Set the global variables in secondary.py using the returned values from get_db_credentials
    get_db_credentials(db_username, db_password, db_name)

    # Get this script name
    log_file_name = __file__.split('\\')
    log_file_name = f'{log_file_name[-1][:-3]}_log.log'

    get_log_file_name(log_file_name)

    # Configure logging file
    configure_logging()
    logger = logging.getLogger('main')

    # Check internet connection, terminate script if no internet
    check_internet()






if __name__ == '__main__':
    main()
