'''
Methods for web scraping stock records
'''
import os
import logging
from logging import config
import json
import traceback
import re
import mariadb
import pandas as pd
import requests

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


# Get logging_file_name from main script
LOG_FILE_NAME = 'placeholder.log'


def get_log_file_name(new_log_file_name):
    '''
    Get logging_file_name from main script

    :new_log_file_name: main script name with ending '_log.log'
    '''
    # Makes file name global for all other functions
    global LOG_FILE_NAME
    LOG_FILE_NAME = new_log_file_name


def configure_logging():
    '''
    Configure logging to using JSON file
    '''
    with open('stocks.json', 'r', encoding='utf-8') as f:
        log_conf = json.load(f)

    for handler in log_conf.get('handlers', {}).values():
        if handler.get('class') == 'logging.FileHandler':
            handler['filename'] = LOG_FILE_NAME

    logging.config.dictConfig(log_conf)


def log_exception(hierarchy_str, written_string=' '):
    '''
    Save exception as single line in logger

    :hierarchy_str: name of function
    :written_string: Additional info
    '''

    # Change traceback into single string
    exc_message = traceback.format_exc()
    #  Remove ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    exc_message = re.sub(r'\^+', '', exc_message)
    #  Remove ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    exc_message = re.sub(r'\~+', '', exc_message)

    exc_list = exc_message.split('\n')[:-1]
    exc_list = [x.strip(' ') for x in exc_list]
    exc_message = ' - '.join(exc_list)

    configure_logging()
    logger = logging.getLogger(f'main.{hierarchy_str}')
    logger.error(written_string + ' - ' + exc_message)
    logger.debug('')


def check_internet():
    '''
    Check internet connection
    Loggs error if cannot connect to Google
    '''
    try:
        requests.head("http://www.google.com/", timeout=2)
        # logger.debug('Internet connection active')
    except requests.ConnectionError:
        log_exception(' - Cannot connect to internet')


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


def fetch_last_date():
    '''
    Fetch last used date
    '''
    # Instantiate MariaDB cursor
    conn = database_connector()
    mycursor = conn.cursor()

    # Fetch last date recorded, main script will start from this date
    mycursor.execute("SELECT MAX(dates_rec) FROM dates_used")
    # Extract date from mycursor tuple
    last_date = mycursor.fetchone()[0]
    mycursor.close()

    # If table was empty, use '2015-01-02' instead
    last_date = last_date if last_date is not None else '2015-01-02'

    # Create hierarchy of loggers
    logger = logging.getLogger('fetch_last_date')
    logger.debug(f'Last date in database {last_date=}')
    return last_date


def record_date(given_date, has_results):
    '''
    Add date to dates_used table, to remember it's usage

    :given_date: date given by main script
    :any_record: were there results for given day

    DESCRIBE dates_used;
    +--------------+-------------+------+-----+---------+-------+
    | Field        | Type        | Null | Key | Default | Extra |
    +--------------+-------------+------+-----+---------+-------+
    | dates_rec    | varchar(15) | NO   | PRI | NULL    |       |
    | any_record   | TINYINT(1)  | YES  |     | NULL    |       |
    +--------------+-------------+------+-----+---------+-------+
    '''

    # Fetch last recorded date
    recording_str = ("INSERT INTO dates_used "
                     "SET dates_rec = %s, any_record = %s")
    values = (given_date, has_results)

    # Instantiate MariaDB cursor
    conn = database_connector()
    mycursor = conn.cursor()

    try:
        mycursor.execute(recording_str, values)
        conn.commit()
    except :
        log_exception('record_date', f' - Cannot record date {values=}')
    mycursor.close()


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


def relist(given_date, relisted):
    '''
    Record relisted stocks

    :given_date: date given by main script
    :relisted: set of relisted stocks
    '''

    # Check if set is empty
    if not relisted:
        # return immediately if the set is empty
        return

    # Record delisting to SQL table stocks_recorded
    record_relisting = ("UPDATE stocks_recorded SET delisted = NULL, "
                        "relisted = TRUE, relist_count = COALESCE(relist_count, 0) + 1 "
                        "WHERE name = %s")

    # Instantiate MariaDB cursor
    conn = database_connector()
    mycursor = conn.cursor()
    for stock in relisted:
        try:
            mycursor.execute(record_relisting, (stock,))
            conn.commit()
        except:
            log_exception('relist', f' - Variables in {stock=}')
    # Create hierarchy of loggers
    logger = logging.getLogger('relist')
    logger.debug(f'Relisting stocks finished for {given_date=}')
    mycursor.close()


def delist(given_date, obsolete):
    '''
    Record delisted stocks

    :given_date: date given by main script
    :obsolete: set of delisted stocks
    '''

    # Check if set is empty
    if not obsolete:
        # return immediately if the set is empty
        return

    # Prepare dataframe with obsolete listings
    obsolete_df = pd.DataFrame({'obsolete': list(obsolete)})
    obsolete_df.insert(0,'given_date',given_date)
    # Record delisting to SQL table stocks_recorded
    record_delisting = ("UPDATE stocks_recorded "
                        "SET delisted = %s, relisted = NULL "
                        "WHERE name = %s")

    # Instantiate MariaDB cursor
    conn = database_connector()
    mycursor = conn.cursor()
    for row in obsolete_df.itertuples(index=False):
        # row[0] date, row[1] name
        try:
            mycursor.execute(record_delisting, row)
            conn.commit()
        except:
            log_exception('delist', f' - Variables in {row=}')
    # Create hierarchy of loggers
    logger = logging.getLogger('obsolete')
    logger.debug(f'Delisting stocks finished for {given_date=}')
    mycursor.close()


def add(given_df, given_date, new):
    '''
    Add new stocks
    :given_df: pandas dataframe, layout as with polish stock exchange GPW
    :given_date: date given by main script
    :new: set of new stocks
    '''

    # Check if set is empty
    if not new:
        # return immediately if the set is empty
        return

    # Prepare dataframe with new listings
    # Select first two columns
    new_df = given_df[['name', 'code_isin']].copy()
    # Select only new listings
    new_df = new_df.loc[new_df['name'].isin(new)]

    # Add listing date as given
    new_df.loc[:, 'listed'] = given_date

    # Add new stocks to SQL table stocks_recorded
    add_stock = ("INSERT INTO stocks_recorded "
                 "(name, code_isin, listed, delisted, relisted,  relist_count) "
                 "VALUES (%s, %s, %s, NULL, NULL, NULL)")

    # Instantiate MariaDB cursor
    conn = database_connector()
    mycursor = conn.cursor()
    for row in new_df.itertuples(index=False):
        # row[0] name, row[1] code_isin, row[2] date given
        try:
            mycursor.execute(add_stock, row)
            conn.commit()
        except:
            log_exception('add', f' - Variables in {row=}')

    # Get all current tables
    mycursor.execute("SHOW TABLES")
    current_tables = set()
    for row in mycursor:
        current_tables.add(row[0])
    # Don't remove stocks_recorded
    current_tables.remove('stocks_recorded')

    # Create new tables
    # Prepare dataframe table names
    new_tables_df = pd.DataFrame({'name': list(new)})
    new_tables_df['name'] = 't_' + new_tables_df['name'].astype(str).str.lower()

    # Create SQL table for each new stock
    TABLE_NAMES_WHITELIST = frozenset(new_tables_df['name'])

    # Add new stocks to stocks_recorded
    # Check for unexpected table names
    for table_name in TABLE_NAMES_WHITELIST:
        if table_name not in TABLE_NAMES_WHITELIST:
            log_exception('add',
                          f' - Possiple SQL injection attempt in {table_name=}')

        # Check if table already exists
        if table_name not in current_tables:
            try:
                mycursor.execute("CREATE TABLE {table_name} "
                                 "(date_rec DATE NOT NULL PRIMARY KEY, "
                                 "price_openning FLOAT, "
                                 "price_max FLOAT, "
                                 "price_min FLOAT, "
                                 "price_closing FLOAT, "
                                 "change_percent FLOAT, "
                                 "traded_pieces INT, "
                                 "number_transactions INT, "
                                 "trading_value_in_1000 FLOAT);"
                                 .format(table_name=table_name))
                conn.commit()
            except:
                log_exception('add',
                              f' - Cannot create new stock table {table_name=}')
    # Create hierarchy of loggers
    logger = logging.getLogger('write_results')
    logger.debug(f'Adding stocks finished for {given_date=}')
    mycursor.close()


def write_results(given_df, given_date):
    '''
    Write new stock resutls to seperate table for each stock

    :given_df: pandas dataframe, layout as with polish stock exchange GPW
    :given_date: date given by main script
    '''
    # given_df   - pandas dataframe, layout as with polish stock exchange GPW
    # given_date - date given by main script

    # Prepare database
    given_df['name'] = 't_' + given_df['name'].astype(str).str.lower()
    given_df = given_df.drop(['code_isin', 'currency'], axis=1)
    given_df.insert(1, 'given_date', given_date)

    # Instantiate MariaDB cursor
    conn = database_connector()
    mycursor = conn.cursor()
    TABLE_NAMES_WHITELIST = frozenset(given_df['name'])

    for row in given_df.itertuples(index=False):
        '''
        row[0] table_name, row[1] date, row[2] price_openning,
        row[3] price_max, row[4] price_min, row[5] price_closing,
        row[6] change_percent, row[7] traded_pieces,
        row[8] number_transactions, row[9] trading_value_in_1000

        '''
        # Check for unexpected table names
        if row[0] not in TABLE_NAMES_WHITELIST:
            log_exception('write_results',
                          f' - Possiple SQL injection attempt in {row[0]=}')

        # Prepare SQL querry
        sql_string = '''INSERT INTO {0}
        (date_rec, price_openning, price_max, price_min, price_closing,
         change_percent, traded_pieces, number_transactions,
        trading_value_in_1000) VALUES ('{1}', {2}, {3}, {4}, {5}, {6}, {7},
         {8}, {9})'''.format(row[0],row[1],row[2], row[3], row[4], row[5],
                             row[6], row[7], row[8], row[9])

        # Append all stock tables
        try:
            mycursor.execute(sql_string)
            conn.commit()
        except:
            log_exception('write_results',
                          f' - Cannot write to stocks tables {sql_string=}')
    # Create hierarchy of loggers
    logger = logging.getLogger('write_results')
    logger.debug(f'Appending stocks results finished for {given_date=}')
    mycursor.close()


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
