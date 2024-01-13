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
                print(f'Cannot create table {table_name}')
    # Create hierarchy of loggers
    logger = logging.getLogger('write_results')
    logger.debug(f'Adding stocks finished for {given_date}')
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
                          f' - Possiple SQL injection attempt in {row[0]}')

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
                          f' - Cannot write to stocks tables {sql_string}')
    # Create hierarchy of loggers
    logger = logging.getLogger('write_results')
    logger.debug(f'Appending stocks results finished for {given_date}')
    mycursor.close()



if __name__ == '__main__':
    print('main')
