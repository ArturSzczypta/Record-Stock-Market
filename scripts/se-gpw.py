''''functions for GPW - polish stock exchange'''

import os
import datetime
from time import sleep
import logging
import pandas as pd


def get_results(given_date, attempts=3):
    '''Get stock records from GPW as pandas Dataframe
    :given_date: date given by main script
    :return: pandas dataframe, layout as with polish stock exchange GPW
    Recomend using random sleep between 10 and 45 seconds'''

    # Check if date in weekend
    if given_date.weekday() > 4:
        return pd.DataFrame()
    
    # Convert date to string
    given_date_str = given_date.strftime('%Y-%m-%d')
    url = 'https://www.gpw.pl/archiwum-notowan-full?type=10&instrument=&date=' + given_date_str

    try:
        # Try 5 times to read values, then log error
        attempts = 3
        attempt = 1
        stock_df = pd.DataFrame()
        while attempt <= attempts:
            try:
                # Take full results from stock market, 
                stock_df = pd.read_html(url, encoding='utf-8', decimal=',',
                                        thousands=' ')[1]
                logging.debug(f'''Succesfull reading of HTML for {given_date=},
                     attempt: {attempt} of max:{attempts}''')
                break
            except:
                logging.debug(f'''Failed to read HTML for {given_date},
                     attempt: {attempt=} of  max:{attempts}''')
                attempt += 1

        # Rename few columns to english
        stock_df.rename(columns={'Nazwa': 'name', 'Kod ISIN': 'code_isin',
                        'Waluta': 'currency'}, inplace=True)

        # Replace '.' with'_' - stock oponeo.pl is seen as database
        stock_df['name'] = stock_df['name'].str.replace('.','_', regex=True)
        return stock_df
    except:
        # Record date
        logging.error('reading_html', f'No stock trades for {given_date}')
        return pd.DataFrame()


def add_to_db(db_connector, table_name, data, given_date):
    '''Add new results to database'''

    # Return if no data
    if not data:
        return
    
    for row in given_df.itertuples(index=False):
        '''
        row[0] table_name, row[1] date, row[2] currency,
        row[3] price_openning, row[4] price_max, row[5] price_min,
        row[6] price_closing,row[7] change_percent, row[8] traded_pieces,
        row[9] number_transactions, row[10] trading_value_in_1000
        '''
        
        # Prepare SQL querry
        sql_string = '''INSERT INTO {0}
        (date_rec, currency, price_openning, price_max, price_min, price_closing,
         change_percent, traded_pieces, number_transactions,
        trading_value_in_1000) VALUES ('{1}', {2}, {3}, {4}, {5}, {6}, {7},
         {8}, {9}, {10})'''.format(row[0],row[1],row[2], row[3], row[4], row[5],
                             row[6], row[7], row[8], row[9], row[10])

        # Append all stock tables
        try:
            db_connector.execute(sql_string)
            db_connector.commit()
        except:
            logging.error('add_to_db', f'Cannot add {given_date} to database')




"(date_rec DATE NOT NULL PRIMARY KEY, "
"price_openning FLOAT, "
"price_max FLOAT, "
"price_min FLOAT, "
"price_closing FLOAT, "
"change_percent FLOAT, "
"traded_pieces INT, "
"number_transactions INT, "
"trading_value_in_1000 FLOAT);"