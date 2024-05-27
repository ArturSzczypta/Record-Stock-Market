''''functions for GPW - polish stock exchange'''

import datetime
import logging
import pandas as pd



def get_pl_stock_results(given_date: datetime, attempts: int = 3) -> pd.DataFrame:
    '''Extract stock records from Polish GPW as pandas Dataframe
    :given_date: date given by main script
    I Recomend using random sleep between 10 and 45 seconds'''

    # Check if date in weekend
    if given_date.weekday() > 4:
        return pd.DataFrame()

    # Convert date to string
    given_date_str = given_date.strftime('%Y-%m-%d')
    url = 'https://www.gpw.pl/archiwum-notowan-full?type=10&instrument=&' \
          f'date={given_date_str}'

    try:
        attempt = 1
        while attempt <= attempts:
            try:
                stock_df = pd.read_html(url, encoding='utf-8', decimal=',',
                                        thousands=' ')[1]
                logging.debug(f'''Succesfull reading of HTML for date: {given_date},
                     attempt: {attempt} of {attempts}''')
                break
            except ValueError:
                logging.debug(f'''Failed to read HTML for date{given_date},
                     attempt: {attempt=} of {attempts}''')
                attempt += 1

        # Rename few columns to english
        stock_df.rename(columns={'Nazwa': 'name', 'Kod ISIN': 'code_isin',
                        'Waluta': 'currency'}, inplace=True)
        return stock_df
    
    except ValueError:
        logging.error('reading_html', f'No stock trades for {given_date}')

def clean_data_frame_names(data_frame: pd.DataFrame) -> pd.DataFrame:
    '''Clean data frame from unwanted characters
    :data_frame: pandas dataframe with stock data'''

    unwanted_characters = [' ', '.', '-', '(', ')', ',', '/', ';', ':', '?',
                           '!', '\'', '"', '`', '~', '@', '#', '$', '%', '^',
                           '&', '*', '+', '=', '<', '>', '{', '}']
    for character in unwanted_characters:
        data_frame['name'] = data_frame['name'].str.replace(character, '_', regex=True)
    return data_frame
