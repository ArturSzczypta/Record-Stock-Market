''''functions for GPW - polish stock exchange'''

import datetime
import logging
import pandas as pd


def get_results(given_date: datetime, attempts: int = 3) -> pd.DataFrame:
    '''Get stock records from GPW as pandas Dataframe
    :given_date: date given by main script
    :return: pandas dataframe, layout as with polish stock exchange GPW
    Recomend using random sleep between 10 and 45 seconds'''

    # Check if date in weekend
    if given_date.weekday() > 4:
        return pd.DataFrame()

    # Convert date to string
    given_date_str = given_date.strftime('%Y-%m-%d')
    url = 'https://www.gpw.pl/archiwum-notowan-full?type=10&instrument=&' \
          f'date={given_date_str}'

    try:
        # Try 5 times to read values, then log error
        attempt = 1
        while attempt <= attempts:
            try:
                # Take full results from stock market
                stock_df = pd.read_html(url, encoding='utf-8', decimal=',',
                                        thousands=' ')[1]
                logging.debug(f'''Succesfull reading of HTML for {given_date=},
                     attempt: {attempt} of max:{attempts}''')
                break
            except ValueError:
                logging.debug(f'''Failed to read HTML for {given_date},
                     attempt: {attempt=} of  max:{attempts}''')
                attempt += 1

        # Rename few columns to english
        stock_df.rename(columns={'Nazwa': 'name', 'Kod ISIN': 'code_isin',
                        'Waluta': 'currency'}, inplace=True)

        # Replace '.' with'_' - stock oponeo.pl is seen as database
        stock_df['name'] = stock_df['name'].str.replace('.', '_', regex=True)
        return stock_df
    except ValueError:
        # Record date
        logging.error('reading_html', f'No stock trades for {given_date}')
        return pd.DataFrame()
