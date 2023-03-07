'''
Script downloads records from Polish stock exchange GPW to MySQL MariaDB
'''
import os
import datetime
from time import sleep
import logging
from numpy import random
import pandas as pd
import stock_db_functions as f

#Get the database login credentials from environment variables
db_username = os.environ.get('MYSQL_USERNAME')
db_password = os.environ.get('MYSQL_PASSWORD')
db_name = os.environ.get('MYSQL_DB_NAME')
# Set the global variables in secondary.py using the returned values from get_db_credentials
f.get_db_credentials(db_username, db_password, db_name)

#Get this script name
log_file_name = __file__.split('\\')
log_file_name = f'{log_file_name[-1][:-3]}_log.log'

f.get_log_file_name(log_file_name)

#Configure logging file 
f.configure_logging()
logger = logging.getLogger('main')

#Check internet connection, terminate script if no internet
f.check_internet()

#Iterate through dates
#Check is date correct, then continue
delta = datetime.timedelta(days=1)
try:
    last_date = f.fetch_last_date()
    current_date = datetime.datetime(last_date.year,last_date.month,
        last_date.day) + delta
except:
    f.log_exception('last_date', ' - Cannot fetch date from database')
'''
I assume the end_date will be the date today. If not uncomment line below
and comment the 
end_date = datetime.datetime(2023, 1, 1)
'''
date_today = datetime.date.today()
end_date = datetime.datetime(date_today.year, date_today.month,date_today.day)

#Eliminate weekends - .weekday() == 5 is Saturday, 6 is Sunday
#Adjust start date
if current_date.weekday() == 5:
    current_date += 2 * delta
if current_date.weekday() == 6:
    current_date += delta
#Adjust end date (usually today's date)
if end_date.weekday() == 5:
    end_date -= delta
if end_date.weekday() == 6:
    end_date -= 2 * delta

while current_date <= end_date:
    
    #Fetch given day's stocks
    given_date = current_date.strftime('%Y-%m-%d')
    given_date_url = current_date.strftime('%d-%m-%Y')
    logger.info(f'{given_date}')
    url='https://www.gpw.pl/archiwum-notowan-full?type=10&instrument=&date='+\
    given_date_url

    try:
        #Try 5 times to read values, then log error
        MAX_ATTEMPTS = 3
        attempt = 1
        given_df = pd.DataFrame()
        while attempt <= MAX_ATTEMPTS:
            try:
                #Take full results from stock market, 
                given_df = pd.read_html(url,encoding = 'utf-8', decimal=',',
                    thousands=' ')[1]
                logger.debug(f'''Succesfull reading of HTML for {given_date=},
                     current {attempt=} of {MAX_ATTEMPTS=}''')
                print('')
                break
            except:
                logger.debug(f'''Failed to read HTML for {given_date=},
                     current {attempt=} of {MAX_ATTEMPTS=}''')
                attempt += 1
                #Prevent sending too many requests in short period
                sleep(random.uniform(10,45))
        
        #Rename few columns, so they fit to method check_database()
        given_df.rename(columns = {'Nazwa': 'name', 'Kod ISIN': 'code_isin',
            'Waluta': 'currency'},
            inplace = True)

        #Replace '.' with'_' - stock oponeo.pl is seen as database
        given_df['name'] = given_df['name'].str.replace('.','_', regex=True)
        
    except:
        #Record date
        f.record_date (given_date, 0)
        f.log_exception('reading_html', f'No stock trades for {given_date=}')

    else:
        #Checks are there relisted, obsolete or new stocks
        new_stocks,obsolete_stocks,relisted_stocks= f.check_database(given_df)

        #Record relisted stocks
        f.relist_stocks(given_date, relisted_stocks)

        #Record delisted stocks
        f.delist_stocks(given_date, obsolete_stocks)

        #Add new stocks
        f.add_stocks(given_df, given_date, new_stocks)

        #Write new stock resutls to seperate table for each stock
        f.write_results (given_df, given_date)
        
        #Celar stock sets
        new_stocks.clear()
        obsolete_stocks.clear()
        relisted_stocks.clear()

        #Record date
        f.record_date (given_date, 1)
    
    finally:
        #Skip weekends: 5 is Saturday, 6 is Sunday
        if current_date.weekday() < 4:
            current_date += delta
        else:
            current_date += 3 * delta
            if current_date > end_date:
                break
        #Prevent sending too many requests per second
        logger.debug('\n-----\n')
        sleep(random.uniform(10,45))       
#Close connection
f.database_connector().close()
