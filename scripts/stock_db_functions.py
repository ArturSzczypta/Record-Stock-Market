''' Functions for creating and writing to database'''
import logging
import pandas as pd
import datetime


def create_table(conn, stock_exchange: str) -> None:
    '''Create table in database
    :conn: connector to database
    :stock_exchange: name of table in database'''

    # Check if table already exists
    mycursor = conn.cursor()
    mycursor.execute("SHOW TABLES")
    current_tables = set()
    for row in mycursor:
        current_tables.add(row[0])
    if stock_exchange not in current_tables:
        try:
            mycursor.execute("CREATE TABLE {stock_exchange} "
                             "(date_rec DATE NOT NULL, "
                             "currency VARCHAR(3), "
                             "price_openning FLOAT, "
                             "price_max FLOAT, "
                             "price_min FLOAT, "
                             "price_closing FLOAT, "
                             "change_percent FLOAT, "
                             "traded_pieces INT, "
                             "number_transactions INT, "
                             "trading_value_in_1000 FLOAT);"
                             .format(stock_exchange=stock_exchange))
            conn.commit()
        except Exception as e:
            print(f'Cannot create table {stock_exchange}, {e}')
    mycursor.close()


def write_to_db(db_connector, table_name: str, data_frame: pd.data_frameFrame,
                given_date: datetime) -> None:
    '''Add new results to table
    :db_connector: connector to database
    :table_name: name of the table
    :data_frame: pandas dataframe with stock results
    :given_date: date given by main script'''

    # Add date to dataframe
    data_frame.insert(1, 'date', given_date)

    for row in data_frame.itertuples(index=False):

        # Prepare SQL querry
        sql_string = f'''INSERT INTO {table_name}
        (date, currency, price_openning, price_max, price_min,
        price_closing, change_percent, traded_pieces, number_transactions,
        trading_value_in_1000) VALUES ('{row[0]}', {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]},
        {row[6]}, {row[7]}, {row[8]}, {row[9]}, {row[10]})'''

        # Append all stock tables
        try:
            db_connector.execute(sql_string)
            db_connector.commit()
        except Exception as e:
            logging.error(f'Cannot add results from {given_date}'
                          f'to database, {e}')
    logging.info(f'Added results from {given_date} to database')

    # Terminate connection
    db_connector.close()
