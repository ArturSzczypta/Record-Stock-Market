''' Functions for creating and writing to database'''

import logging
import pandas as pd
import datetime

def missing_stock_indexes(db_connector, data_frame: pd.DataFrame) -> dict:
    '''Check for missing indexes in indexes table'''
    mycursor = db_connector.cursor()
    mycursor.execute("SELECT code_isin FROM stock_indexes")
    missing_indexes = dict()
    for index, code_isin, currency in data_frame.iterrows():
        if code_isin not in mycursor:
            missing_indexes[index] = [code_isin, currency]
    mycursor.close()
    return missing_indexes

def missing_stock_tables(db_connector, data_frame: pd.DataFrame) -> set:
    '''Check for missing stock tables in the database'''
    mycursor = db_connector.cursor()
    mycursor.execute("SHOW TABLES")
    current_tables = set()
    for row in mycursor:
        current_tables.add(row[0])

    missing_tables = set(data_frame['name']) - current_tables
    mycursor.close()
    return missing_tables

def add_missing_indexes(db_connector, missing_indexes: dict) -> None:
    '''Add missing ISINs to the database'''
    mycursor = db_connector.cursor()
    for key, values in missing_indexes.items():
        try:
            mycursor.execute(f'''INSERT INTO stock_indexes (name, code_isin,
                             currency) VALUES ('{key}',
                             '{values[0]}', '{values[1]}');''')
            db_connector.commit()
        except Exception as e:
            logging.error(f'Cannot add stock {row["name"]}'
                          f' to stocks table - {repr(e)}')
    mycursor.close()

def create_stock_table(db_connector, stock_name: str) -> None:
    '''Create missing table in the database'''
    mycursor = db_connector.cursor()
    try:
        mycursor.execute("CREATE TABLE {stock_name} "
                            "(date_rec DATE NOT NULL, "
                            "price_openning FLOAT, "
                            "price_max FLOAT, "
                            "price_min FLOAT, "
                            "price_closing FLOAT, "
                            "change_percent FLOAT, "
                            "traded_volume INT, "
                            "number_transactions INT, "
                            "traded_value_in_1000 FLOAT);"
                            .format(stock_name= stock_name))
        db_connector.commit()
    except Exception as e:
        logging.critical(f'Cannot create table {stock_name} - {repr(e)}')
    mycursor.close()

def write_to_db(db_connector, table_name: str, data_frame: pd.DataFrame,
                given_date: datetime) -> None:
    '''Add new results to tables in database.
    Dataframe has to have columns in a specific order'''
    for row in data_frame.itertuples(index=False):
        sql_querry = f'''INSERT INTO {table_name}
        (date, price_openning, price_max, price_min, price_closing,
        change_percent, traded_volume, number_transactions,
        traded_value_in_1000) VALUES ({str(given_date)}, {row[3]}, {row[3]},
        {row[4]}, {row[5]}, {row[6]}, {row[7]}, {row[8]}, {row[9]},{row[10]})'''

        # Append all stock tables
        try:
            db_connector.execute(sql_querry)
            db_connector.commit()
        except Exception as e:
            logging.error(f'Cannot add results from {given_date}'
                          f' to database - {repr(e)}')
    logging.info(f'Added results from {given_date} to database')

    # Terminate connection
    db_connector.close()
