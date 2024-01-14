''' Functions for creating and writing to database indexes
Taken from Yahoo Finance'''
import logging
import pandas as pd


def create_table(conn, index_name: str, currency: str) -> None:
    '''Create table in database
    :conn: connector to database
    :index_name: name of table in database
    :currency: currency of index, from website

    price_closing: close price adjusted for splits
    price_closing_adjusted: adjusted close price adjusted for splits
                            and dividend and/or capital gain distributions'''

    # Check if table already exists
    mycursor = conn.cursor()
    mycursor.execute("SHOW TABLES")
    current_tables = set()
    for row in mycursor:
        current_tables.add(row[0])
    if index_name not in current_tables:
        try:
            mycursor.execute("CREATE TABLE {index_name} "
                             "(date_rec DATE NOT NULL, "
                             "currency VARCHAR(3) {currency}, "
                             "price_openning FLOAT, "
                             "price_max FLOAT, "
                             "price_min FLOAT, "
                             "price_closing FLOAT, "
                             "price_closing_adjusted FLOAT, "
                             "volume INT);"
                             .format(index_name=index_name,
                                     currency=currency))
            conn.commit()
        except Exception as e:
            print(f'Cannot create table {index_name}, {e}')
    mycursor.close()


def write_to_db(db_connector, index_name: str, currency: str,
                data: pd.DataFrame) -> None:
    '''Add new results to database
    :db_connector: connector to database
    :index_name: name of table in database
    :data: pandas dataframe with stock results'''

    # Return if no data
    if not data:
        return

    # Add currency to dataframe
    data.insert(2, 'currency', currency)

    for row in data.itertuples(index=False):

        # Prepare SQL querry
        sql_string = '''INSERT INTO {0}
        (date, currency, price_openning, price_max, price_min,
        price_closing, price_closing_adjusted, volume) VALUES ('{1}', {2}, {3},
        {4},{5},{6},{7},{8})'''.format(index_name, row[0], row[1], row[2],
                                       row[3], row[4], row[5], row[6], row[7])

        # Append all stock tables
        try:
            db_connector.execute(sql_string)
            db_connector.commit()
        except Exception as e:
            logging.error(f'Cannot add results to {index_name}'
                          f'to database, {e}')
    logging.info(f'Added results to {index_name} to database')

    # Terminate connection
    db_connector.close()
