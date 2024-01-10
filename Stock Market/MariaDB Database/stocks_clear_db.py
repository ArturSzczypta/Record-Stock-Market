'''
Clear necessary tables, delete any other tables
'''
import stock_db_functions as f

# Delete tables for all stocks and clear table stock_records

# Check if a csv file for errors exiss. If not it create it

# Create database connector
conn = f.database_connector()
mycursor = conn.cursor()

# Delete all record from stocks_recorded
mycursor.execute("DELETE FROM stocks_recorded")

# Delete all record from dates_used
mycursor.execute("DELETE FROM dates_used")

tables = set()
for row in mycursor:
    tables.add(row[0])
# Don't remove stocks_recorded and dates_used
tables.remove('stocks_recorded')
tables.remove('dates_used')

# Remove tables
for item in tables:
    mycursor.execute("DROP TABLE " + str(item))
    conn.commit()

conn.close()
