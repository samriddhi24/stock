import psycopg2
from kite_login import Login
from datetime import datetime, timedelta
import pandas as pd
import time

# Database connection setup
conn = psycopg2.connect(
    dbname="your_dbname",
    user="your_username",
    password="your_password",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Kite Connect setup
kite = Login().get_kite_connect()

# Fetch minute-wise data for one day
instrument_token = 779521  # Replace with your actual instrument token
stock_symbol = 'RELIANCE'  # Replace with the actual stock symbol

to_date = datetime.now().date()
from_date = to_date - timedelta(days=1)

historical_data = kite.historical_data(
    instrument_token=instrument_token,
    from_date=from_date,
    to_date=to_date,
    interval="minute"
)

# Convert the data to a DataFrame
df = pd.DataFrame.from_records(historical_data)

# Timing the insertion process
start_time = time.time()

# Insert the data into the database
for index, row in df.iterrows():
    cursor.execute("""
        INSERT INTO stock_data (timestamp, stock_symbol, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (row['date'], stock_symbol, row['open'], row['high'], row['low'], row['close'], row['volume']))

conn.commit()  # Commit the transaction

end_time = time.time()
insert_time = end_time - start_time
print(f"Data insertion took {insert_time} seconds")

# Timing a sample query (e.g., fetching all data for that day)
start_time = time.time()

cursor.execute("""
    SELECT * FROM stock_data
    WHERE stock_symbol = %s AND timestamp >= %s AND timestamp < %s
""", (stock_symbol, from_date, to_date))
query_results = cursor.fetchall()

end_time = time.time()
query_time = end_time - start_time
print(f"Data querying took {query_time} seconds")

# Close the database connection
cursor.close()
conn.close()
