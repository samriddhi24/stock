import psycopg2
from kite_login import Login
from datetime import datetime, timedelta
import pandas as pd
import time
from kiteconnect.exceptions import DataException, TokenException, NetworkException, InputException

# Database connection setup
conn = psycopg2.connect(
    dbname="stock_data_db",
    user="postgres",
    password="admin123",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Kite Connect setup
kite = Login().get_kite_connect()

# Dictionary of symbols and their corresponding instrument tokens
symbol_token_dict = {
    'ASIANPAINT': 60417, 'HDFCBANK': 341249, 'HINDUNILVR': 356865,
    'INFY': 408065, 'ITC': 424961, 'KOTAKBANK': 492033,
    'RELIANCE': 738561, 'SBIN': 779521, 'SUNPHARMA': 857857,
    'TATAMOTORS': 884737, 'TATASTEEL': 895745, 'ICICIBANK': 1270529,
    'AXISBANK': 1510401, 'BHARTIARTL': 2714625, 'MARUTI': 2815745,
    'LT': 2939649, 'TCS': 2953217, 'NTPC': 2977281, 'POWERGRID': 3834113, 'MUTHOOTFIN':6054401	
}

# Time range for 10 years
end_date = datetime.now().date()
start_date = end_date - timedelta(days=3653)  # Approximate 10 years
days_per_batch = 60
# Fetch and insert data for each stock
total_batches = (end_date - start_date).days // days_per_batch + 1
for stock_symbol, instrument_token in symbol_token_dict.items():
    for batch in range(total_batches):
        # Calculate the date range for the current batch
        current_start_date = start_date + timedelta(days=batch * days_per_batch)
        current_end_date = min(current_start_date + timedelta(days=days_per_batch), end_date)
        
        
        try:
            # Fetch minute-wise historical data for the given range
            historical_data = kite.historical_data(
                instrument_token=instrument_token,
                from_date=current_start_date,
                to_date=current_end_date,
                interval="minute"
            )

            # Convert the data to a DataFrame
            df = pd.DataFrame.from_records(historical_data)

            # Insert the data into the database
            for index, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO stock_data (timestamp, stock_symbol, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (row['date'], stock_symbol, row['open'], row['high'], row['low'], row['close'], row['volume']))

            conn.commit()  # Commit the transaction
            break

    
        except KeyError as e:
            print(f"Error: {e}")
        
        except NetworkException as e:
            print(f"Network error while processing symbol {stock_symbol}: {str(e)}")
            time.sleep(60)
            continue
    
        except InputException as e:
            print(f"Input error for symbol {stock_symbol}: {str(e)}")
            continue
    
        except DataException as e:
            print(f"Data error for symbol {stock_symbol}: {str(e)}")
            break
    
        except TokenException as e:
            print(f"Token error for symbol {stock_symbol}: {str(e)}")
            break
    
        except Exception as e:
            error_message = str(e)
            if "429" in error_message or "rate limit" in error_message.lower():
                print(f"Rate limit exceeded: {e}")
                break  
            else:
                print(f"An unexpected error occurred: {e}")
    continue
    
'''start_time = time.time()
    
end_time = time.time()
insert_time = end_time - start_time 
print(f"Data insertion took {insert_time} seconds")'''

start_time = time.time()



end_time = time.time()
query_time = end_time - start_time
print(f"Data quering took {query_time} seconds")

conn.commit()  



        
current_start_date = current_end_date

        
time.sleep(1) 

cursor.close()
conn.close()

print ("Data for all stocks inserted successfully.")