import psycopg2
from kite_login import Login
from datetime import datetime, timedelta
import pandas as pd
import time, os
from kiteconnect.exceptions import (
    DataException,
    TokenException,
    NetworkException,
    InputException,
)
import asyncio
from dotenv import load_dotenv

load_dotenv()


# Database connection setup
"""conn = psycopg2.connect(
    dbname="stock_data_db",
    user="postgres",
    password="admin123",
    host="localhost",
    port="5432",
)
cursor = conn.cursor()
"""


async def fn():
    print("This is ")
    await asyncio.sleep(1)
    print("asynchronous programming")
    await asyncio.sleep(1)
    print("and not multi-threading")


def dbOperation(df):
    for index, row in df.iterrows():
        cursor.execute(
            """
                INSERT INTO stock_data (timestamp, stock_symbol, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row["date"],
                stock_symbol,
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
            ),
        )

    conn.commit()  # Commit the transaction


def stockData(instrument_token, current_start_date, enddate):
    try:
        # Fetch minute-wise historical data for the given range
        historical_data = kite.historical_data(
            instrument_token=instrument_token,
            from_date=current_start_date,
            to_date=enddate,
            interval="minute",
        )

        # Convert the data to a DataFrame
        df = pd.DataFrame.from_records(historical_data)
        dbOperation(df)
        # Insert the data into the database

    except KeyError as e:
        print(f"Error: {e}")

    except NetworkException as e:
        print(f"Network error while processing symbol {stock_symbol}: {str(e)}")
        time.sleep(60)

    except InputException as e:
        print(f"Input error for symbol {stock_symbol}: {str(e)}")

    except DataException as e:
        print(f"Data error for symbol {stock_symbol}: {str(e)}")

    except TokenException as e:
        print(f"Token error for symbol {stock_symbol}: {str(e)}")

    except Exception as e:
        error_message = str(e)
        if "429" in error_message or "rate limit" in error_message.lower():
            print(f"Rate limit exceeded: {e}")

        else:
            print(f"An unexpected error occurred: {e}")

def loop(token,endDate):
    while endDAte < today:
        try:
            endDAte = getData(token, endDAte)
            print("end: ", endDAte)
        except Exception as e:
            print("Error:", str(e))
            break

def getData(instrument_token, current_start_date):

    try:
        current_end_date = current_start_date + timedelta(days=60)
        print(current_end_date)

        stockData(instrument_token, current_start_date, current_end_date)

        if current_end_date >= datetime.now().date():
            return datetime.now().date()
        else:
            return current_end_date
    except Exception as e:
        error_message = str(e)
        if "429" in error_message or "rate limit" in error_message.lower():
            print(f"Rate limit exceeded: {e}")
            return ""
        else:
            print(f"An unexpected error occurred: {e}")


def println(text):
    for _ in range(20):
        print("*", sep="", end="")
        print(f"****************************\t{text}\t*******************")
        print("*", sep="", end="")


load_dotenv()



if __name__ == "__main__":
    # DB CONFIG
    conn = psycopg2.connect(
        dbname=os.environ['DB'],
        user=os.environ['DBUSER'],
        password=os.environ['DBPASS'],
        host=os.environ['DBHOST'],
        port=os.environ['DBPORT'],
    )
    cursor = conn.cursor()

    # Kite Connect setup
    kite = Login().get_kite_connect()

    # Dictionary of symbols and their corresponding instrument tokens
    symbol_token_dict = {
        "ASIANPAINT": 60417,
        "HDFCBANK": 341249,
        "HINDUNILVR": 356865,
        "INFY": 408065,
        "ITC": 424961,
        "KOTAKBANK": 492033,
        "RELIANCE": 738561,
        "SBIN": 779521,
        "SUNPHARMA": 857857,
        "TATAMOTORS": 884737,
        "TATASTEEL": 895745,
        "ICICIBANK": 1270529,
        "AXISBANK": 1510401,
        "BHARTIARTL": 2714625,
        "MARUTI": 2815745,
        "LT": 2939649,
        "TCS": 2953217,
        "NTPC": 2977281,
        "POWERGRID": 3834113,
        "MUTHOOTFIN": 6054401,
    }

    today = datetime.now().date()
    # Approximate 10 years
    start_date = today - timedelta(days=3653)  
    endDAte = start_date

    for stock_symbol, instrument_token in symbol_token_dict.items():
        print(stock_symbol)
        pass
    
    cursor.close()
