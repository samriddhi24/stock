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


async def fn():
    print("This is ")
    await asyncio.sleep(1)
    print("asynchronous programming")
    await asyncio.sleep(1)
    print("and not multi-threading")


def getData(instrument_token, current_start_date):

    try:
        current_end_date = current_start_date + timedelta(days=60)


        # Function call to kiteDATA


        print(current_end_date)
        if current_end_date >= today:
            return today
        else:
            return current_end_date
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")


def println(text):
    for _ in range(20):
        print("*", sep="", end="")
        print(f"****************************\t{text}\t*******************")
        print("*", sep="", end="\n")


today = datetime.now().date()
if __name__ == "__main__":

    start_date = today - timedelta(days=3653)  # Approximate 10 years
    # print(start_date)
    # print(start_date + timedelta(days=60))
    endDAte = start_date
    token = 351315
    for stock_symbol, instrument_token in symbol_token_dict.items():
        println(f"working on {stock_symbol}")
    # while endDAte < today:
        if endDAte < today:
            try:
                endDAte = getData(instrument_token, endDAte)
                print("end: ", endDAte)
            except Exception as e:
                print("Error:", str(e))
                break
    endDAte = start_date
