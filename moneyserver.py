from pymongo import MongoClient
from urllib.parse import quote_plus
from nsepython import *
import pandas as pd
import os
import datetime
import time

username = 'akshitnarwal03'
password = 'Narwal@2003'

# MongoDB connection details
MONGODB_URI = f"mongodb+srv://{quote_plus(username)}:{quote_plus(password)}@moneypi.ifmycrj.mongodb.net/?retryWrites=true&w=majority"
DB_NAME = "moneypi"

def save_to_mongodb(oi_data, index_name, current_timestamp):
    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]

    oi_data['current_timestamp'] = current_timestamp
    oi_data_dict = oi_data.to_dict(orient='records')

    collection = db[index_name] 
    collection.insert_many(oi_data_dict)
    print(f"Data saved to MongoDB at {current_timestamp}")

def fetch_option_chain(index_name):
    while True:
        try:
            oi_data, ltp, crontime = oi_chain_builder(index_name, "latest", "full")
            return oi_data, ltp, crontime
        except Exception as e:
            print(f"Error fetching data for {index_name}: {e}")
            time.sleep(1)

def main():
    # Define a list of specific times
    target_times = [
        (9, 15, 40), (9, 18, 40), (9, 21, 40), (9, 24, 40), (9, 27, 40), (9, 30, 40),
        (9, 33, 40), (9, 36, 40), (9, 39, 40), (9, 42, 40), (9, 45, 40), (9, 48, 40),
        (9, 51, 40), (9, 54, 40), (9, 57, 40), (10, 0, 40), (10, 3, 40), (10, 6, 40),
        (10, 9, 40), (10, 12, 40), (10, 15, 40), (10, 18, 40), (10, 21, 40), (10, 24, 40),
        (10, 27, 40), (10, 30, 40), (10, 33, 40), (10, 36, 40), (10, 39, 40), (10, 42, 40),
        (10, 45, 40), (10, 48, 40), (10, 51, 40), (10, 54, 40), (10, 57, 40), (11, 0, 40),
        (11, 3, 40), (11, 6, 40), (11, 9, 40), (11, 12, 40), (11, 15, 40), (11, 18, 40),
        (11, 21, 40), (11, 24, 40), (11, 27, 40), (11, 30, 40), (11, 33, 40), (11, 36, 40),
        (11, 39, 40), (11, 42, 40), (11, 45, 40), (11, 48, 40), (11, 51, 40), (11, 54, 40),
        (11, 57, 40), (12, 0, 40), (12, 3, 40), (12, 6, 40), (12, 9, 40), (12, 12, 40),
        (12, 15, 40), (12, 18, 40), (12, 21, 40), (12, 24, 40), (12, 27, 40), (12, 30, 40),
        (12, 33, 40), (12, 36, 40), (12, 39, 40), (12, 42, 40), (12, 45, 40), (12, 48, 40),
        (12, 51, 40), (12, 54, 40), (12, 57, 40), (13, 0, 40), (13, 3, 40), (13, 6, 40),
        (13, 9, 40), (13, 12, 40), (13, 15, 40), (13, 18, 40), (13, 21, 40), (13, 24, 40),
        (13, 27, 40), (13, 30, 40), (13, 33, 40), (13, 36, 40), (13, 39, 40), (13, 42, 40),
        (13, 45, 40), (13, 48, 40), (13, 51, 40), (13, 54, 40), (13, 57, 40), (14, 0, 40),
        (14, 3, 40), (14, 6, 40), (14, 9, 40), (14, 12, 40), (14, 15, 40), (14, 18, 40),
        (14, 21, 40), (14, 24, 40), (14, 27, 40), (14, 30, 40), (14, 33, 40), (14, 36, 40),
        (14, 39, 40), (14, 42, 40), (14, 45, 40), (14, 48, 40), (14, 51, 40), (14, 54, 40),
        (14, 57, 40), (15, 0, 40), (15, 3, 40), (15, 6, 40), (15, 9, 40), (15, 12, 40),
        (15, 15, 40), (15, 18, 40), (15, 21, 40), (15, 24, 40), (15, 27, 40), (15, 30, 40)
    ]

    while True:
        # Get option chain data for NIFTY
        nifty_oi_data, nifty_ltp, nifty_crontime = fetch_option_chain("NIFTY")

        # Get option chain data for BANKNIFTY
        banknifty_oi_data, banknifty_ltp, banknifty_crontime = fetch_option_chain("BANKNIFTY")

        # Save data for NIFTY and BANKNIFTY
        market_open = datetime.time(9, 15)
        market_close = datetime.time(15, 30)
        now = datetime.datetime.now().time()

        # Check if it's a weekend or a holiday
        today_date = datetime.datetime.now().strftime('%d-%b-%Y')
        holiday_dates = [holiday['tradingDate'] for holiday in nse_holidays()['FO']]
        if today_date in holiday_dates or datetime.datetime.now().weekday() in [5, 6]:
            print(f"Today is a weekend or a holiday. Data will not be saved.")
            time.sleep(60)  # Sleep for a minute and check again
            continue

        # Check if the market is open
        if market_open <= now <= market_close:
            current_time = (now.hour, now.minute, now.second)
            # Check if the current time is within 10 seconds after the target time for either NIFTY or BANKNIFTY
            if any(target <= current_time <= (target[0], target[1], target[2] + 10) for target in target_times):
                # Save NIFTY data
                nifty_oi_data = nifty_oi_data.drop(columns=['CALLS_Chart', 'PUTS_Chart'])
                nifty_oi_data['lastprice'] = nifty_ltp
                adjusted_nifty_timestamp = datetime.datetime.now().replace(second=40, microsecond=0).strftime('%H:%M:%S')
                save_to_mongodb(nifty_oi_data, "nifty_data", adjusted_nifty_timestamp)

                # Save BANKNIFTY data
                banknifty_oi_data = banknifty_oi_data.drop(columns=['CALLS_Chart', 'PUTS_Chart'])
                banknifty_oi_data['lastprice'] = banknifty_ltp
                adjusted_banknifty_timestamp = datetime.datetime.now().replace(second=40, microsecond=0).strftime('%H:%M:%S')
                save_to_mongodb(banknifty_oi_data, "banknifty_data", adjusted_banknifty_timestamp)
                time.sleep(10)

if __name__ == "__main__":
    main()
