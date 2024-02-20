from flask import Flask, jsonify
from pymongo import MongoClient
from urllib.parse import quote_plus
from nsepython import *
import pandas as pd
import os
import datetime
import time

app = Flask(__name__)

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

@app.route('/nifty_data')
def get_nifty_data():
    nifty_oi_data, nifty_ltp, nifty_crontime = fetch_option_chain("NIFTY")
    nifty_oi_data = nifty_oi_data.drop(columns=['CALLS_Chart', 'PUTS_Chart'])
    nifty_oi_data['lastprice'] = nifty_ltp
    adjusted_nifty_timestamp = datetime.datetime.now().replace(second=40, microsecond=0).strftime('%H:%M:%S')
    save_to_mongodb(nifty_oi_data, "nifty_data", adjusted_nifty_timestamp)
    return jsonify({"message": "Nifty data saved successfully."})

@app.route('/banknifty_data')
def get_banknifty_data():
    banknifty_oi_data, banknifty_ltp, banknifty_crontime = fetch_option_chain("BANKNIFTY")
    banknifty_oi_data = banknifty_oi_data.drop(columns=['CALLS_Chart', 'PUTS_Chart'])
    banknifty_oi_data['lastprice'] = banknifty_ltp
    adjusted_banknifty_timestamp = datetime.datetime.now().replace(second=40, microsecond=0).strftime('%H:%M:%S')
    save_to_mongodb(banknifty_oi_data, "banknifty_data", adjusted_banknifty_timestamp)
    return jsonify({"message": "BankNifty data saved successfully."})

if __name__ == "__main__":
    app.run(debug=True)