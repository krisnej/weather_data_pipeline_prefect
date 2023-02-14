import os

import joblib
import pandas as pd
import requests

from src.temperature_forecast.utils import get_historical_temperature, pickle_path
from src.clickhouse_connection import get_connection
from prefect import task

api_key = os.environ.get("API_KEY")
lat = 52.084516
lon = 5.115539


@task
def create_tables():
    client = get_connection()
    client.command(
        """
        CREATE TABLE IF NOT EXISTS forecast_temperatures (
            timestamp UInt32, 
            temperature Nullable(Float64), 
        ) ENGINE ReplacingMergeTree
        PRIMARY KEY timestamp
        """
    )
    client.command(
        """
        CREATE TABLE IF NOT EXISTS actual_temperatures (
            timestamp UInt32, 
            temperature Nullable(Float64), 
        ) ENGINE ReplacingMergeTree
        PRIMARY KEY timestamp
        """
    )


def predict_latest():
    """
    In order to create the model input features, 24 hours of historical temperatures are needed.
    For that reason this method takes 2 days of API history and filters to the last 24 records.
    This script only prints the prediction (an array of length 1) and does not implement any storage.
    """
    # todo add try/except to catch missing/ wrong API_KEY
    prediction_range = pd.date_range(end=pd.Timestamp.now().date(), periods=2, freq="d")
    df_pred = get_historical_temperature(prediction_range).iloc[-25:]
    pipeline = joblib.load(pickle_path)

    return pipeline.predict(df_pred)


@task(log_prints=True)
def insert_forecast_value(timestamp):
    client = get_connection()

    forecast = predict_latest()[0]
    query = f'INSERT INTO forecast_temperatures VALUES ({timestamp}, {forecast})'
    client.command(query)


def get_current_temperature(timestamp):
    api_call = (
        f"https://api.openweathermap.org/data/2.5/onecall/timemachine"
        f"?lat={lat}&lon={lon}&dt={timestamp}&units=metric"
        f"&appid={api_key}&only_current={{true}}"
    )
    result = requests.get(api_call).json()["current"]["temp"]
    return result


@task
def insert_actual_value(timestamp):
    client = get_connection()

    current_temperature = get_current_temperature(timestamp)
    query = f'INSERT INTO actual_temperatures VALUES ({timestamp}, {current_temperature})'
    client.command(query)
