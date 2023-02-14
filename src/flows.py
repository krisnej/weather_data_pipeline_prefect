import time
from datetime import timedelta

from prefect import context, flow

from src.temperature_forecast.predict import (create_tables,
                                              insert_actual_value,
                                              insert_forecast_value)
from src.temperature_forecast.train import (collect_data, save_pipeline,
                                            train_model)


@flow
def train():
    df = collect_data()
    pipeline = train_model(df)
    save_pipeline(pipeline)


@flow
def predict():
    execution_date = context.get_run_context().start_time
    forecast_timestamp = int(
        time.mktime((execution_date + timedelta(hours=1)).timetuple())
    )
    update_timestamp = int(time.mktime(execution_date.timetuple()))
    create_tables()
    insert_forecast_value(forecast_timestamp)
    insert_actual_value(update_timestamp)
