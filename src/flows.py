import time
from datetime import timedelta

from prefect import flow, context

from src.temperature_forecast.predict import (
    create_tables,
    insert_forecast_value,
    update_actual_value,
)
from src.temperature_forecast.train import collect_data, train_model, save_pipeline


@flow
def predict_precipitation():
    execution_date = context.get_run_context().start_time
    forecast_timestamp = int(
        time.mktime((execution_date + timedelta(hours=1)).timetuple())
    )
    update_timestamp = int(time.mktime(execution_date.timetuple()))
    create_tables()
    insert_forecast_value(forecast_timestamp)
    update_actual_value(update_timestamp)


@flow
def train():
    df = collect_data()
    pipeline = train_model(df)
    save_pipeline(pipeline)


if __name__ == "__main__":
    train()
    predict_precipitation()
