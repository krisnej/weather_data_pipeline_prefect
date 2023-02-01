import joblib
import pandas as pd
from prefect import task
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.pipeline import Pipeline
from src.temperature_forecast.utils import (
    LagCreator,
    NanDropper,
    SmoothedVarCreator,
    get_historical_temperature,
    pickle_path,
)


@task
def collect_data():
    """
    This method collects the last 5 days (plus today's hours up to current moment) of temperatures.
    """
    historical_range = pd.date_range(end=pd.Timestamp.now().date(), periods=6, freq="d")
    return get_historical_temperature(historical_range)


@task
def train_model(df, window=1):
    """
    This method builds the training pipeline needed to fit the temperature model.
    The sklearn pipeline includes the estimator, so that class can be used directly for predictions.
    The trimming logic is to make sure the y values align correctly with the X features.
    """

    trim_y = lambda y: y.shift(-window).iloc[24:].dropna()
    trim_x = lambda x: x.iloc[:-window]

    pipeline = Pipeline(
        [
            ("svc", SmoothedVarCreator(var="temp", alpha_list=[0.3, 0.1])),
            ("lc", LagCreator(var="temp", lag_list=[0, 1, 2, 23, 24])),
            ("nd", NanDropper()),
            ("gbr", GradientBoostingRegressor(random_state=42)),
        ]
    )

    pipeline.fit(trim_x(df), trim_y(df["temp"]))
    return pipeline


@task
def save_pipeline(pipeline):
    joblib.dump(pipeline, pickle_path)


if __name__ == "__main__":
    df = collect_data()
    pipeline = train_model(df)
    save_pipeline(pipeline)
