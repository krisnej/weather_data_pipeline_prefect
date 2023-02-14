import os

import pandas as pd
import requests
from sklearn.base import BaseEstimator, TransformerMixin

lat = 52.084516
lon = 5.115539
api_key = os.environ.get("API_KEY")
pickle_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pipeline.pkl")


def get_historical_temperature(date_range: pd.date_range):
    """
    This method queries the historical API and parses timestamps and temperatures to a DataFrame.

    Args:
        date_range (pandas.date_range): a range of dates to collect weather data for (max 5 days ago)
    returns:
        DataFrame with single temp column, indexed with timestamps
    """
    hourlies = []
    for t in date_range:
        time = int(t.timestamp())
        api_call = f"https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={lat}&lon={lon}&dt={time}&units=metric&appid={api_key}"
        result = requests.get(api_call)
        if result.status_code != 200:
            raise Exception(result.text)
        hourlies.append(result.json()["hourly"])

    df = (
        pd.concat([pd.DataFrame(hourly) for hourly in hourlies])
        .assign(dt=lambda df: pd.to_datetime(df["dt"], unit="s"))
        .set_index("dt")[["temp"]]
    )
    return df


class SmoothedVarCreator(BaseEstimator, TransformerMixin):
    """
    This transformer allows one to calculate an exponentially weighted moving average on the target temperatures.

    Args:
        var (str): name of the column to create moving average features for
        alpha_list (List[float]): list of alpha values to pass to pandas.series.ewm
    returns:
        DataFrame with ewm columns added
    """

    def __init__(self, var, alpha_list):
        self.var = var
        self.alpha_list = alpha_list

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        for alpha in self.alpha_list:
            func = {
                "%s_sm%s"
                % (self.var, int(alpha * 10)): lambda df: X[self.var]
                .ewm(alpha=alpha, min_periods=0)
                .mean()
            }
            X = X.assign(**func)
        return X


class LagCreator(BaseEstimator, TransformerMixin):
    """
    This transformer allows one to calculate an exponentially weighted moving average on the target temperatures.

    Args:
        var (str): name of the column to create lagged features for
        lag_list (List[float]): list of lags to create
        drop_var (bool): whether to drop the original var column at the end
    returns:
        DataFrame with lagged columns added
    """

    def __init__(self, var, lag_list, drop_var=True):
        self.var = var
        self.lag_list = lag_list
        self.drop_var = drop_var

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        for lag in self.lag_list:
            func = {"%s_lag%s" % (self.var, lag): lambda df: X[self.var].shift(lag)}
            X = X.assign(**func)
        if self.drop_var:
            X = X.drop(self.var, axis=1)
        return X


class NanDropper(TransformerMixin, BaseEstimator):
    """
    This transformer drops any rows that contain NaNs.

    returns:
        DataFrame with NaN rows dropped
    """

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return X.dropna()
