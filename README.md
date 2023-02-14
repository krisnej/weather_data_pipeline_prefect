# Context

This is a batch data pipeline that processes data from a weather API on a structural basis. 
The API used is OpenWeatherMap (https://openweathermap.org/api/one-call-api). 
An API key can be generated via their website, it is free tiered and rate-limited at **60 calls per minute**.

# Deploying a temperature forecast
The aim is to create an hourly batch job that predicts the temperature for the next hour. 
An ML model can be trained on hourly historical data. 
The API's free tier returns max 5 days of history, hence this is the extent of the training set that was used.

Description of the data science code:

    ├── temperature_forecast
    │   ├── train.py        <- Script that can be used to retrain the model on the last ~5 days of API data
    │   ├── predict.py      <- Script that makes a prediction using the latest 24 hours of API data
    │   ├── utils.py        <- Helper functions & API parameters
    │   └── pipeline.pkl    <- Stored model pickle to be used for predictions

# Implementation
The solution :
* Uses a proper scheduling tool for running your batch job
* Runs the logic from `predict.py` and stores the result in a database (a single prediction each time)
* Stores the real temperature values (which will be available an hour later)
* Runs a separate training pipeline that retrains the model daily

# Architecture
[ToDo]

# How to run
Start clickhouse db: `docker-compose up -d`

Start Prefect orion server and agent:

`prefect orion start`

`prefect agent start -q 'test_work_queue'`

Build and apply deployments of existing flows (train and predict): 

`prefect deployment build ./src/flows.py:[flow-name] -n [flow-name] -q test_work_queue --override env.API_KEY=[API_KEY]`

`prefect deployment apply [flow-name]-deployment.yaml`

Then go to Prefect UI and manually run or schedule the train and predict flows.
