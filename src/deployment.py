import datetime

from prefect.orion.schemas.schedules import IntervalSchedule

from src.flows import train, predict_precipitation
from prefect.deployments import Deployment


schedule_daily = IntervalSchedule(
    anchor_date=datetime.datetime(2023, 2, 2, 0, 0, 0),
    interval=datetime.timedelta(hours=24)
)


schedule_every_60_seconds = IntervalSchedule(
    anchor_date=datetime.datetime.now() + datetime.timedelta(seconds=1),
    interval=datetime.timedelta(seconds=60)
)

deployment_train = Deployment.build_from_flow(
    flow=train,
    name="train",
    infra_overrides={"env": {"API_KEY": "secret_api_key"}},
    work_queue_name="test_work_queue",
    schedule=schedule_daily
)


deployment_predict = Deployment.build_from_flow(
    flow=predict_precipitation,
    name="predict_precipitation",
    infra_overrides={"env": {"API_KEY": "secret_api_key"}},
    work_queue_name="test_work_queue",
    schedule=schedule_every_60_seconds
)


if __name__ == "__main__":
    deployment_train.apply()
    deployment_predict.apply()
