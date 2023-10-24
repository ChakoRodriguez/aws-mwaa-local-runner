from functools import partial

import pendulum
from airflow.decorators import dag
from airhelp.operators.dynamodb import DynamoDBS3LoaderOperator
from airhelp.operators.slack import alert_slack_channel
from airhelp.sensors.dynamodb import DynamoDBExportSensor
from pendulum.tz import timezone

local_tz = timezone("Europe/Warsaw")


@dag(
    description="Load daily Aircom Stats data to Snowflake",
    start_date=pendulum.datetime(2017, 1, 1).astimezone(local_tz),
    schedule="03 6 * * *",  # 06:30 local time
    catchup=False,
    tags=["dynamodb", "snowflake"],
    on_failure_callback=partial(
        alert_slack_channel, "dynamodb", "aircom_stats", "failed"
    ),
)
def aircom_stats():
    check_aircom_stats_export = DynamoDBExportSensor(
        task_id=f"check_aircom_stats_export",
    )
    load_aircom_stats_to_snowflake = DynamoDBS3LoaderOperator(
        task_id=f"load_aircom_stats_to_snowflake",
    )

    check_aircom_stats_export >> load_aircom_stats_to_snowflake


aircom_stats_dag = aircom_stats()
