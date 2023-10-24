import pendulum
from airflow.models.baseoperator import chain
from airflow.models.dag import dag
from airhelp.operators.ga360 import (
    GA360ExporterOperator,
    GA360ToSnowflakeOperator,
)
from airhelp.sensors.ga360 import GA360TableExistenceSensor
from pendulum.tz import timezone

local_tz = timezone("Europe/Warsaw")


def get_ga360_pool_name() -> str:
    """Get the Google Analytics query pool name."""
    return "ga360"


@dag(
    dag_id="ga360_loader_intraday",
    description="Load intraday Google Analytics data to Snowflake",
    start_date=pendulum.datetime(2017, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google-analytics", "ga360", "snowflake", "big-query", "gcs"],
)
def ga360_loader_intraday():
    table_exists = GA360TableExistenceSensor(
        task_id="ga360_table_exists",
        exponential_backoff=True,
        mode="reschedule",
        timeout=0,
        intraday=True,
    )

    export_table = GA360ExporterOperator(
        task_id="ga360_exporter", retries=1, intraday=True
    )

    ga360_to_snowflake = GA360ToSnowflakeOperator(
        task_id="ga360_to_snowflake",
        snowflake_conn_id="snowflake_conn_id",
        pool=get_ga360_pool_name(),
        retries=1,
        intraday=True,
    )

    chain(table_exists, export_table, ga360_to_snowflake)


dag_daily = ga360_loader_intraday()
