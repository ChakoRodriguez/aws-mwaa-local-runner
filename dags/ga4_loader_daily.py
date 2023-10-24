import pendulum
from airflow.models.baseoperator import chain
from airflow.models.dag import dag
from airhelp.operators.ga4 import GA4ExporterOperator, GA4ToSnowflakeOperator
from airhelp.sensors.ga4 import GA4TableExistenceSensor


def get_ga4_pool_name() -> str:
    """Get the Google Analytics 4 query pool name."""
    return "ga4"


@dag(
    dag_id="ga4_loader_daily",
    description="Load daily Google Analytics 4 data to Snowflake",
    start_date=pendulum.datetime(2017, 1, 1),
    schedule=None,
    catchup=False,
    tags=["google-analytics", "ga4", "snowflake", "big-query", "gcs"],
)
def ga4_loader_daily():
    table_exists = GA4TableExistenceSensor(
        task_id="ga4_table_exists",
        exponential_backoff=True,
        mode="reschedule",
        timeout=0,
    )

    export_table = GA4ExporterOperator(task_id="ga4_exporter", retries=1)

    ga4_to_snowflake = GA4ToSnowflakeOperator(
        task_id="ga4_to_snowflake",
        snowflake_conn_id="snowflake_conn_id",
        pool=get_ga4_pool_name(),
        retries=1,
    )

    chain(table_exists, export_table, ga4_to_snowflake)


dag_daily = ga4_loader_daily()
