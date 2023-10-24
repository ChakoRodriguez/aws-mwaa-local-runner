from functools import partial

import pendulum
from airflow.api.common.experimental.pool import create_pool, get_pool
from airflow.decorators import task
from airflow.exceptions import PoolNotFound
from airflow.models.dag import dag
from airflow.models.variable import Variable
from airhelp.hooks.dbt_cloud import DbtCloudConnectionHook
from airhelp.hooks.google_cloud import GCPConnectionHook
from airhelp.hooks.slack_api import SlackConnectionHook
from airhelp.hooks.snowflake_connection import SnowflakeConnectionHook
from airhelp.operators.slack import alert_slack_channel
from airhelp.tasks.google_analytics.config import GoogleAnalyticsConfig


def create_ga_connection(version: str):
    config = GoogleAnalyticsConfig.from_yaml(version=version)
    GCPConnectionHook(
        conn_id=config.gcp_conn_id,
        project_id=config.project_id,
        service_account_ssm_key=config.service_account_ssm_key,
    ).get_conn(force=True)


@task(task_id="create_dbt_cloud_connection")
def create_dbt_cloud_connection():
    DbtCloudConnectionHook().get_conn(force=True)


@task(task_id="create_google_cloud_connection")
def create_google_cloud_connection():
    create_ga_connection("ga360")
    create_ga_connection("ga4")


@task(task_id="create_snowflake_connection")
def create_snowflake_connection():
    SnowflakeConnectionHook().get_conn(force=True)


@task(task_id="create_slack_connection")
def create_slack_connection():
    SlackConnectionHook().get_conn(force=True)


def create_dag_pool(pool_name: str, pool_slots: int):
    try:
        get_pool(name=pool_name)
    except PoolNotFound:
        create_pool(
            pool_name,
            Variable.get(
                f"{pool_name}_pool_slots",
                default_var=pool_slots,
            ),
            description=f"Number of slots in the {pool_name} pool",
        )


@task(task_id="create_snapshot_loader_pool")
def create_snapshot_loader_pool():
    create_dag_pool("snapshot_loader", 32)


@task(task_id="create_google_analytics_pool")
def create_google_analytics_pool():
    create_dag_pool("ga360", 1)
    create_dag_pool("ga4", 1)


@dag(
    dag_id="initialize_airflow",
    description="Create all connections and pools needed for the Airflow DAGs",
    start_date=pendulum.yesterday(),
    schedule="@once",
    catchup=False,
    tags=["dbt cloud", "google cloud", "snowflake", "pools"],
    on_failure_callback=partial(alert_slack_channel, None, None, "failed"),
)
def initialize_airflow():
    create_dbt_cloud_connection()
    create_google_cloud_connection()
    create_snowflake_connection()
    create_snapshot_loader_pool()
    create_google_analytics_pool()
    create_slack_connection()


initialize_airflow_dag = initialize_airflow()
