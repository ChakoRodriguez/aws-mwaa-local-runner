from functools import partial

import pendulum
from airflow.decorators import dag
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.trigger_rule import TriggerRule
from airhelp.hooks.dbt_cloud import DbtCloudConnectionHook
from airhelp.operators.slack import alert_slack_channel
from airhelp.operators.snapshot import (
    SnapshotMetadataOperator,
    SnapshotSnowflakeOperator,
)
from airhelp.sensors.snapshot import SchemaSnapshotsSensor
from pendulum.tz import timezone

local_tz = timezone("Europe/Warsaw")


def get_snapshot_loader_pool_name() -> str:
    """Get the snapshot loader query pool name."""
    return "snapshot_loader"


def get_snapshot_process_metadata() -> dict:
    """Get the snapshot process metadata."""
    try:
        return Variable.get("snapshot_process_metadata", deserialize_json=True)
    except KeyError:
        return {}


def get_snapshot_process_schema_names() -> dict:
    """Get the snapshot process metadata."""
    try:
        return Variable.get(
            "snapshot_process_schema_names", deserialize_json=True
        )
    except KeyError:
        return {}


def get_snapshot_schema_name(schema: str) -> dict:
    """Get the snapshot process metadata."""
    schemas = get_snapshot_process_schema_names()
    return schemas.get(schema, schema)


def get_environment() -> str:
    """Get the environment."""
    return Variable.get("environment", "staging")


def get_database_name():
    """Get the database name."""
    return "ah_raw" if get_environment() == "production" else "ah_raw_dev"


def get_grant_usage_on_database(database: str, role: str) -> str:
    """Get the grant usage SQL statements."""
    return f"grant select on all tables in database {database} to role {role};"


@dag(
    dag_id="snapshot_loader",
    description="Load all snapshots to Snowflake",
    start_date=pendulum.yesterday().astimezone(local_tz),
    schedule="0 2 * * *",  # 02:00 local time
    catchup=False,
    tags=["snapshot", "snowflake", "s3"],
    on_failure_callback=partial(
        alert_slack_channel,
        "snapshot_loader",
        None,
        "failed",
        message="Some snapshots failed to load to Snowflake due to an error",
    ),
    on_success_callback=partial(
        alert_slack_channel,
        "snapshot_loader",
        None,
        "success",
        message="All snapshots successfully loaded to Snowflake",
    ),
)
def snapshot_loader():
    """Load all snapshots to Snowflake."""

    metadata = get_snapshot_process_metadata()
    database = get_database_name()
    pool = get_snapshot_loader_pool_name()

    dbt_cloud_conn_id = DbtCloudConnectionHook().get_conn()

    if get_environment() == "production":
        deploy_dbt = DbtCloudRunJobOperator(
            task_id="deploy_dbt_models",
            dbt_cloud_conn_id=dbt_cloud_conn_id,
            job_id=340801,
            wait_for_termination=False,
            trigger_rule=TriggerRule.ALL_DONE,
            on_failure_callback=partial(
                alert_slack_channel, None, None, "failed"
            ),
        )
    else:
        deploy_dbt = EmptyOperator(
            task_id="deploy_dbt_models", trigger_rule=TriggerRule.ALL_DONE
        )

    for schema in metadata:
        check_schema = SchemaSnapshotsSensor(
            task_id=f"check_snapshots_{schema}_any",
            schema=schema,
            how_many="any",
            exponential_backoff=True,
            mode="reschedule",
            retries=1,
            pool=pool,
            on_failure_callback=partial(
                alert_slack_channel, schema, None, "failed"
            ),
        )

        load_schema_metadata = SnapshotMetadataOperator(
            task_id=f"save_{schema}_metadata",
            schema=schema,
            retries=1,
            pool=pool,
            trigger_rule=TriggerRule.ALL_DONE,
            on_failure_callback=partial(
                alert_slack_channel, schema, None, "failed"
            ),
        )

        load_schema_snapshots = SnapshotSnowflakeOperator(
            task_id=f"load_snapshots_{schema}",
            snowflake_conn_id="snowflake_conn_id",
            schema=schema,
            trigger_rule=TriggerRule.ALL_DONE,
            retries=1,
            pool=pool,
            on_failure_callback=partial(
                alert_slack_channel, schema, None, "failed"
            ),
        )

        # schema snapshot has arrived and its loaded to Snowflake
        (check_schema >> load_schema_metadata >> load_schema_snapshots)

        # run dbt models
        load_schema_snapshots >> deploy_dbt

        if get_environment() == "production":
            clone_schema_eng = SnowflakeOperator(
                task_id=f"clone_schema_eng_{schema}",
                snowflake_conn_id="snowflake_conn_id",
                sql=f"create or replace schema {database}_eng.{get_snapshot_schema_name(schema)} clone {database}.{get_snapshot_schema_name(schema)}_airflow",
                retries=1,
                pool=pool,
                on_failure_callback=partial(
                    alert_slack_channel, schema, None, "failed"
                ),
            )
            grant_privileges_schema_eng = SnowflakeOperator(
                task_id=f"grant_privileges_schema_eng_{schema}",
                snowflake_conn_id="snowflake_conn_id",
                sql=get_grant_usage_on_database(
                    f"{database}_eng", "reporter_eng"
                ),
                retries=1,
                pool=pool,
                on_failure_callback=partial(
                    alert_slack_channel, schema, None, "failed"
                ),
            )
            (
                load_schema_snapshots
                >> clone_schema_eng
                >> grant_privileges_schema_eng
            )


snapshot_loader_dag = snapshot_loader()
