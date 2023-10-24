from functools import partial

import pendulum
from airflow.decorators import task
from airflow.models.dag import dag
from airflow.operators.python import get_current_context
from airhelp.operators.slack import alert_slack_channel
from airhelp.tasks.snapshot_loader.snapshots_factory import S3SnapshotsFactory


@task(task_id="snapshot_process_metadata")
def snapshot_process_metadata():
    return S3SnapshotsFactory(
        context=get_current_context(), legacy=False
    ).snapshots.save_snapshot_process_metadata()


@task(task_id="snapshot_schema_names")
def snapshot_schema_names():
    return S3SnapshotsFactory(
        context=get_current_context(), legacy=False
    ).snapshots.save_snapshot_process_schema_names()


@dag(
    dag_id="snapshot_dags",
    description="Get metadata to create snapshot DAGs",
    start_date=pendulum.yesterday(),
    schedule="0 0/4 * * *",  # every 4 hours from 00:00 local time
    catchup=False,
    tags=["snapshot", "metadata", "s3"],
    on_failure_callback=partial(alert_slack_channel, None, None, "failed"),
)
def snapshot_dags():
    snapshot_process_metadata()
    snapshot_schema_names()


snapshot_dags_dag = snapshot_dags()
