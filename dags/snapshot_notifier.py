from functools import partial

import pendulum
from airflow.models.dag import dag
from airflow.models.variable import Variable
from airhelp.operators.slack import alert_slack_channel
from airhelp.sensors.snapshot import SchemaSnapshotsSensor
from pendulum.tz import timezone

local_tz = timezone("Europe/Warsaw")


def get_snapshot_process_metadata() -> dict:
    """Get the snapshot process metadata."""
    return Variable.get(
        "snapshot_process_metadata", default_var={}, deserialize_json=True
    )


def get_snapshot_loader_pool_name() -> str:
    """Get the snapshot loader query pool name."""
    return "snapshot_loader"


@dag(
    dag_id="snapshot_notifier",
    description="Notify about delayed snapshots",
    start_date=pendulum.yesterday().astimezone(local_tz),
    schedule="0 8,9 * * *",  # 08:00 and 09:00 local time
    catchup=False,
    tags=["snapshot", "s3"],
)
def snapshot_notifier():
    metadata = get_snapshot_process_metadata()
    pool = get_snapshot_loader_pool_name()

    for schema in metadata:
        SchemaSnapshotsSensor(
            task_id=f"check_snapshots_{schema}_any",
            schema=schema,
            how_many="any",
            timeout=0,
            pool=pool,
            retries=1,  # one retry to avoid Negsignal.SIGKILL error
            on_failure_callback=partial(
                alert_slack_channel,
                schema,
                None,
                "failed",
                message=f"Snapshot for `{schema}` database is missing",
            ),
        )


snapshot_notifier_dag = snapshot_notifier()
