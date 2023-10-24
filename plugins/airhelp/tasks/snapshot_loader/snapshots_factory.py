from airflow.utils.context import Context
from airhelp.tasks.snapshot_loader.snapshots import S3Snapshots
from airhelp.tasks.snapshot_loader.snapshots_datalake import (
    S3SnapshotsDataLake,
)
from airhelp.tasks.snapshot_loader.snapshots_legacy import S3SnapshotsLegacy


class S3SnapshotsFactory:
    """Factory to get snapshots for a given context."""

    def __init__(self, context: Context, legacy: bool) -> None:
        self.context = context
        self.legacy = legacy

    @property
    def snapshots(self) -> S3Snapshots:
        if self.legacy:
            return S3SnapshotsLegacy(context=self.context)
        else:
            return S3SnapshotsDataLake(context=self.context)
