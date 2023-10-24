import pendulum
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airhelp.tasks.snapshot_loader.snapshots_factory import S3SnapshotsFactory
from typing_extensions import Literal


class SchemaSnapshotsSensor(BaseSensorOperator):
    """Sensor to check if a schema has any or all snapshots available."""

    template_fields = "schema"

    def __init__(
        self,
        schema: str,
        how_many: Literal["any", "all"] = "any",
        legacy: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.schema = schema
        self.how_many = how_many
        self.legacy = legacy

    def poke(self, context: Context) -> bool:
        """Check if a schema has all snapshots available."""
        snapshots = S3SnapshotsFactory(
            context=context, legacy=self.legacy
        ).snapshots

        return snapshots.is_schema_available(
            self.schema,
            context.get("data_interval_end", pendulum.now()).format(
                "YYYYMMDD"
            ),
        )


class TableSnapshotSensor(BaseSensorOperator):
    """Sensor to check if the table snapshot is available."""

    template_fields = ("schema", "table")

    def __init__(
        self,
        schema: str,
        table: str,
        legacy: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.schema = schema
        self.table = table
        self.legacy = legacy

    def poke(self, context: Context) -> bool:
        """Check if the table snapshot is available."""
        snapshots = S3SnapshotsFactory(
            context=context, legacy=self.legacy
        ).snapshots
        return snapshots.check_snapshot(self.schema, self.table)
