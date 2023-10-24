from typing import List

from airhelp.tasks.snapshot_loader.snapshots import S3Snapshots


class S3SnapshotsLegacy(S3Snapshots):
    """Class to handle snapshots."""

    def get_config_path(self) -> str:
        """Get the config path."""
        return "plugins/airhelp/tasks/snapshot_loader/config_legacy.yaml"

    def get_schemas(self) -> List[str]:
        """Get all schemas with snapshots."""
        response = self.s3_datalake.list_objects_v2(
            Bucket=self.config.bucket_name,
            Prefix=self.config.prefix,
            Delimiter="/",
        )
        return sorted(
            [
                prefix["Prefix"]
                .replace(self.config.prefix, "")
                .replace("/", "")
                for prefix in response.get("CommonPrefixes", [])
            ]
        )

    def get_schema_tables(self, schema: str) -> List[str]:
        """Get all tables from a schema with snapshots."""
        response = self.s3_datalake.list_objects_v2(
            Bucket=self.config.bucket_name,
            Prefix=f"{self.config.prefix}{schema}/",
            Delimiter="/",
        )
        return sorted(
            [
                prefix["Prefix"]
                .replace(f"{self.config.prefix}{schema}/{schema}.", "")
                .replace("/", "")
                for prefix in response.get("CommonPrefixes", [])
            ]
        )

    def get_snapshot_path(self, schema: str, table: str) -> str:
        """Get the snapshot path for a table."""
        return (
            f"{self.config.prefix}{schema}/{schema}.{table}/"
            f"snapshot_date={self.get_snapshot_date()}"
        )

    def get_snapshot_schema_name(self, schema: str) -> str:
        """Get the snapshot schema name."""
        return schema.replace("-", "")

    def check_snapshot(self, schema: str, table: str) -> bool:
        """Check if snapshot exists."""
        snapshot_path = self.get_snapshot_path(schema, table)
        response = self.s3_datalake.list_objects_v2(
            Bucket=self.config.bucket_name,
            Prefix=f"{snapshot_path}/_SUCCESS",
        )
        return response.get("KeyCount", 0) > 0

    def is_schema_available(self, schema: str, date: str) -> bool:
        """Check if the schema path for a given date is available."""
        return True

    def get_snapshot_exported_at(self, schema: str, table: str) -> str:
        """Get snapshot exported_at."""
        snapshot_path = self.get_snapshot_path(schema=schema, table=table)
        return self.s3_datalake.head_object(
            Bucket=self.config.bucket_name,
            Key=f"{snapshot_path}/_SUCCESS",
        )["LastModified"].isoformat()

    def get_parquet_path(self, schema: str, table: str) -> str:
        """Get the S3 parquet path for a table."""
        snapshot_path = self.get_snapshot_path(schema, table)
        response = self.s3_datalake.list_objects_v2(
            Bucket=self.config.bucket_name,
            Prefix=f"{snapshot_path}/part-00000",
        )
        if response.get("KeyCount", 0) > 0:
            path = response["Contents"][0]["Key"]
            return f"s3://{self.config.bucket_name}/{path}"

        raise Exception(
            f"Parquet file not found for {schema}.{table}.{self.get_snapshot_date()}"
        )
