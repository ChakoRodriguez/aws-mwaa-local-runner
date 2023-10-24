from functools import cached_property
from typing import List

import boto3
import pendulum
import s3fs
from airhelp.tasks.snapshot_loader.snapshots import S3Snapshots


class S3SnapshotsDataLake(S3Snapshots):
    """Class to handle snapshots."""

    @cached_property
    def client_kwargs(self) -> dict:
        """Get client kwargs."""
        session = boto3.client("sts")
        response = session.assume_role(
            RoleArn=self.config.assumed_role,
            RoleSessionName="airflow-snapshots",
        )
        return {
            "aws_access_key_id": response["Credentials"]["AccessKeyId"],
            "aws_secret_access_key": response["Credentials"][
                "SecretAccessKey"
            ],
            "aws_session_token": response["Credentials"]["SessionToken"],
        }

    @cached_property
    def s3_fs(self):
        """Get S3 filesystem."""
        client_kwargs = self.client_kwargs
        return s3fs.S3FileSystem(
            key=client_kwargs["aws_access_key_id"],
            secret=client_kwargs["aws_secret_access_key"],
            token=client_kwargs["aws_session_token"],
        )

    @cached_property
    def s3_datalake(self):
        """Get S3 client."""
        client_kwargs = self.client_kwargs
        new_session = boto3.Session(
            aws_access_key_id=client_kwargs["aws_access_key_id"],
            aws_secret_access_key=client_kwargs["aws_secret_access_key"],
            aws_session_token=client_kwargs["aws_session_token"],
        )
        return new_session.client("s3")

    def get_config_path(self) -> str:
        """Get the config path."""
        return "plugins/airhelp/tasks/snapshot_loader/config.yaml"

    def get_environment_suffix(self) -> str:
        """Get the environment suffix."""
        return f"-{self.config.environment.replace('staging', 'sta')}"

    def get_schemas(self) -> List[str]:
        """Get all schemas with snapshots."""
        response = self.s3_datalake.list_objects_v2(
            Bucket=self.config.bucket_name,
            Delimiter="/",
        )
        return sorted(
            [
                prefix["Prefix"]
                .replace(self.get_environment_suffix(), "")
                .replace("/", "")
                for prefix in response.get("CommonPrefixes", [])
            ]
        )

    def last_schema_snapshot_path(self, schema: str) -> str:
        """Get the last schema snapshot path."""
        response = self.s3_datalake.list_objects_v2(
            Bucket=self.config.bucket_name,
            Prefix=f"{schema}{self.get_environment_suffix()}/",
            Delimiter="/",
        )
        sorted_snapshots = sorted(
            [prefix["Prefix"] for prefix in response.get("CommonPrefixes", [])]
        )

        return sorted_snapshots[-1]

    def get_last_snapshot_path(self, schema: str):
        """Get the last snapshot path for a schema."""
        last_snapshot = self.last_schema_snapshot_path(schema)
        response = self.s3_datalake.list_objects_v2(
            Bucket=self.config.bucket_name,
            Prefix=f"{last_snapshot}",
            Delimiter="/",
        )
        return response.get("CommonPrefixes")[0]["Prefix"][:-1]

    def is_schema_available(self, schema: str, date: str) -> bool:
        """Check if the schema path for a given date is available."""
        last_snapshot_path = self.last_schema_snapshot_path(schema)
        if not date in last_snapshot_path:
            return False
        return self.is_sentinel_available(last_snapshot_path)

    def is_sentinel_available(self, last_snapshot_path: str) -> bool:
        """Check if the sentinel file for the snapshot export is available."""
        if self.is_export_info_available(last_snapshot_path):
            return True
        if self.is_export_tables_info_available(last_snapshot_path):
            return True
        return False

    def is_export_info_available(self, snapshot_path: str) -> bool:
        """Check if the export_info is available."""
        response = self.s3_datalake.list_objects_v2(
            Bucket=self.config.bucket_name,
            Prefix=f"{snapshot_path}export_info",
            Delimiter="/",
        )
        return response.get("KeyCount", 0) > 0

    def is_export_tables_info_available(self, snapshot_path: str) -> bool:
        """
        Check if the export_tables_info files are available.
        This function is a proxy to detect if the snapshot has been created when
        the export_info is not available. It checks that all the export_tables_info
        files have been created more than 1 hour ago and assumes that the snapshot
        has been created. It's a temporary solution until we have a better way to
        detect it.
        """
        response = self.s3_datalake.list_objects_v2(
            Bucket=self.config.bucket_name,
            Prefix=f"{snapshot_path}export_tables_info",
            Delimiter="/",
        )
        # detect if all the files have been created more than 1 hour ago
        if response.get("KeyCount", 0) > 0:
            last_modified = [
                file["LastModified"] for file in response["Contents"]
            ]
            return max(last_modified) < (pendulum.now().subtract(hours=1))
        return False

    def get_schema_tables(self, schema: str) -> List[str]:
        """Get all tables from a schema with snapshots."""
        last_snapshot_path = self.get_last_snapshot_path(schema)

        response = self.s3_datalake.list_objects_v2(
            Bucket=self.config.bucket_name,
            Prefix=f"{last_snapshot_path}/",
            Delimiter="/",
        )

        return sorted(
            [
                prefix["Prefix"]
                .replace(f"{last_snapshot_path}/public.", "")
                .replace("/", "")
                for prefix in response.get("CommonPrefixes", [])
            ]
        )

    def get_snapshot_path(self, schema: str, table: str) -> str:
        """Get the snapshot path for a table."""
        last_snapshot_path = self.get_last_snapshot_path(schema)
        return f"{last_snapshot_path}/public.{table}"

    def get_snapshot_schema_name(self, schema: str) -> str:
        return self.get_last_snapshot_path(schema).split("/")[-1]

    def get_snapshot_exported_at(self, schema: str, table: str) -> str:
        """Get snapshot exported_at."""
        snapshot_path = self.get_snapshot_path(schema=schema, table=table)
        return self.s3_datalake.head_object(
            Bucket=self.config.bucket_name,
            Key=f"{snapshot_path}/1/_SUCCESS",
        )["LastModified"].isoformat()

    def get_parquet_path(self, schema: str, table: str) -> str:
        """Get the S3 parquet path for a table."""
        snapshot_path = self.get_snapshot_path(schema, table)
        response = self.s3_datalake.list_objects_v2(
            Bucket=self.config.bucket_name,
            Prefix=f"{snapshot_path}/1/part-00000",
        )
        if response.get("KeyCount", 0) > 0:
            path = response["Contents"][0]["Key"]
            return f"s3://{self.config.bucket_name}/{path}"

        raise Exception(
            f"Parquet file not found for {schema}.{table}.{self.get_snapshot_date()}"
        )
