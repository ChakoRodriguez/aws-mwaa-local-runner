import json
import logging
from abc import ABC, abstractmethod
from functools import cached_property
from typing import List, Optional, Tuple

import boto3
import pyarrow.parquet as pq
import s3fs
import yaml
from airflow.models.variable import Variable
from airflow.utils.context import Context
from airhelp.tasks.snapshot_loader.config import SnapshotsConfig
from airhelp.utils.data_types import DataTypes
from pyarrow.lib import Schema


class S3Snapshots(ABC):
    """Class to handle snapshots."""

    def __init__(self, context: Optional[Context] = None):
        """Initialize the class."""
        self.context = context
        self.config = SnapshotsConfig.from_yaml(self.get_config_path())

    @cached_property
    def s3_fs(self):
        """Get S3 filesystem."""
        return s3fs.S3FileSystem()

    @cached_property
    def s3_datalake(self):
        """Get S3 client."""
        return boto3.client("s3")

    @cached_property
    def s3_mwaa(self):
        """Get S3 client."""
        return boto3.client("s3")

    @abstractmethod
    def get_config_path(self) -> str:
        pass

    @abstractmethod
    def get_schemas(self) -> List[str]:
        pass

    @abstractmethod
    def get_schema_tables(self, schema: str) -> List[str]:
        pass

    @abstractmethod
    def get_snapshot_path(self, schema: str, table: str) -> str:
        pass

    @abstractmethod
    def get_snapshot_schema_name(self, schema: str) -> str:
        pass

    @abstractmethod
    def is_schema_available(self, schema: str, date: str) -> bool:
        pass

    def get_snapshot_date(self) -> str:
        """Get snapshot date from context."""
        return self.context["data_interval_end"].to_date_string()  # type: ignore

    def check_snapshot(self, schema: str, table: str) -> bool:
        """Check if snapshot exists."""
        snapshot_path = self.get_snapshot_path(schema, table)
        response = self.s3_datalake.list_objects_v2(
            Bucket=self.config.bucket_name,
            Prefix=f"{snapshot_path}/_SUCCESS",
        )
        return response.get("KeyCount", 0) > 0

    def get_missing_snapshots(self, schema: str) -> List[str]:
        """Get missing snapshots."""
        tables = self.get_schema_tables(schema)
        missing_tables = []
        for table in tables:
            if not self.check_snapshot(schema, table):
                missing_tables.append(table)
        return missing_tables

    @abstractmethod
    def get_parquet_path(self, schema: str, table: str) -> str:
        pass

    def get_parquet_schema(self, schema: str, table: str) -> Schema:
        """Get parquet schema."""
        s3_path = self.get_parquet_path(schema, table)
        dataset = pq.ParquetDataset(
            s3_path, filesystem=self.s3_fs, use_legacy_dataset=False
        )
        return dataset.schema

    def get_snapshot_process_blacklist(self) -> List[str]:
        """Get snapshot process blacklist."""
        return Variable.get("snapshot_process_black_list", [])

    def save_snapshot_process_metadata(self) -> None:
        """Get snapshot process metadata."""
        data = {
            schema: [table for table in self.get_schema_tables(schema)]
            for schema in self.get_schemas()
            if schema not in self.get_snapshot_process_blacklist()
        }
        Variable.set("snapshot_process_metadata", json.dumps(data))

    def save_snapshot_process_schema_names(self) -> None:
        """Get snapshot schema names."""
        data = {
            schema: self.get_snapshot_schema_name(schema)
            for schema in self.get_schemas()
            if schema not in self.get_snapshot_process_blacklist()
        }
        Variable.set("snapshot_process_schema_names", json.dumps(data))

    def get_snapshot_process_metadata_from_file(self) -> dict:
        """Get snapshot process metadata from file."""
        yaml_object = self.s3_mwaa.get_object(
            Bucket=self.config.metadata.bucket_name,
            Key=self.config.metadata.loader,
        )
        return yaml.safe_load(yaml_object["Body"].read())

    def save_sql_to_s3(self, sql: str, schema: str, table: str) -> None:
        """Save SQL to S3."""
        self.s3_mwaa.put_object(
            Bucket=self.config.metadata.bucket_name,
            Key=f"{self.config.metadata.prefix}snapshot_date={self.get_snapshot_date()}/{schema}/{table}/load.sql",
            Body=sql,
        )

    def save_metadata_to_s3(
        self, metadata: dict, schema: str, table: str
    ) -> None:
        """Save metadata to S3."""
        self.s3_mwaa.put_object(
            Bucket=self.config.metadata.bucket_name,
            Key=f"{self.config.metadata.prefix}snapshot_date={self.get_snapshot_date()}/{schema}/{table}/metadata.yaml",
            Body=yaml.dump(metadata),
        )

    def load_all_sql_from_s3(
        self,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> List[str]:
        """Get all SQL on the given path."""
        prefix = f"{self.config.metadata.prefix}snapshot_date={self.get_snapshot_date()}/"
        if schema is not None:
            prefix += f"{schema}/"
        if table is not None:
            prefix += f"{table}/"
        response = self.s3_mwaa.list_objects_v2(
            Bucket=self.config.metadata.bucket_name, Prefix=prefix
        )

        sql = []
        for s3_object in response["Contents"]:
            key = s3_object["Key"]
            if key.endswith(".sql"):
                response = self.s3_mwaa.get_object(
                    Bucket=self.config.metadata.bucket_name, Key=key
                )
                sql.append(response["Body"].read().decode("utf-8"))
        return sql

    def load_all_metadata_from_s3(
        self,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> List[dict]:
        """Get all metadata on the given path."""
        prefix = f"{self.config.metadata.prefix}snapshot_date={self.get_snapshot_date()}/"
        if schema is not None:
            prefix += f"{schema}/"
        if table is not None:
            prefix += f"{table}/"
        response = self.s3_mwaa.list_objects_v2(
            Bucket=self.config.metadata.bucket_name, Prefix=prefix
        )

        metadata = []
        for s3_object in response["Contents"]:
            key = s3_object["Key"]
            if key.endswith(".yaml"):
                response = self.s3_mwaa.get_object(
                    Bucket=self.config.metadata.bucket_name,
                    Key=key,
                )
                metadata.append(
                    yaml.load(
                        response["Body"].read().decode("utf-8"),
                        Loader=yaml.FullLoader,
                    )
                )
        return metadata

    def get_table_columns(
        self, parquet_schema: Schema
    ) -> List[Tuple[str, str]]:
        """Get table columns with and its type."""
        return [
            (field.name, DataTypes.transform(field.type))
            for field in parquet_schema
        ]

    def save_all_schemas_metadata(self):
        """Get snapshot metadata and save it to S3."""
        schemas = self.get_schemas()
        for schema in schemas:
            self.save_schema_metadata(schema)

    def save_schema_metadata(self, schema: str):
        """Get snapshot metadata and save it to S3."""
        tables = self.get_schema_tables(schema)
        for table in tables:
            self.save_table_metadata(schema, table)

    def save_table_metadata(self, schema: str, table: str):
        """Get snapshot metadata and save it to S3."""
        try:
            parquet_schema = self.get_parquet_schema(schema, table)
        except Exception as e:
            logging.error(
                f"Error getting parquet schema for {schema}.{table}: {e}"
            )
            return
        metadata = {
            "environment": self.config.environment,
            "schema": schema,
            "table": table,
            "date": self.get_snapshot_date(),
            "columns": self.get_table_columns(parquet_schema),
            "pii": self.config.get_table_pii(schema, table),
            "json_subfields_white_list": self.config.get_table_json_subfields_white_list(
                schema, table
            ),
            "snapshot_exported_at": self.get_snapshot_exported_at(
                schema, table
            ),
        }
        self.save_metadata_to_s3(metadata, schema, table)

    def sql_from_template(self, parameters: dict) -> str:
        """Generate the load SQL statement for a table."""
        with open(self.config.template, "r", encoding="UTF-8") as template:
            return template.read().format(**parameters)

    @abstractmethod
    def get_snapshot_exported_at(self, schema: str, table: str) -> str:
        pass
