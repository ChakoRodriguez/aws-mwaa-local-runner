import asyncio
import logging
from typing import List, Optional

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airhelp.hooks.snowflake_connection import SnowflakeConnectionHook
from airhelp.tasks.snapshot_loader.snapshots_factory import S3SnapshotsFactory
from astronomer.providers.snowflake.hooks.snowflake import SnowflakeHookAsync

# List of Snowflake reserved words
# https://docs.snowflake.com/en/sql-reference/reserved-keywords.html
SNOWFLAKE_RESERVED_WORDS = [
    "account",
    "all",
    "alter",
    "and",
    "any",
    "as",
    "between",
    "by",
    "case",
    "cast",
    "check",
    "column",
    "connect",
    "connection",
    "constraint",
    "create",
    "cross",
    "current",
    "current_date",
    "current_time",
    "current_timestamp",
    "current_user",
    "database",
    "delete",
    "distinct",
    "drop",
    "else",
    "exists",
    "false",
    "following",
    "for",
    "from",
    "full",
    "grant",
    "group",
    "gscluster",
    "having",
    "ilike",
    "in",
    "increment",
    "inner",
    "insert",
    "intersect",
    "into",
    "is",
    "issue",
    "join",
    "lateral",
    "left",
    "like",
    "localtime",
    "localtimestamp",
    "minus",
    "natural",
    "not",
    "null",
    "of",
    "on",
    "or",
    "order",
    "organization",
    "qualify",
    "regexp",
    "revoke",
    "right",
    "rlike",
    "row",
    "rows",
    "sample",
    "schema",
    "select",
    "set",
    "some",
    "start",
    "table",
    "tablesample",
    "then",
    "to",
    "trigger",
    "true",
    "try_cast",
    "union",
    "unique",
    "update",
    "using",
    "values",
    "view",
    "when",
    "whenever",
    "where",
    "with",
]


class SnapshotMetadataOperator(BaseOperator):
    """Operator to save snapshot metadata to S3."""

    def __init__(
        self,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        legacy: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.schema = schema
        self.table = table
        self.legacy = legacy

    def execute(self, context: Context):
        """Execute the operator."""
        snapshots = S3SnapshotsFactory(
            context=context, legacy=self.legacy
        ).snapshots
        if self.schema is None:
            return snapshots.save_all_schemas_metadata()
        if self.table is None:
            return snapshots.save_schema_metadata(self.schema)
        return snapshots.save_table_metadata(self.schema, self.table)


class SnapshotSnowflakeOperator(BaseOperator):
    """Operator to execute SQL statements in Snowflake."""

    def __init__(
        self,
        snowflake_conn_id: str,
        run_asynchronous: bool = False,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        legacy: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.run_asynchronous = run_asynchronous
        self.schema = schema
        self.table = table
        self.legacy = legacy

    def get_snowflake_columns(
        self,
        fields: dict,
        pii: List[str],
        json_subfields_white_list: List[str],
    ) -> str:
        """Generate columns on the COPY INTO SQL statement for a table."""
        columns = [
            f"$1:{field}" if field not in pii else f"md5($1:{field})"
            for field, _ in fields
        ]
        for field in json_subfields_white_list:
            columns.append(self.get_snowflake_json_column(field))
        return ", ".join(columns)

    def get_snowflake_json_column(self, field_name: str) -> str:
        """Generate a Snowflake SQL column for selecting a JSON subfield from a table."""
        parts = field_name.split(".")
        json_selector = f"parse_json($1:{parts[0]})"
        for idx in range(1, len(parts)):
            json_selector += f":{parts[idx]}"
        return f"{json_selector}::varchar"

    def get_snowflake_json_column_names(self, field_name: str) -> str:
        """Generate a Snowflake SQL column for selecting a JSON subfield from a table."""
        return ",".join(field_name.split("."))

    def escape_reserved_words(self, column: str) -> str:
        """Escape reserved words with double quotes."""
        if column.lower() in SNOWFLAKE_RESERVED_WORDS:
            return f'"{column.upper()}"'
        return column

    def execute(self, context: Context):
        snowflake_conn_id = SnowflakeConnectionHook(
            conn_id=self.snowflake_conn_id
        ).get_conn()

        snapshots = S3SnapshotsFactory(
            context=context, legacy=self.legacy
        ).snapshots
        metadata = snapshots.load_all_metadata_from_s3(self.schema, self.table)

        queries = []

        queries.append(
            f"use warehouse {snapshots.config.snowflake_warehouse_name};"
        )

        for metadatum in metadata:
            logging.info("Loading metadata: %s", metadatum)
            column_names = [
                f"{self.escape_reserved_words(column)} {data_type}"
                for column, data_type in metadatum["columns"]
            ]
            column_names.extend(
                [
                    f"{field.replace('.', '_')} VARCHAR"
                    for field in metadatum["json_subfields_white_list"]
                ]
            )

            snapshot_path = snapshots.get_snapshot_path(
                schema=metadatum["schema"], table=metadatum["table"]
            )

            query = snapshots.sql_from_template(
                {
                    "schema_name": snapshots.get_snapshot_schema_name(
                        metadatum["schema"]
                    ),
                    "table_name": metadatum["table"],
                    "column_names": ", ".join(column_names),
                    "copy_columns": self.get_snowflake_columns(
                        metadatum["columns"],
                        metadatum["pii"],
                        metadatum["json_subfields_white_list"],
                    ),
                    "snapshot_exported_at": metadatum["snapshot_exported_at"],
                    "stage_name": snapshots.config.snowflake_stage_name,
                    "snapshot_path": snapshot_path,
                }
            )
            snapshots.save_sql_to_s3(
                query, metadatum["schema"], metadatum["table"]
            )

            queries.append(query)

        if self.run_asynchronous:
            return self.run_async(snowflake_conn_id, queries)

        self.run_sync(snowflake_conn_id, queries)

    def run_async(self, snowflake_conn_id: str, queries: List[str]) -> list:
        """Run a SQL script asynchronously in Snowflake."""
        hook = SnowflakeHookAsync(snowflake_conn_id)
        query_ids = hook.run(queries)
        return query_ids

    def run_sync(self, snowflake_conn_id: str, queries: List[str]) -> None:
        """Run a SQL script synchronously in Snowflake."""
        hook = SnowflakeHookAsync(snowflake_conn_id)
        query_ids = hook.run(queries)
        status = asyncio.run(
            hook.get_query_status(query_ids, poll_interval=10)
        )

        logging.info(f"Result: {status}")

        if status["status"] == "error":
            if (
                status["message"]
                == "Error parsing the parquet file: Invalid: Parquet file size is 0 bytes"
            ):
                logging.warning(
                    "Ignoring error: %s on query %s",
                    status["message"],
                    queries,
                )
            raise Exception(f"Query {queries} failed: {status['message']}")
