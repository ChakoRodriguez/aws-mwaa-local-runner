import asyncio
import logging
from functools import cached_property

from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.utils.context import Context
from airhelp.hooks.google_cloud import GCPConnectionHook
from airhelp.tasks.google_analytics.config import GoogleAnalyticsConfig
from airhelp.utils.environment import Environment
from astronomer.providers.snowflake.hooks.snowflake import SnowflakeHookAsync


class GAExporterOperator(BaseOperator):
    def __init__(
        self,
        intraday: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.intraday = intraday
        self.export_format = "Parquet"
        self.compression = "SNAPPY"
        self._config = None

    @property
    def version(self) -> str:
        """Get version."""
        raise NotImplementedError

    @cached_property
    def config(self):
        """Get config."""
        return GoogleAnalyticsConfig.from_yaml(version=self.version)

    def get_parquet_filename(self, context: Context):
        """Get parquet filename."""
        date_without_hyphens = self.get_date_without_hyphens(context)
        intraday = "intraday_" if self.intraday else ""
        return f"{self.config.table_prefix}_{intraday}{date_without_hyphens}-*.parquet"

    def get_table_id(self, context: Context):
        """Get table id."""
        date_without_hyphens = self.get_date_without_hyphens(context)
        intraday = "intraday_" if self.intraday else ""
        return f"{self.config.table_prefix}_{intraday}{date_without_hyphens}"

    def get_destination_cloud_storage_uris(self, context: Context):
        """Get destination cloud storage uris."""
        return [f"{self.config.gcs_uri}{self.get_parquet_filename(context)}"]

    def get_date_without_hyphens(self, context: Context):
        """Get date without hyphens."""
        return context["logical_date"].strftime("%Y%m%d")

    def get_source_project_dataset_table(self, context: Context):
        """Get source project dataset table."""
        return f"{self.config.project_id}.{self.config.dataset_id}.{self.get_table_id(context)}"

    def execute(self, context: Context):
        """Execute the operator."""
        bq_export = BigQueryToGCSOperator(
            task_id=self.task_id,
            gcp_conn_id=GCPConnectionHook(
                conn_id=self.config.gcp_conn_id,
                project_id=self.config.project_id,
                service_account_ssm_key=self.config.service_account_ssm_key,
            ).get_conn(),
            source_project_dataset_table=self.get_source_project_dataset_table(
                context
            ),
            destination_cloud_storage_uris=self.get_destination_cloud_storage_uris(
                context
            ),
            export_format=self.export_format,
            compression=self.compression,
            force_rerun=True,
        )
        bq_export.execute(context)


class GoogleAnalyticsToSnowflakeOperator(BaseOperator):
    def __init__(
        self, snowflake_conn_id: str, intraday: bool = False, **kwargs
    ):
        super().__init__(**kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.intraday = intraday

    @property
    def version(self) -> str:
        """Get version."""
        raise NotImplementedError

    @property
    def stage(self) -> str:
        """Get stage."""
        raise NotImplementedError

    @property
    def environment(self) -> str:
        """Get environment."""
        return Environment().environment

    @cached_property
    def config(self):
        """Get config."""
        return GoogleAnalyticsConfig.from_yaml(version=self.version)

    @cached_property
    def database(self):
        """Get database name."""
        return (
            "ah_raw_dev"
            if Environment().environment == "staging"
            else "ah_raw"
        )

    def sql_from_template(self, parameters: dict) -> str:
        """Generate the load SQL statement for a table."""
        with open(self.config.template, "r", encoding="UTF-8") as template:
            return template.read().format(**parameters)

    def get_date_without_hyphens(self, context: Context):
        """Get date without hyphens."""
        return context["logical_date"].strftime("%Y%m%d")

    def get_parquet_filename_prefix(self, context: Context):
        """Get parquet filename prefix."""
        date_without_hyphens = self.get_date_without_hyphens(context)
        intraday = "intraday_" if self.intraday else ""
        return f"{self.config.table_prefix}_{intraday}{date_without_hyphens}-"

    def execute(self, context: Context):
        """Execute the operator."""
        query = self.sql_from_template(
            {
                "date": self.get_date_without_hyphens(context),
                "database": self.database,
                "stage": self.stage,
                "parquet_filename": self.get_parquet_filename_prefix(context),
            }
        )

        hook = SnowflakeHookAsync(self.snowflake_conn_id)
        query_ids = hook.run(query)
        status = asyncio.run(hook.get_query_status(query_ids, poll_interval=1))

        logging.info(f"Result: {status}")

        if status["status"] == "error":
            raise Exception(f"Query {query} failed: {status['message']}")
