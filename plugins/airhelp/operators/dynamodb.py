import asyncio
import logging
from typing import List, Optional

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airhelp.hooks.dynamodb_export import DynamoDBExportHook
from airhelp.hooks.snowflake_connection import SnowflakeConnectionHook
from airhelp.tasks.snapshot_loader.snapshots_factory import S3SnapshotsFactory
from astronomer.providers.snowflake.hooks.snowflake import SnowflakeHookAsync


class DynamoDBS3LoaderOperator(BaseOperator):
    """Operator to load DynamoDB export from S3 to Snowflake."""

    def execute(self, context: Context):
        """Execute the operator."""
        hook = DynamoDBExportHook()
        # Get the date from the context
        date = context["logical_date"]
        # Check if the export is available
        if not hook.is_export_available(date):
            logging.info(f"Export for {date} not available yet")
            return
        # Load the export
        hook.load_from_date(date)
