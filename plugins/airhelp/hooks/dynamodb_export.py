import asyncio
import logging
from datetime import datetime
from typing import List

import boto3
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airhelp.hooks.snowflake_connection import SnowflakeConnectionHook
from airhelp.utils.environment import Environment
from pendulum.datetime import DateTime


class DynamoDBExportHook:
    """DynamoDB export hook to handle the export of the DynamoDB table."""

    def __init__(self, **kwargs) -> None:
        self.environment = Environment().environment
        self.bucket_name = self.get_bucket_name()
        self.role_arn = self.get_role_arn()
        self.snowflake_conn_id = SnowflakeConnectionHook(
            conn_id="snowflake_conn_id"
        ).get_conn()
        super().__init__(**kwargs)

    def get_aws_account_id(self) -> str:
        return (
            "555911484621"
            if self.environment == "production"
            else "064764542321"
        )

    def get_role_arn(self) -> str:
        return f"arn:aws:iam::{self.get_aws_account_id()}:role/jenkins-xrole-for-dt-{self.get_environment()}"

    def get_environment(self) -> str:
        return self.environment if self.environment == "production" else "sta"

    def get_bucket_name(self) -> str:
        return f"airhelp-datalake-{self.get_environment()}" if self.environment == "production" else f"ah-{self.get_environment()}-datalake-{self.get_environment()}"

    def is_export_available(self, date: DateTime) -> bool:
        """Checks if the export is available in the S3 bucket."""
        all_export_paths = self.get_all_exports()
        return any(
            [
                path
                for path in all_export_paths
                if self.is_timestamp_from_date(path, date)
            ]
        )

    def is_timestamp_from_date(self, path: str, date: DateTime) -> bool:
        """
        Check if the timestamp in the path is from the given date.

        Parameters:
        path (str): The file path containing the Unix timestamp.
        date (str): The date to compare against, in the format 'YYYY-MM-DD'.

        Returns:
        bool: True if the timestamp in the path is from the given date, False otherwise.
        """
        # Extract the Unix timestamp from the path
        try:
            timestamp_str = path.split("/")[1].split("-")[0]
            timestamp = (
                int(timestamp_str) / 1000
            )  # Convert from milliseconds to seconds
        except (ValueError, IndexError):
            print("Invalid path format. Unable to extract timestamp.")
            return False

        # Convert the Unix timestamp to a date string
        timestamp_date = datetime.utcfromtimestamp(timestamp)

        # Compare the extracted date with the input date
        return date.is_same_day(timestamp_date)

    def get_all_exports(self) -> List[str]:
        """Gets the last export path from the S3 bucket."""

        # Create an STS client
        sts_client = boto3.client("sts")

        # Assume the given role
        assumed_role_object = sts_client.assume_role(
            RoleArn=self.role_arn, RoleSessionName="AssumeRoleSession1"
        )

        # Extract the temporary credentials from the assumed role
        credentials = assumed_role_object["Credentials"]

        # Create an S3 client using the temporary credentials
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

        response = s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=f"AWSDynamoDB/",
            Delimiter="/",
        )

        exports = [item["Prefix"] for item in response.get("CommonPrefixes")]

        return exports

    def get_export_from_date(self, day: DateTime):
        all_exports = self.get_all_exports()
        for export in all_exports:
            if self.is_timestamp_from_date(export, day):
                return export
        return None

    def sql_from_template(self, parameters: dict) -> str:
        """Generate the load SQL statement for a table."""
        with open(
            "plugins/airhelp/tasks/aircom_stats/template/load_from_s3.sql",
            "r",
            encoding="UTF-8",
        ) as template:
            return template.read().format(**parameters)

    def get_database(self, environment: str) -> str:
        return "AH_RAW" if environment == "production" else "AH_RAW_DEV"

    def get_stage(self, environment: str) -> str:
        return (
            "S3_CSV_PRODUCTION_STAGE"
            if environment == "production"
            else "S3_CSV_STA_STAGE"
        )

    def file_format(self, environment: str) -> str:
        return (
            "JSONL_DT_PRODUCTION"
            if environment == "production"
            else "JSONL_DT_STAGING"
        )

    def load_from_date(self, day: DateTime) -> None:
        environment = Environment().environment
        database = self.get_database(environment)
        stage = self.get_stage(environment)
        file_format = self.file_format(environment)
        location = self.get_export_from_date(day)

        queries = self.sql_from_template(
            {
                "database": database,
                "stage": stage,
                "location": location,
                "file_format": file_format,
            }
        )

        # in order to run queries sequentially, we need to pass them as a list
        query_list = [query.strip() for query in queries.split(";") if query.strip() != ""]

        hook = SnowflakeHook(self.snowflake_conn_id)
        hook.run(query_list)

        logging.info(f"Loaded export from {location} to Snowflake")
