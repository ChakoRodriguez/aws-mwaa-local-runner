from functools import cached_property

from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor,
)
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airhelp.hooks.google_cloud import GCPConnectionHook
from airhelp.tasks.google_analytics.config import GoogleAnalyticsConfig


class GATableExistenceSensor(BaseSensorOperator):
    def __init__(self, intraday: bool = False, **kwargs):
        self.intraday = intraday
        super().__init__(
            **kwargs,
        )

    @property
    def version(self) -> str:
        raise NotImplementedError

    @cached_property
    def config(self) -> GoogleAnalyticsConfig:
        return GoogleAnalyticsConfig.from_yaml(version=self.version)

    def get_date(self, context: Context):
        """Get date from context."""
        if "logical_date" not in context:
            raise ValueError("logical_date not found in context.")
        return context["logical_date"].strftime("%Y-%m-%d")

    def get_table_id(self, context: Context):
        date_without_hyphens = self.get_date(context).replace("-", "")
        intraday = "intraday_" if self.intraday else ""
        return f"{self.config.table_prefix}_{intraday}{date_without_hyphens}"

    def poke(self, context: Context):
        sensor = BigQueryTableExistenceSensor(
            task_id="check_table_exists",
            gcp_conn_id=GCPConnectionHook(
                conn_id=self.config.gcp_conn_id,
                project_id=self.config.project_id,
                service_account_ssm_key=self.config.service_account_ssm_key,
            ).get_conn(),
            project_id=self.config.project_id,
            dataset_id=str(self.config.dataset_id),
            table_id=self.get_table_id(context),
        )
        return sensor.poke(context)
