from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airhelp.hooks.dynamodb_export import DynamoDBExportHook


class DynamoDBExportSensor(BaseSensorOperator):
    """Sensor to check if the DynamoDB export is available."""

    def __init__(self, **kwargs) -> None:
        self.dynamodb_export_hook = DynamoDBExportHook()
        super().__init__(**kwargs)

    def poke(self, context: Context) -> bool:
        date = context["logical_date"]
        return self.dynamodb_export_hook.is_export_available(date)
