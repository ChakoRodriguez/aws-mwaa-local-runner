from airflow.models.connection import Connection
from airhelp.hooks.connection import ConnectionHook
from airhelp.hooks.ssm import SSMHook


class DbtCloudConnectionHook(ConnectionHook):
    """Hook to create a dbt Cloud connection"""

    def __init__(
        self,
        conn_id: str = "dbt_cloud_conn_id",
        **kwargs,
    ) -> None:
        super().__init__(conn_id, **kwargs)

    def _create_connection(self) -> Connection:
        account_id = SSMHook("DBT_CLOUD_ACCOUNT_ID").get_conn()
        api_key = SSMHook("DBT_CLOUD_API_KEY").get_conn()

        return Connection(
            conn_id=self.conn_id,
            conn_type="dbt-cloud",
            host="",
            login=account_id,
            password=api_key,
            description="dbt cloud connection",
        )

    def _get_extra_field(self) -> dict:
        return {}
