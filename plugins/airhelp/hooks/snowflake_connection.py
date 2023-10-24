from airflow.models.connection import Connection
from airhelp.hooks.connection import ConnectionHook
from airhelp.hooks.ssm import SSMHook


class SnowflakeConnectionHook(ConnectionHook):
    """Hook to create a Snowflake connection"""

    def __init__(
        self,
        conn_id: str = "snowflake_conn_id",
        **kwargs,
    ) -> None:
        super().__init__(conn_id, **kwargs)

    @property
    def user(self) -> str:
        return SSMHook("SNOWFLAKE_AIRFLOW_USER").get_conn()

    @property
    def password(self) -> str:
        return SSMHook("SNOWFLAKE_AIRFLOW_PASSWORD").get_conn()

    @property
    def account(self) -> str:
        return SSMHook("SNOWFLAKE_ACCOUNT_ID").get_conn()

    @property
    def role(self) -> str:
        return SSMHook("SNOWFLAKE_AIRFLOW_ROLE").get_conn()

    @property
    def warehouse(self) -> str:
        return SSMHook("SNOWFLAKE_AIRFLOW_WAREHOUSE").get_conn()

    @property
    def database(self) -> str:
        return SSMHook("SNOWFLAKE_AIRFLOW_DATABASE").get_conn()

    @property
    def region(self) -> str:
        return SSMHook("SNOWFLAKE_AIRFLOW_REGION").get_conn()

    def _create_connection(self) -> Connection:
        """Create a connection"""
        return Connection(
            conn_id=self.conn_id,
            conn_type="snowflake",
            description="Snowflake connection",
            host="snowflakecomputing.com",
            login=self.user,
            password=self.password,
            port=443,
        )

    def _get_extra_field(self) -> dict:
        return {
            "extra__snowflake__account": self.account,
            "extra__snowflake__database": self.database,
            "extra__snowflake__region": self.region,
            "extra__snowflake__role": self.role,
            "extra__snowflake__warehouse": self.warehouse,
        }
