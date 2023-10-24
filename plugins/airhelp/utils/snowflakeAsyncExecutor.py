import snowflake.connector as sf
from airhelp.hooks.ssm import SSMHook


class SnowflakeExecutor:
    def __init__(self):
        self.sf_connection = sf.connect(
            user=SSMHook("SNOWFLAKE_AIRFLOW_USER").get_conn(),
            password=SSMHook("SNOWFLAKE_AIRFLOW_PASSWORD").get_conn(),
            account=SSMHook("SNOWFLAKE_ACCOUNT_ID").get_conn(),
            warehouse=SSMHook("SNOWFLAKE_AIRFLOW_WAREHOUSE").get_conn(),
            schema="public",
        )

    def execute_query_async(self, query: str):
        try:
            cursor = self.sf_connection.cursor()
            cursor.execute_async(query)
        except Exception as ex:
            print(ex.message)
        return cursor

    def close(self):
        self.sf_connection.close()
