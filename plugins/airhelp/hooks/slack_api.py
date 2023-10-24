from airflow.models.connection import Connection
from airhelp.hooks.connection import ConnectionHook
from airhelp.hooks.ssm import SSMHook


class SlackConnectionHook(ConnectionHook):
    """Hook to create a Slack connection"""

    def __init__(
        self,
        conn_id: str = "slack_api_default",
        **kwargs,
    ) -> None:
        super().__init__(conn_id, **kwargs)

    @property
    def password(self) -> str:
        return SSMHook("ELT_SLACK_TOKEN").get_conn()

    def _create_connection(self) -> Connection:
        """Create a connection"""
        return Connection(
            conn_id="slack_api_default",
            conn_type="slack",
            description="Slack connection",
            password=self.password,
        )

    def _get_extra_field(self) -> dict:
        return {}
