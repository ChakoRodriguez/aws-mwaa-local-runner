from typing import Optional

from airflow.models.baseoperator import BaseOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.context import Context
from airhelp.utils.environment import Environment


def alert_slack_channel(
    source_name: Optional[str],
    table_name: Optional[str],
    status: str,
    context: Context,
    message: Optional[str] = None,
):
    task_instance = context.get("task_instance")
    DataTeamMonitorSlackOperator(
        task_id="notify_slack_channel",
        dag_name=task_instance.dag_id,
        task_name=task_instance.task_id,
        execution_time=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
        source_name=source_name,
        table_name=table_name,
        status=status,
        message=message or context.get("exception") or context.get("reason"),
    ).execute(context=context)


class DataTeamMonitorSlackOperator(BaseOperator):
    def __init__(
        self,
        dag_name: str,
        task_name: str,
        execution_time: str,
        log_url: str,
        source_name: Optional[str],
        table_name: Optional[str],
        status: str,
        message: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.dag_name = dag_name
        self.task_name = task_name
        self.execution_time = execution_time
        self.log_url = log_url
        self.source_name = source_name
        self.table_name = table_name
        self.status = status
        self.message = message

    def execute(self, context):
        SlackAPIPostOperator(
            task_id=self.task_id,
            slack_conn_id=self._get_slack_conn_id(),
            channel=self._get_channel(),
            icon_url=self._get_icon_url(),
            attachments=self._get_attachments(),
            text=self._get_message(),
        ).execute(context=context)

    def _get_slack_conn_id(self):
        """Get the slack connection ID."""
        return "slack_api_default"

    def _get_color(self):
        """Get the slack message color."""
        return "#FF0000" if self.status == "failed" else "#36a64f"

    def _get_channel(self):
        """Get the slack channel name."""
        return (
            "#data-team-monitoring-staging"
            if Environment().is_staging()
            else "#data-team-monitoring"
        )

    def _get_icon_url(self):
        """Get the icon URL."""
        return "https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_64x64_emoji_transparent.png?api=v2"

    def _get_message(self):
        """Get the message."""
        return f"{self.message}\n<{self.log_url}|Task log>"

    def _get_attachments(self):
        """Get the attachments."""
        return [
            {
                "color": self._get_color(),
                "blocks": self._get_blocks(),
            }
        ]

    def _get_status_block(self):
        """Get the status block."""
        return {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:*\n`{self.dag_name}`"},
                {"type": "mrkdwn", "text": f"*Status:*\n`{self.status}`"},
            ],
        }

    def _get_source_block(self):
        """Get the source section block."""
        fields = [
            {
                "type": "mrkdwn",
                "text": f"*Source name:*\n`{self.source_name}`",
            },
        ]
        if self.table_name is not None:
            fields.append(
                {
                    "type": "mrkdwn",
                    "text": f"*Table name:*\n`{self.table_name}`",
                }
            )
        return {
            "type": "section",
            "fields": fields,
        }

    def _get_execution_time_block(self):
        """Get the execution time block."""
        return {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Executed at {self.execution_time}",
                }
            ],
        }

    def _get_blocks(self):
        """Get the slack message blocks."""
        return [
            self._get_status_block(),
            self._get_source_block(),
            self._get_execution_time_block(),
        ]
