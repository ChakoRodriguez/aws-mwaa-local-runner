from dataclasses import dataclass

import yaml


@dataclass
class GoogleAnalyticsConfig:
    """Class for handling Google Analytics configuration."""

    project_id: str
    dataset_id: str
    table_prefix: str
    template: str
    gcs_uri: str
    gcp_conn_id: str
    service_account_ssm_key: str

    @classmethod
    def from_yaml(cls, version: str):
        """Create Google Analytics configuration from YAML file."""
        with open(
            "plugins/airhelp/tasks/google_analytics/config.yaml",
            "r",
            encoding="UTF-8",
        ) as file:
            config = yaml.safe_load(stream=file)[version]

        return cls(
            project_id=config["project_id"],
            dataset_id=config["dataset_id"],
            table_prefix=config["table_prefix"],
            template=config["template"],
            gcs_uri=config["gcs_uri"],
            gcp_conn_id=config["gcp_conn_id"],
            service_account_ssm_key=config["service_account_ssm_key"],
        )
