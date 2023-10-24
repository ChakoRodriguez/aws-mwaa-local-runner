from dataclasses import dataclass
from typing import List

import yaml
from airhelp.utils.environment import Environment


@dataclass
class SnapshotsMetadataConfig:
    """Class for handling metadata configuration."""

    bucket_name: str
    prefix: str
    loader: str


@dataclass
class SnapshotsConfig:
    """Class for handling snapshots configuration."""

    environment: str
    bucket_name: str
    prefix: str
    template: str
    metadata: SnapshotsMetadataConfig
    pii: dict
    json_subfields_white_list: dict
    snowflake_stage_name: str
    assumed_role: str
    snowflake_warehouse_name: str

    @classmethod
    def from_yaml(cls, yaml_path: str):
        """Create SnapshotsConfig from YAML file."""
        environment = Environment().environment
        with open(yaml_path, "r", encoding="UTF-8") as file:
            config = yaml.safe_load(stream=file)
        pii_metadata = cls.from_yaml_pii(
            "plugins/airhelp/tasks/snapshot_loader/pii.yaml"
        )
        return cls(
            environment=environment,
            bucket_name=config["snapshots"]["bucket_name"][environment],
            template=config["snapshots"]["template"],
            prefix=config["snapshots"]["prefix"],
            metadata=SnapshotsMetadataConfig(
                bucket_name=f"mwaa-dt-{environment}",
                prefix=config["snapshots"]["metadata"]["prefix"],
                loader=config["snapshots"]["metadata"]["loader"],
            ),
            pii=pii_metadata["pii"],
            json_subfields_white_list=pii_metadata[
                "json_subfields_white_list"
            ],
            snowflake_stage_name=config["snapshots"]["snowflake_stage_name"][
                environment
            ],
            assumed_role=config["snapshots"]["assumed_role"][environment],
            snowflake_warehouse_name=config["snapshots"][
                "snowflake_warehouse_name"
            ][environment],
        )

    @classmethod
    def from_yaml_pii(cls, yaml_path: str):
        """Get PII fields from YAML file."""
        with open(yaml_path, "r", encoding="UTF-8") as file:
            config = yaml.safe_load(stream=file)
        return {
            "pii": config["pii"],
            "json_subfields_white_list": config["json_subfields_white_list"],
        }

    def get_table_pii(self, schema: str, table: str) -> List[str]:
        """Get PII columns for a given table."""
        return self.pii.get(schema, {}).get(table.lower(), [])

    def get_table_json_subfields_white_list(
        self, schema: str, table: str
    ) -> List[str]:
        """Get JSON PII white list for a given table."""
        return self.json_subfields_white_list.get(schema, {}).get(
            table.lower(), []
        )
