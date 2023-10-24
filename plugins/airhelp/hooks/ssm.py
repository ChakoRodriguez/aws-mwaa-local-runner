import boto3
from airflow.hooks.base import BaseHook
from airhelp.utils.environment import Environment


class SSMHook(BaseHook):
    """Hook to get credentials from AWS SSM Parameter Store"""

    def __init__(
        self,
        key: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.key = key
        self.region = Environment().region
        self.environment = f"dt-{Environment().environment}"

    def get_conn(self):
        parameter = self._get_parameter_name(self.environment, self.key)
        print(f"Getting {parameter} parameter from AWS SSM Parameter Store")
        credentials = self._get_credentials(parameter)
        print(f"Credentials obtained for parameter {parameter}")
        return credentials

    def _get_credentials(self, parameter: str) -> str:
        """Get the credential from AWS SSM Parameter Store"""
        session = boto3.Session(region_name=self.region)
        ssm = session.client("ssm")
        credential = ssm.get_parameter(Name=parameter, WithDecryption=True)[
            "Parameter"
        ]["Value"]
        del session
        return credential

    def _get_parameter_name(self, environment: str, key: str) -> str:
        """Get the parameter name from the key and environment"""
        return f"/{environment}/dt-tools/{key}"
