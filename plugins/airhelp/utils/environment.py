import boto3
from botocore.exceptions import ClientError

AWS_REGION = "eu-west-1"
MWAA_ENVIRONMENT = "MWAA_ENVIRONMENT"


class ParameterNotFound(Exception):
    pass


class Environment(object):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Environment, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.region = AWS_REGION
        self.environment = self._get_environment()

    def _get_key(self, environment: str, name: str) -> str:
        return f"/dt-{environment}/dt-tools/{name}"

    def _get_environment(self) -> str:
        session = boto3.Session(region_name=AWS_REGION)
        ssm = session.client("ssm")
        key = self._get_key("staging", MWAA_ENVIRONMENT)
        try:
            mwaa_environment = ssm.get_parameter(
                Name=key,
                WithDecryption=True,
            )["Parameter"]["Value"]
        except ClientError as exception:
            if exception.response["Error"]["Code"] == "ParameterNotFound":
                raise ParameterNotFound(f"Parameter {key} does not exist")
            mwaa_environment = ssm.get_parameter(
                Name=self._get_key("production", MWAA_ENVIRONMENT),
                WithDecryption=True,
            )["Parameter"]["Value"]
        return mwaa_environment

    def is_staging(self) -> bool:
        return self.environment == "staging"

    def is_production(self) -> bool:
        return self.environment == "production"
