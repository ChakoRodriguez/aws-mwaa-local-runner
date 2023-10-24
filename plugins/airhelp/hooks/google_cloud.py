from tempfile import NamedTemporaryFile

from airflow.models.connection import Connection
from airhelp.hooks.connection import ConnectionHook
from airhelp.hooks.ssm import SSMHook


class GCPConnectionHook(ConnectionHook):
    """Hook to create a Google Cloud connection"""

    def __init__(
        self,
        conn_id: str,
        project_id: str,
        service_account_ssm_key: str,
        **kwargs,
    ) -> None:
        super().__init__(conn_id, **kwargs)
        self.project_id = project_id
        self.service_account_ssm_key = service_account_ssm_key

    def _create_connection(self) -> Connection:
        return Connection(
            conn_id=self.conn_id, conn_type="google_cloud_platform"
        )

    def _get_extra_field(self) -> dict:
        return {
            "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform",
            "extra__google_cloud_platform__project": self.project_id,
            "extra__google_cloud_platform__keyfile_dict": self._get_credentials(),
        }

    def _get_key_path(self, content: str) -> str:
        """Create a temporary file from a string"""
        temp_file = NamedTemporaryFile(delete=False, suffix=".json")
        temp_file.write(content.encode())
        temp_file.close()
        return temp_file.name

    def _get_credentials(self) -> str:
        """Get the Google Cloud credentials"""
        return SSMHook(self.service_account_ssm_key).get_conn()
