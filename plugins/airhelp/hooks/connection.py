import json
from abc import ABC, abstractmethod

from airflow import settings
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection


class ConnectionHook(BaseHook, ABC):
    """Hook to create an Airflow connection"""

    def __init__(
        self,
        conn_id: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id

    @abstractmethod
    def _create_connection(self) -> Connection:
        pass

    @abstractmethod
    def _get_extra_field(self) -> dict:
        pass

    def _get_extra_fieds_str(self) -> str:
        return json.dumps(self._get_extra_field())

    def get_conn(self, force: bool = False) -> str:
        """Get the connection or create it if it doesn't exist"""
        if settings.Session is None:
            raise Exception("Session is not available")

        session = settings.Session

        try:
            conn = (
                session.query(Connection)
                .filter(Connection.conn_id == self.conn_id)
                .one_or_none()
            )

            if conn is not None and force:
                conn.set_extra(self._get_extra_fieds_str())
                session.commit()

            if conn is None:
                conn = self._create_connection()
                conn.set_extra(self._get_extra_fieds_str())
                session.add(conn)
                session.commit()

        except BaseException as e:
            print("Airflow DB Session error:" + str(e))
            session.rollback()
            raise
        finally:
            session.close()

        return self.conn_id
