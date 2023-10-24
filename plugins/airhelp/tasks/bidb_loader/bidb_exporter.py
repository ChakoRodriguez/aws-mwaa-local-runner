import datetime
import os
import subprocess

import pandas as pd
from sqlalchemy import create_engine


class BIDBExporter:
    """BIDBExporter class."""

    @classmethod
    def connection_string(cls, source_db) -> str:
        """Returns connection_string for source_db."""

        if source_db not in ["bidb", "dwh2"]:
            raise ValueError(f"Unknown source_db: {source_db}")

        infix = {
            "bidb": "OLD",
            "dwh2": "NEW",
        }

        username = os.getenv(f"BIDB_{infix[source_db]}_LOGIN")
        password = os.getenv(f"BIDB_{infix[source_db]}_PASSWORD")
        host = os.getenv(f"BIDB_{infix[source_db]}_HOST")

        return f"postgresql+psycopg2://{username}:{password}@{host}/bi"

    @classmethod
    def sync_table_to_s3(
        cls, bucket_name, from_table, from_schema, source_db, chunk_size=100000
    ):
        """Syncs table from source_db to S3 bucket."""
        connection = cls.connection_string(source_db)
        source_engine = create_engine(connection)

        now = str(datetime.datetime.now().date())
        location = f"{source_db}/{from_schema}/{from_table}/dt={now}/"
        command = (
            f"aws s3api put-object --bucket {bucket_name} --key {location}"
        )
        subprocess.run(
            command, shell=True, universal_newlines=True, check=True
        )

        for part, chunk in enumerate(
            pd.read_sql_table(
                from_table,
                source_engine,
                schema=from_schema,
                chunksize=chunk_size,
            )
        ):
            file_name = f"s3://{bucket_name}/{source_db}/{from_schema}/{from_table}/dt={now}/{from_table}-part-{part:05d}.parquet.gzip"
            chunk = chunk.applymap(str)
            chunk.to_parquet(file_name, index=False, compression="gzip")
