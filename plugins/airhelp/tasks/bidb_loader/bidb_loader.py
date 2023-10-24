import time
from datetime import datetime

import pyarrow.parquet as pq
import s3fs
import yaml
from airflow.decorators import task
from airflow.models import Variable
from airhelp.utils.data_types import DataTypes
from airhelp.utils.environment import Environment
from airhelp.utils.queryGenerator import QueryGenerator
from airhelp.utils.snowflakeAsyncExecutor import SnowflakeExecutor
from snowflake.connector import ProgrammingError


@task
def get_environment() -> str:
    env = Environment().environment
    Variable.set("environment", env)
    return env


@task
def read_configuration_file() -> dict:
    try:
        with open(
            "plugins/airhelp/tasks/bidb_loader/config.yaml",
            "r",
            encoding="UTF-8",
        ) as file:
            config = yaml.safe_load(stream=file)
            return config["bidb"]
    except Exception as e:
        print(e)


@task
def get_files_to_load(environment: str, config: dict) -> list:
    export_date = str(datetime.now().date())
    files = []
    for schema, tables in config.items():
        for table in tables:
            files.append(
                f"s3://ah-dt{environment[0]}-datalake-dt-{environment}/bidb/{schema}/{table}/dt={export_date}/{table}.parquet.gzip"
            )
            files.append(
                f"s3://ah-dt{environment[0]}-datalake-dt-{environment}/bidb/{schema}/{table}/dt={export_date}/{table}-00000.parquet.gzip"
            )
    return files


@task
def transform_data_types(paths: list) -> list:
    files = []
    for path in paths:
        try:
            s3_fs = s3fs.S3FileSystem()
            dataset = pq.ParquetDataset(
                path, filesystem=s3_fs, use_legacy_dataset=False
            )
            files.append(
                [
                    path,
                    {
                        field.name: DataTypes.transform(field.type)
                        for field in dataset.schema
                    },
                ]
            )
        except Exception as e:
            print(e)
            print("Error reading in path: ", path)
    return files


@task
def create_tables_query_generator(files: list) -> list:
    environment = Variable.get("environment")
    if environment != "production":
        db = "AH_RAW_DEV"
    else:
        db = "AH_RAW"
    create_table_queries = []
    for table in files:
        path = table[0]
        columns = table[1]
        parts = path.split("/")
        target_schema = f"{parts[3]}".upper()
        table_name = parts[5].upper()
        formated_columns = ""

        for field, data_type in columns.items():
            formated_columns += f"{field} {data_type},\n"
        formated_columns = formated_columns[:-2]

        qg = QueryGenerator()
        query = qg.create_table_query(
            db=db,
            target_schema=target_schema,
            table_name=table_name,
            formated_columns=formated_columns,
        )
        create_table_queries.append(query)
    return create_table_queries


@task
def data_loading_query_generator(files: list) -> list:
    environment = Variable.get("environment")
    export_date = str(datetime.now().date())
    if environment != "production":
        db = "AH_RAW_DEV"
        stage_name = "AH_RAW_DEV_DT_STAGING"
    else:
        db = "AH_RAW"
        stage_name = "AH_RAW_DT_PRODUCTION"  # TODO: storage integration and stage pending to be created
    loading_queries = []
    for table in files:
        path = table[0]
        parts = path.split("/")
        source_db = parts[3]
        source_schema = parts[4]
        target_schema = f"{parts[3]}".upper()
        table_name = parts[5].upper()
        columns = table[1]
        column_names = ""

        for field, data_type in columns.items():
            column_names += f"$1:{field},\n"
        column_names = column_names[:-2]

        qg = QueryGenerator()
        query = qg.data_loading_query(
            db=db,
            source_db=source_db,
            source_schema=source_schema,
            target_schema=target_schema,
            table_name=table_name,
            column_names=column_names,
            export_date=export_date,
            stage_name=stage_name,
            environment=environment,
        )
        loading_queries.append(query)
    return loading_queries


@task
def execute_queries_async(queries: list, sensor=False):
    conn = SnowflakeExecutor()
    jobs = []
    for query in queries:
        id = conn.execute_query_async(query)
        jobs.append([query, id.sfqid])
    conn.close()
    return jobs


@task
def get_job_status(jobs: list, sensor=False):
    conn = SnowflakeExecutor().sf_connection
    while len(jobs) > 0:
        for job in jobs:
            try:
                if conn.is_still_running(
                    conn.get_query_status_throw_if_error(job[1])
                ):
                    print(job[0][0:30], job[1], "Query running...")
                    time.sleep(10)
                else:
                    print(job[0][0:30], job[1], "Done")
                    jobs.remove([job[0], job[1]])
            except ProgrammingError as err:
                print("Programming Error: {0}".format(err))
                jobs.remove([job[0], job[1]])
    conn.close()
    if sensor:
        return True
