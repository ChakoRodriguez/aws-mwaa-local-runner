from functools import partial

import airhelp.tasks.bidb_loader.bidb_loader as bi
import pendulum
from airflow import DAG
from airhelp.operators.slack import alert_slack_channel

with DAG(
    "bidb_loader",
    start_date=pendulum.yesterday(),
    # Jenkins bidb exporter process finishes some minutes before 07:00 UTC, so we
    # schedule this DAG to start the loading process just after that.
    # We also schedule it to run again at 09:00 UTC, in case the exporter process is late.
    schedule="0 7,9 * * *",  # 07:00 and 09:00 UTC
    catchup=False,
    on_failure_callback=partial(alert_slack_channel, "bidb", None, "failed"),
) as dag:
    environment = bi.get_environment()
    files = bi.get_files_to_load(
        environment=environment, config=bi.read_configuration_file()
    )
    schema_types = bi.transform_data_types(files)

    create_table_queries = bi.create_tables_query_generator(schema_types)
    loading_queries = bi.data_loading_query_generator(schema_types)

    created_tables = bi.get_job_status(
        bi.execute_queries_async(create_table_queries), sensor=True
    )
    bi.get_job_status(
        bi.execute_queries_async(loading_queries, sensor=created_tables)
    )
