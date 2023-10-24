use database {database};
create schema if not exists google_analytics_airflow;
use schema google_analytics_airflow;
create table if not exists google_analytics_airflow.ga_sessions
    using template (
        select array_agg(object_construct(*))
        from table(
            infer_schema(
                location=>'@google_analytics_airflow.{stage}/{parquet_filename}',
                file_format=>'google_analytics_airflow.snappy_parquet'
            )
        )
    );
alter table ga_sessions
add column snapshot_exported_at timestamp_ntz, snapshot_loaded_at timestamp_ntz;
