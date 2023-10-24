create schema if not exists {schema}_airflow;
create or replace table {schema}_airflow.{table_name} ({columns});
copy into {schema}_airflow.{table_name}
from (
    select
        {copy_columns}
    from
        '@public.{stage_name}/{snapshot_path}/'
)
    file_format = (format_name = 'custom_parquet_format')
    pattern = '.*part.*'
    on_error = 'skip_file';
