use schema {database}.dynamodb_airflow;

create or replace table {database}.dynamodb_airflow.tmp_aircom_stats (
    event variant
);

copy into {database}.dynamodb_airflow.tmp_aircom_stats
from (
    select parse_json($1):Item
    from
        '@public.{stage}/{location}data'
)
    file_format = (format_name = '{database}.public.{file_format}')
    pattern = '.*json.gz'
    on_error = 'skip_file';

alter table {database}.dynamodb_airflow.tmp_aircom_stats swap with {database}.dynamodb_airflow.aircom_stats;

drop table {database}.dynamodb_airflow.tmp_aircom_stats;
