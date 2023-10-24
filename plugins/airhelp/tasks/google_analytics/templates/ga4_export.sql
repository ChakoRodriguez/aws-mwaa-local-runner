use database {database};
use schema google_analytics_airflow;
delete from google_analytics_airflow.events
where "event_date" = '{date}';
copy into google_analytics_airflow.events
from '@google_analytics_airflow.{stage}/{parquet_filename}'
file_format = (format_name = 'google_analytics_airflow.snappy_parquet')
match_by_column_name = case_sensitive
force = true;
update google_analytics_airflow.events
set snapshot_loaded_at = current_timestamp(), snapshot_exported_at = to_date('{date}', 'YYYYMMDD')
where "event_date" = '{date}';
