create schema if not exists public;
use schema public;

create file format if not exists custom_parquet_format
    type = parquet
    compression = auto;

create stage if not exists datalake_dt_{environment}_snapshots
    storage_integration = s3_dt_{environment}
    url = 's3://ah-dt{environment[0]}-datalake-dt-{environment}/snapshots/'
    file_format = custom_parquet_format;
