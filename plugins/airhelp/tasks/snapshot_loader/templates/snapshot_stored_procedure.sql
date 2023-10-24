create or replace procedure public.snapshot_loader(
    schema_name varchar,
    table_name varchar,
    column_names varchar,
    copy_columns varchar,
    stage_name varchar,
    snapshot_path varchar,
    snapshot_exported_at varchar
)
    returns table(
        file text,
        status text,
        rows_parsed number,
        rows_loaded number,
        error_limit number,
        errors_seen number,
        first_error text,
        first_error_line number,
        first_error_character number,
        first_error_column_name text
    )
    language sql
    as
    declare
        query_create_schema varchar;
        query_create_table varchar;
        query_copy_into varchar;
        result resultset;
    begin
        query_create_schema := 'create schema if not exists ' || schema_name || '_airflow';
        query_create_table := 'create or replace table ' || schema_name || '_airflow.' || table_name || ' (' || column_names || ', snapshot_exported_at datetime, snapshot_loaded_at datetime)';
        query_copy_into := 'copy into ' || schema_name || '_airflow.' || table_name || ' from (select ' || copy_columns || ', \'' || snapshot_exported_at || '\' as snapshot_exported_at, sysdate() as snapshot_loaded_at' || ' from \'@public.' || stage_name|| '/' || snapshot_path || '/\') file_format = (format_name = \'custom_parquet_format\') pattern = \'.*part.*\' on_error = \'skip_file\'';
        begin transaction;
        execute immediate :query_create_schema;
        execute immediate :query_create_table;
        result := (execute immediate :query_copy_into);
        commit;
        return table(result);
    end;
