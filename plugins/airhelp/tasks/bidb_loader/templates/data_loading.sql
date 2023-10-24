copy into {db}.{target_schema}_AIRFLOW.{table_name}
    from (
        select
          {column_names}
        from
            '@{db}.PUBLIC."{stage_name}"/{source_db}/{source_schema}/{table_name}/dt={export_date}/'
    )
        file_format = (format_name = {db}.PUBLIC.PARQUET_DT_{environment})
        on_error = 'skip_file';