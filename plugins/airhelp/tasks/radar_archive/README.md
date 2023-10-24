# Load radar archive CSV files in S3 to Snowflake

This is a one-shot task that loads radar archive CSV files from S3 to Snowflake. The SQL script is located in `templates/radar_archive.sql`. It is intended to be converted to a DAG thta runs concurrently after the results are validated.
