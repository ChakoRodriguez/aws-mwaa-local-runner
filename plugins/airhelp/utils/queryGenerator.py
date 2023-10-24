class QueryGenerator:
    def create_table_query(
        self, db, target_schema, table_name, formated_columns
    ):
        with open(
            "plugins/airhelp/tasks/bidb_loader/templates/table.sql", "r"
        ) as file:
            create_table_query = f"{file.read()}".format(
                db=db,
                target_schema=target_schema,
                table_name=table_name,
                formated_columns=formated_columns,
            )
        return create_table_query

    def data_loading_query(
        self,
        db,
        source_db,
        source_schema,
        target_schema,
        table_name,
        column_names,
        export_date,
        stage_name,
        environment,
    ):
        with open(
            "plugins/airhelp/tasks/bidb_loader/templates/data_loading.sql", "r"
        ) as file:
            data_loading_query = f"{file.read()}".format(
                db=db,
                source_db=source_db,
                source_schema=source_schema,
                target_schema=target_schema,
                table_name=table_name.lower(),
                column_names=column_names,
                export_date=export_date,
                stage_name=stage_name,
                environment=environment,
            )
        return data_loading_query
