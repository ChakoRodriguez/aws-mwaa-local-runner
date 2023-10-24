class DataTypes:
    @classmethod
    def transform(cls, field_type) -> str:
        data_types = {
            "int16": "number",
            "int32": "number",
            "int64": "number",
            "bool": "boolean",
            "float32": "float",
            "float64": "float",
            "double": "float",
            "decimal128": "number",
            "decimal(10, 2)": "number(10, 2)",
            "string": "varchar",
            "date32": "date",
            "timestamp": "timestamp",
            "timestamp[us]": "timestamp",
            "binary": "variant",
        }
        return data_types[field_type]
