import os
import psycopg2
import boto3
from typing import Dict
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pendoguidesproject.aws_parameter_store import AwsParameterStore
import boto3

class redshiftExporter:
    def __init__(self, project, table: str, historical_load=False):
        self.environment = os.environ.get("environment")
        self.region = "us-east-1"
        self.db_user = self.get_db_user(project)
        self.database = "master"
        self.schema = project
        self.table = table
        self.glue_database = self.get_glue_database(project)
        self.incremental_load = True
        self.historical_load = historical_load

    def export(self):
        conn = self.get_connection(self.db_user, self.region, self.database, self.environment)
        cursor = conn.cursor()
        self.create_external_schema_if_not_exists(cursor, self.schema, self.glue_database)
        self.update_external_table(cursor, self.table, self.schema, self.incremental_load, self.historical_load)
        conn.commit()

    def import_data(self):
        conn = self.get_connection(self.db_user, self.region, self.database, self.environment)
        return conn

    def update_external_table(self, cursor, table, schema, incremental_load, historical_load):
        if historical_load:
            table_name = table + "hist"
        else:
            table_name = table
        try:
            print(f"Trying to create table {table} in schema {schema}...")
            if incremental_load:
                cursor.execute(sql.SQL(
                    "CREATE TABLE {2}.{3} AS SELECT * FROM {1}.{0} limit 5")
                    .format(
                    *map(sql.Identifier, (table, schema + 'ext', schema, table_name))
                ))
            else:
                cursor.execute(sql.SQL(
                    "CREATE TABLE {2}.{3} AS SELECT * FROM {1}.{0} WHERE date_partition=(select max(date_partition) from {1}.{0}) limit 5")
                    .format(
                    *map(sql.Identifier, (table, schema + 'ext', schema, table_name))
                ))
        except Exception as e:
            print(e)
            print(f"Table {table} in schema {schema} already exists...")

        external_schema = schema + "ext"
        external_columns_types = self.get_external_columns_types(cursor, table_name, external_schema)
        existing_columns_types = self.get_columms_types(cursor, table_name, schema)
        self.align_schema(cursor, schema, table_name, existing_columns_types, external_columns_types)

        print("Truncating table")
        cursor.execute(sql.SQL("TRUNCATE {}.{}").format(
            sql.Identifier(schema), sql.Identifier(table_name))
        )
        print("Updating table")
        fields = '"'+'", "'.join(list(external_columns_types.keys()))+'"'
        if incremental_load:
            cursor.execute(sql.SQL(
                f"INSERT INTO {schema}.{table_name}({fields}) SELECT {fields} FROM {external_schema}.{table}"
            ))
        else:
            cursor.execute(sql.SQL(
                f"INSERT INTO {schema}.{table_name}({fields}) SELECT {fields} FROM {external_schema}.{table} WHERE date_partition=(select max(date_partition) from {external_schema}.{table})"
            ))

    def create_external_schema_if_not_exists(self, cursor, schema, glue_database):
        cursor.execute(sql.SQL(
            f"CREATE EXTERNAL SCHEMA IF NOT EXISTS {schema + 'ext'} FROM DATA CATALOG DATABASE '{glue_database}' iam_role "
            f"'arn:aws:iam::433320668742:role/cdo-redshift-export-{self.environment}-role' CREATE EXTERNAL DATABASE IF NOT "
            "EXISTS;"))

    def redshift_connection_params(self, db_user, region, database, environment) -> Dict[str, str]:
        def get_cluster_creds(db_user, cluster_id, region, database):
            redshiftClient = boto3.Session(region_name=region).client("redshift") # when not local
            #redshiftClient = boto3.client('redshift') # new to do local testing
            return redshiftClient.get_cluster_credentials(DbUser=db_user, ClusterIdentifier=cluster_id, DbName=database)

        store = AwsParameterStore(region_name=region)
        endpoint = store.get_param(f"/redshift/{environment}/endpoint")
        credentials = get_cluster_creds(db_user, endpoint.split(".")[0], region, database)
        return {
            "endpoint": endpoint,
            "username": credentials["DbUser"],
            "password": credentials["DbPassword"],
        }

    def get_db_user(self, project):
        return f"{project}_rs_user"

    def get_glue_database(self, project):
        if self.environment == "prd":
            glue_database = project
        else:
            glue_database = project + "_dev"
        return glue_database

    def get_connection(self, db_user, region, database, environment):
        connection_properties = self.redshift_connection_params(db_user, region, database, environment)
        host, port = str.split(connection_properties["endpoint"], ":")
        username = connection_properties["username"]
        password = connection_properties["password"]
        database = self.database
        conn = psycopg2.connect(
            "dbname={} host={} user={} password={} port={}".format(database, host, username, password, port))
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  # <-- ADD THIS LINE
        return conn

    def is_loaded_incrementally(self, table: str) -> bool:
        return table.lower() in ["issue"]

    def add_column(self, cursor, table, schema, name, type):
        cursor.execute(sql.SQL(
            f"""    
                alter table {schema}.{table}
                add column "{name}" {type};
                """)
        )

    def remove_column(self, cursor, table, schema, name):
        cursor.execute(sql.SQL(
            f"""alter table {schema}.{table}
                drop column "{name}";
                """)
        )

    def get_external_columns_types(self, cursor, external_table, external_schema) -> dict:
        cursor.execute(sql.SQL(
            f"""    
        select
        columnname,
        external_type
        from svv_external_columns
        where
        tablename = '{external_table}' and 
        schemaname = '{external_schema}';
        """)
        )
        result = cursor.fetchall()
        return dict(result)

    def get_columms_types(self, cursor, table, schema) -> dict:
        cursor.execute(sql.SQL(
            f"""    
        select 
       column_name,
       data_type
        from information_schema.columns
        where
        table_name = '{table}' and 
        table_schema = '{schema}';
        """)
        )
        result = cursor.fetchall()
        return dict(result)

    def parse_column_list(self, column_details, keys):
        for i in range(0, len(column_details)):
            column_details[i] = dict(zip(keys, column_details[i]))

        return column_details

    def align_schema(self, cursor, schema, table, existing_columns_types, external_columns_types):
        existing_columns_list = list(existing_columns_types.keys())
        external_columns_list = list(external_columns_types.keys())

        cols_to_add = list(set(external_columns_list) - set(existing_columns_list))
        cols_to_remove = list(set(existing_columns_list) - set(external_columns_list))
        cols_to_type_check = list(set(existing_columns_list) & set(external_columns_list))

        print("cols_to_add")
        print(cols_to_add)
        print("cols to remove")
        print(cols_to_remove)
        print("cols to typecheck")
        print(cols_to_type_check)

        for col in cols_to_add:
            type = self.get_rs_type(external_columns_types[col])
            self.add_column(cursor, table, schema, col, type)

        for col in cols_to_remove:
            self.remove_column(cursor, table, schema, col)

        for col in cols_to_type_check:
            external_type = self.get_rs_type(external_columns_types[col])
            existing_type = existing_columns_types[col]

            if external_type != existing_type:
                print(external_type)
                print(existing_type)
                print(f"Changing type of col {col}")
                self.remove_column(cursor, table, schema, col)
                self.add_column(cursor, table, schema, col, external_type)

    def get_rs_type(self, type):
        type_dict = {
            "string": "character varying",
            "double": "double precision"
        }

        return type_dict.get(type, type)