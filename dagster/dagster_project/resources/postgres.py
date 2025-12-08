from dagster import resource
import psycopg2

@resource
def postgres_resource(init_context):
    return psycopg2.connect(
        host=init_context.resource_config["localhost"],
        port=init_context.resource_config["5432"],
        dbname=init_context.resource_config["dbt_database"],
        user=init_context.resource_config["dbt_user"],
        password=init_context.resource_config["dbt_password"],
    )
