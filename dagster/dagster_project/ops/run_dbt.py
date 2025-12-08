from dagster import op, get_dagster_logger
from dagster_dbt import DbtCliResource


@op(required_resource_keys={"dbt"})
def run_dbt_models(context, upstream):
    logger = get_dagster_logger()

    logger.info(f"Starting dbt run. Upstream message: {upstream}")

    invocation = context.resources.dbt.cli(["run"], context=context)

    # Stream raw logs produced by dbt
    for raw_event in invocation.stream_raw_events():
        logger.info(raw_event)

    # Wait for dbt to finish
    result = invocation.wait()

    logger.info("dbt run executed successfully.")
    return "DBT_DONE"
