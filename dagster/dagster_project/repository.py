from dagster import job, repository, ScheduleDefinition
from ops.run_ingestion import execute_ingestion_script
from ops.run_dbt import run_dbt_models
from ops.export_reports_sftp import export_reports_sftp
from dagster_dbt import DbtCliResource

dbt_resource = DbtCliResource(
    project_dir="/opt/dagster/app/dbt",
    profiles_dir="/opt/dagster/app/dbt",
)

@job(resource_defs={"dbt": dbt_resource})
def pipeline_ingest_then_dbt():
    ingestion = execute_ingestion_script()
    dbt_run = run_dbt_models(ingestion)
    export_reports_sftp(dbt_run)

daily_pipeline_schedule = ScheduleDefinition(
    job=pipeline_ingest_then_dbt,
    cron_schedule="0 7 * * *",
    execution_timezone="UTC",
)

@repository
def loadsmart_dimensional_modeling_pipeline():
    return [
        pipeline_ingest_then_dbt,
        daily_pipeline_schedule,
    ]
