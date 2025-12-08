from dagster import op
import importlib.util
from pathlib import Path
import sys

SCRIPT_PATH = "/opt/dagster/app/scripts/1_source_to_postgresql_with_ingestion_date.py"


@op
def execute_ingestion_script(context):
    context.log.info(f"Running ingestion script: {SCRIPT_PATH}")

    script_dir = str(Path(SCRIPT_PATH).parent)
    sys.path.append(script_dir)

    spec = importlib.util.spec_from_file_location("ingestion_script", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, "run_all"):
        raise AttributeError(
            f"The script {SCRIPT_PATH} does not define a function named run_all()."
        )

    module.run_all()

    context.log.info("Ingestion completed successfully.")
    return "INGESTION_DONE"
