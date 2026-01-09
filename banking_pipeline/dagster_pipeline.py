import subprocess
from dagster import job, op, OpExecutionContext, Nothing, In

# Paths inside the container
DBT_PROJECT_DIR = "/opt/dagster/app/dbt"
DBT_PROFILES_DIR = "/opt/dagster/app/dbt"
LOCAL_TARGET_FOLDER = "/opt/dagster/app/dbt/target"

# Hardcoded Databricks credentials
DATABRICKS_HOST = "dbc-40d5c66b-e30f.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/07c31e78771ea136"
DATABRICKS_TOKEN = "dapi407dbf225a07f89de27d43a9e0cf63cd"


def run_cmd(cmd: str):
    result = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
    )
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}")


# ------------------------
# Dagster Ops
# ------------------------

@op
def dbt_seed(context: OpExecutionContext):
    """Run dbt seeds"""
    run_cmd(
        f"dbt seed "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR} "
        f"--target dev"
    )
    context.log.info("dbt seed completed ✅")


@op(ins={"start": In(Nothing)})
def dbt_run(context: OpExecutionContext):
    """Run dbt models"""
    run_cmd(
        f"dbt run "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR} "
        f"--target dev"
    )
    context.log.info("dbt run completed ✅")


@op(ins={"start": In(Nothing)})
def dbt_test(context: OpExecutionContext):
    """Run dbt tests"""
    run_cmd(
        f"dbt test "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR} "
        f"--target dev"
    )
    context.log.info("dbt tests completed ✅")


@op(ins={"start": In(Nothing)})
def export_locally_via_sql_connector(context: OpExecutionContext):
    """
    Export a Databricks table directly to a local CSV file
    inside your dbt target folder.
    """
    import pandas as pd
    from databricks import sql
    import os

    if not all([DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN]):
        raise ValueError(
            "Databricks connection info missing."
        )

    # Connect and query the mart table
    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    ) as conn:
        df = pd.read_sql(
            "SELECT * FROM workspace.marts.account_interest_summary",
            conn
        )

    # Ensure target folder exists
    os.makedirs(LOCAL_TARGET_FOLDER, exist_ok=True)

    # Write to local CSV
    output_file = os.path.join(LOCAL_TARGET_FOLDER, "account_interest_summary.csv")
    df.to_csv(output_file, index=False)

    context.log.info(f"Exported mart to local CSV: {output_file} ✅")


# ------------------------
# Dagster Job
# ------------------------

@job
def banking_pipeline():
    """
    End-to-end pipeline:
    dbt seed -> dbt run -> dbt test -> export CSV locally
    """
    seed = dbt_seed()
    run = dbt_run(start=seed)
    test = dbt_test(start=run)
    export_locally_via_sql_connector(start=test)
