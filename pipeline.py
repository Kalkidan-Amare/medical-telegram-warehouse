import os
import subprocess
from pathlib import Path

from dagster import OpExecutionContext, ScheduleDefinition, job, op


ROOT = Path(__file__).parent


def run_command(context: OpExecutionContext, command: list[str]) -> None:
    context.log.info("Running command: %s", " ".join(command))
    result = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        context.log.info(result.stdout)
    if result.stderr:
        context.log.warning(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(command)}")


@op
def scrape_telegram_data(context: OpExecutionContext) -> None:
    run_command(context, ["python", "src/scraper.py"])


@op
def load_raw_to_postgres(context: OpExecutionContext) -> None:
    run_command(context, ["python", "src/load_raw_to_postgres.py"])


@op
def run_dbt_transformations(context: OpExecutionContext) -> None:
    run_command(
        context,
        [
            "dbt",
            "run",
            "--project-dir",
            "medical_warehouse",
            "--profiles-dir",
            "medical_warehouse",
        ],
    )
    run_command(
        context,
        [
            "dbt",
            "test",
            "--project-dir",
            "medical_warehouse",
            "--profiles-dir",
            "medical_warehouse",
        ],
    )


@op
def run_yolo_enrichment(context: OpExecutionContext) -> None:
    run_command(context, ["python", "src/yolo_detect.py"])
    run_command(context, ["python", "src/load_yolo_to_postgres.py"])


@job
def telegram_warehouse_job() -> None:
    scrape_telegram_data()
    load_raw_to_postgres()
    run_dbt_transformations()
    run_yolo_enrichment()


daily_schedule = ScheduleDefinition(
    job=telegram_warehouse_job,
    cron_schedule="0 2 * * *",
    execution_timezone=os.getenv("DAGSTER_TZ", "UTC"),
)


__all__ = ["telegram_warehouse_job", "daily_schedule"]
