import csv
import io
import json
import logging
import os
import re
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

TEAMS_WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL", "")

S3_CONN_ID = "aws_s3"
INPUTS_PREFIX = "inputs/"
OUTPUTS_PREFIX = "outputs/"

SCAFFOLDS_RE = re.compile(r"^inputs/(?P<dataset_id>[^/]+)_scaffolds\.csv$")
R_GROUPS_RE = re.compile(r"^inputs/(?P<dataset_id>[^/]+)_r_groups\.csv$")


def _get_s3_hook() -> S3Hook:
    return S3Hook(aws_conn_id=S3_CONN_ID)


def _send_teams_alert(context) -> None:
    if not TEAMS_WEBHOOK_URL:
        logger.warning("TEAMS_WEBHOOK_URL not set — skipping Teams notification.")
        return

    import urllib.request

    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    run_id = context.get("run_id", "unknown")
    exception = context.get("exception", "unknown error")

    payload = json.dumps({
        "text": (
            f"❌ **Airflow task failed**\n\n"
            f"**DAG:** {dag_id}\n"
            f"**Task:** {task_id}\n"
            f"**Run:** {run_id}\n"
            f"**Error:** {exception}"
        )
    }).encode("utf-8")

    req = urllib.request.Request(
        TEAMS_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        logger.info("Teams alert sent for task %s", task_id)
    except Exception as exc:
        logger.warning("Failed to send Teams alert: %s", exc)


def discover_datasets(**context) -> None:
    """
    Determines which dataset(s) this DAG run should process.

    - If the `dataset_id` param is explicitly set, only that dataset is
      considered (manual/backfill/reprocessing mode).
    - Otherwise, scans s3://<bucket>/inputs/ for any <id>_scaffolds.csv +
      <id>_r_groups.csv pairs (the weekly/scheduled mode).

    A dataset counts as "already processed" if outputs/<id>/molecules.csv
    already exists in S3.
    """
    requested_dataset_id: str = (context["params"].get("dataset_id") or "").strip()
    overwrite: bool = bool(context["params"].get("overwrite", False))

    bucket = os.environ["S3_BUCKET"]
    hook = _get_s3_hook()

    if requested_dataset_id:
        candidate_ids = {requested_dataset_id}
    else:
        keys = hook.list_keys(bucket_name=bucket, prefix=INPUTS_PREFIX) or []
        scaffold_ids = {m.group("dataset_id") for k in keys if (m := SCAFFOLDS_RE.match(k))}
        r_group_ids = {m.group("dataset_id") for k in keys if (m := R_GROUPS_RE.match(k))}

        candidate_ids = scaffold_ids & r_group_ids

        for missing_id in sorted(scaffold_ids - r_group_ids):
            logger.warning(
                "Dataset '%s' has a scaffolds.csv but no matching r_groups.csv — skipping.",
                missing_id,
            )
        for missing_id in sorted(r_group_ids - scaffold_ids):
            logger.warning(
                "Dataset '%s' has a r_groups.csv but no matching scaffolds.csv — skipping.",
                missing_id,
            )

    datasets = []
    for dataset_id in sorted(candidate_ids):
        scaffolds_key = f"{INPUTS_PREFIX}{dataset_id}_scaffolds.csv"
        r_groups_key = f"{INPUTS_PREFIX}{dataset_id}_r_groups.csv"
        molecules_output_key = f"{OUTPUTS_PREFIX}{dataset_id}/molecules.csv"

        for key in (scaffolds_key, r_groups_key):
            try:
                exists = hook.check_for_key(key=key, bucket_name=bucket)
            except Exception as e:
                raise PermissionError(
                    f"Error accessing s3://{bucket}/{key} — check S3 credentials "
                    f"on the '{S3_CONN_ID}' connection."
                ) from e
            if not exists:
                raise FileNotFoundError(f"Expected file not found in S3: s3://{bucket}/{key}")

        already_processed = hook.check_for_key(key=molecules_output_key, bucket_name=bucket)
        if already_processed and not overwrite:
            logger.info(
                "Dataset '%s' was already processed (%s exists) — skipping "
                "(set overwrite=True to reprocess).",
                dataset_id,
                molecules_output_key,
            )
            continue

        if already_processed and overwrite:
            logger.info("Dataset '%s' was already processed — reprocessing (overwrite=True).", dataset_id)

        datasets.append({"dataset_id": dataset_id})

    if requested_dataset_id and not datasets:
        raise ValueError(
            f"Dataset '{requested_dataset_id}' was already processed and overwrite=False. "
            f"Set overwrite=True to reprocess it."
        )

    if not datasets:
        logger.info("No new datasets found to process. Nothing to do.")

    logger.info("Datasets to process this run: %s", [d["dataset_id"] for d in datasets])
    context["ti"].xcom_push(key="datasets", value=datasets)


def _run_worker(stage: str, dataset_id: str) -> None:
    result = subprocess.run(
        [
            "docker", "run", "--rm",
            "--network", "pipeline_infra",
            "-e", f"S3_ENDPOINT_URL={os.environ['S3_ENDPOINT_URL']}",
            "-e", f"S3_ACCESS_KEY={os.environ['S3_ACCESS_KEY']}",
            "-e", f"S3_SECRET_KEY={os.environ['S3_SECRET_KEY']}",
            "-e", f"S3_BUCKET={os.environ['S3_BUCKET']}",
            "-e", f"S3_REGION={os.environ.get('S3_REGION', 'us-east-1')}",
            "pipeline_worker",
            "python", "run.py", stage, "--dataset-id", dataset_id,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"pipeline_worker ({stage}) failed for dataset '{dataset_id}':\n{result.stderr}"
        )

    logger.info("pipeline_worker (%s) completed for dataset '%s'", stage, dataset_id)


def run_molecule_generation(**context) -> None:
    datasets: list = context["ti"].xcom_pull(task_ids="discover_datasets", key="datasets") or []
    if not datasets:
        logger.info("No datasets to process — skipping molecule generation.")
        return
    for entry in datasets:
        _run_worker("generate", entry["dataset_id"])


def quality_checks_molecules(**context) -> None:
    """
    Post-generation quality checks on each dataset's molecules.csv:
    - File exists in S3
    - File is non-empty
    - CSV has the expected 'smiles' column
    - All rows have a non-empty smiles value
    """
    datasets: list = context["ti"].xcom_pull(task_ids="discover_datasets", key="datasets") or []
    if not datasets:
        logger.info("No datasets to quality-check — skipping.")
        return

    bucket = os.environ["S3_BUCKET"]
    hook = _get_s3_hook()

    for entry in datasets:
        dataset_id = entry["dataset_id"]
        s3_key = f"{OUTPUTS_PREFIX}{dataset_id}/molecules.csv"

        if not hook.check_for_key(key=s3_key, bucket_name=bucket):
            raise FileNotFoundError(
                f"Quality check failed: molecules.csv not found in S3 for dataset "
                f"'{dataset_id}': s3://{bucket}/{s3_key}"
            )

        raw = hook.read_key(key=s3_key, bucket_name=bucket).strip()
        if not raw:
            raise ValueError(f"Quality check failed: molecules.csv is empty for dataset '{dataset_id}'")

        reader = csv.DictReader(io.StringIO(raw))
        if "smiles" not in (reader.fieldnames or []):
            raise ValueError(
                f"Quality check failed: molecules.csv for dataset '{dataset_id}' is "
                f"missing the 'smiles' column. Found: {reader.fieldnames}"
            )

        rows = list(reader)
        if not rows:
            raise ValueError(
                f"Quality check failed: molecules.csv for dataset '{dataset_id}' has a "
                f"header but no data rows."
            )

        empty_smiles = [i + 2 for i, row in enumerate(rows) if not row.get("smiles", "").strip()]
        if empty_smiles:
            raise ValueError(
                f"Quality check failed: molecules.csv for dataset '{dataset_id}' has "
                f"empty smiles values on rows: {empty_smiles}"
            )

        logger.info(
            "Quality check passed for dataset '%s': %d molecules generated.",
            dataset_id,
            len(rows),
        )


def run_properties_calculation(**context) -> None:
    datasets: list = context["ti"].xcom_pull(task_ids="discover_datasets", key="datasets") or []
    if not datasets:
        logger.info("No datasets to process — skipping properties calculation.")
        return
    for entry in datasets:
        _run_worker("properties", entry["dataset_id"])


def quality_checks_properties(**context) -> None:
    """
    Post-calculation quality checks on each dataset's properties.csv:
    - File exists in S3
    - File is non-empty
    - CSV has the expected property columns
    - At least one row of results
    """
    datasets: list = context["ti"].xcom_pull(task_ids="discover_datasets", key="datasets") or []
    if not datasets:
        logger.info("No datasets to quality-check — skipping.")
        return

    bucket = os.environ["S3_BUCKET"]
    hook = _get_s3_hook()
    required_columns = {"mol_weight", "log_p", "tpsa", "hba", "hbd"}

    for entry in datasets:
        dataset_id = entry["dataset_id"]
        s3_key = f"{OUTPUTS_PREFIX}{dataset_id}/properties.csv"

        if not hook.check_for_key(key=s3_key, bucket_name=bucket):
            raise FileNotFoundError(
                f"Quality check failed: properties.csv not found in S3 for dataset "
                f"'{dataset_id}': s3://{bucket}/{s3_key}"
            )

        raw = hook.read_key(key=s3_key, bucket_name=bucket).strip()
        if not raw:
            raise ValueError(f"Quality check failed: properties.csv is empty for dataset '{dataset_id}'")

        reader = csv.DictReader(io.StringIO(raw))
        found_columns = set(reader.fieldnames or [])
        missing = required_columns - found_columns
        if missing:
            raise ValueError(
                f"Quality check failed: properties.csv for dataset '{dataset_id}' is "
                f"missing columns: {sorted(missing)}. Found: {reader.fieldnames}"
            )

        rows = list(reader)
        if not rows:
            raise ValueError(
                f"Quality check failed: properties.csv for dataset '{dataset_id}' has a "
                f"header but no data rows."
            )

        logger.info(
            "Quality check passed for dataset '%s': %d rows with properties.",
            dataset_id,
            len(rows),
        )


with DAG(
    dag_id="cheminformatics_pipeline",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": _send_teams_alert,
    },
    params={
        "dataset_id": Param(
            default="",
            type=["string", "null"],
            description=(
                "Optional. If set, only this dataset is processed (manual "
                "reprocessing/backfill), regardless of the weekly scan. "
                "If left empty, the DAG scans s3://<bucket>/inputs/ for all "
                "new dataset pairs (<id>_scaffolds.csv + <id>_r_groups.csv)."
            ),
        ),
        "overwrite": Param(
            default=False,
            type="boolean",
            description=(
                "If True, reprocess dataset(s) even if outputs/<id>/molecules.csv "
                "already exists. If False (default), already-processed datasets "
                "are skipped."
            ),
        ),
    },
    tags=["cheminformatics", "molecules_generation", "properties_calculation"],
) as dag:
    t_start = EmptyOperator(task_id="start")

    t_discover = PythonOperator(
        task_id="discover_datasets",
        python_callable=discover_datasets,
    )

    t_run_generation = PythonOperator(
        task_id="run_molecule_generation",
        python_callable=run_molecule_generation,
    )

    t_quality_checks_molecules = PythonOperator(
        task_id="quality_checks_molecules",
        python_callable=quality_checks_molecules,
    )

    t_run_properties = PythonOperator(
        task_id="run_properties_calculation",
        python_callable=run_properties_calculation,
    )

    t_quality_checks_properties = PythonOperator(
        task_id="quality_checks_properties",
        python_callable=quality_checks_properties,
    )

    t_finish = EmptyOperator(
        task_id="finish",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    (
        t_start
        >> t_discover
        >> t_run_generation
        >> t_quality_checks_molecules
        >> t_run_properties
        >> t_quality_checks_properties
        >> t_finish
    )
