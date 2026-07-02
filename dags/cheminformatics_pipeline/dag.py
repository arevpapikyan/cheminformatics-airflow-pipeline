import csv
import io
import json
import logging
import os
import re
import subprocess
import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)


# TODO: replace hardcoded DEFAULT_USER_ID with a real service-account user
# once auth/attribution is implemented.
DEFAULT_USER_ID = 1

TEAMS_WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL", "")

S3_CONN_ID = "aws_s3"
INPUTS_PREFIX = "inputs/"

SCAFFOLDS_RE = re.compile(r"^inputs/(?P<dataset_id>[^/]+)_scaffolds\.csv$")
R_GROUPS_RE = re.compile(r"^inputs/(?P<dataset_id>[^/]+)_r_groups\.csv$")


def _get_s3_hook() -> S3Hook:
    return S3Hook(aws_conn_id=S3_CONN_ID)


def _get_session():
    engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)()


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


def _dataset_already_processed(session, dataset_id: str) -> bool:
    """
    A dataset counts as "already processed" if at least one MOLECULE_GENERATION
    task already exists in the DB with this dataset_id recorded in its params.
    This is used instead of a wall-clock "since last launch" timestamp so that
    manual re-triggers, backfills, and missed schedule runs all behave
    consistently — the DB (not a stored "last run time") is the source of
    truth for what has and hasn't been processed.
    """
    result = session.execute(
        text(
            "SELECT 1 FROM tasks "
            "WHERE task_type = 'MOLECULE_GENERATION' "
            "AND params ->> 'dataset_id' = :dataset_id "
            "LIMIT 1"
        ),
        {"dataset_id": dataset_id},
    ).first()
    return result is not None


def discover_datasets(**context) -> None:
    """
    Determines which dataset(s) this DAG run should process.

    - If the `dataset_id` param is explicitly set, only that dataset is
      considered (manual/backfill/reprocessing mode).
    - Otherwise, scans s3://<bucket>/inputs/ for any <id>_scaffolds.csv +
      <id>_r_groups.csv pairs (the weekly/scheduled mode).

    In both cases, a dataset is skipped if it was already processed
    (see _dataset_already_processed) unless params["overwrite"] is True.
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

        only_scaffolds = scaffold_ids - r_group_ids
        only_r_groups = r_group_ids - scaffold_ids
        for missing_id in sorted(only_scaffolds):
            logger.warning(
                "Dataset '%s' has a scaffolds.csv but no matching r_groups.csv — skipping.",
                missing_id,
            )
        for missing_id in sorted(only_r_groups):
            logger.warning(
                "Dataset '%s' has a r_groups.csv but no matching scaffolds.csv — skipping.",
                missing_id,
            )

    datasets = []
    session = _get_session()
    try:
        for dataset_id in sorted(candidate_ids):
            scaffolds_key = f"{INPUTS_PREFIX}{dataset_id}_scaffolds.csv"
            r_groups_key = f"{INPUTS_PREFIX}{dataset_id}_r_groups.csv"

            for key in (scaffolds_key, r_groups_key):
                try:
                    exists = hook.check_for_key(key=key, bucket_name=bucket)
                except Exception as e:
                    raise PermissionError(
                        f"Error accessing s3://{bucket}/{key} — check S3 credentials "
                        f"on the '{S3_CONN_ID}' connection."
                    ) from e
                if not exists:
                    raise FileNotFoundError(
                        f"Expected file not found in S3: s3://{bucket}/{key}"
                    )

            already_processed = _dataset_already_processed(session, dataset_id)
            if already_processed and not overwrite:
                logger.info(
                    "Dataset '%s' was already processed — skipping (set overwrite=True to reprocess).",
                    dataset_id,
                )
                continue

            if already_processed and overwrite:
                logger.info("Dataset '%s' was already processed — reprocessing (overwrite=True).", dataset_id)

            datasets.append({
                "dataset_id": dataset_id,
                "scaffolds_key": scaffolds_key,
                "r_groups_key": r_groups_key,
            })
    finally:
        session.close()

    if requested_dataset_id and not datasets:
        raise ValueError(
            f"Dataset '{requested_dataset_id}' was already processed and overwrite=False. "
            f"Set overwrite=True to reprocess it."
        )

    if not datasets:
        logger.info("No new datasets found to process. Nothing to do.")

    logger.info("Datasets to process this run: %s", [d["dataset_id"] for d in datasets])
    context["ti"].xcom_push(key="datasets", value=datasets)


def create_or_get_experiment(**context) -> None:
    session = _get_session()
    try:
        experiment_id = session.execute(
            text("SELECT id FROM experiments WHERE id = 1")
        ).scalar_one()
    finally:
        session.close()

    logger.info("Using experiment %s", experiment_id)
    context["ti"].xcom_push(key="experiment_id", value=experiment_id)


def create_molecule_generation_tasks(**context) -> None:
    """
    For every dataset discovered in discover_datasets, reads all scaffolds
    from that dataset's scaffolds CSV and creates one MOLECULE_GENERATION
    task per scaffold. dataset_id is stored in each task's params both so
    the worker/downstream steps can trace it back, and so future runs of
    this DAG can tell (via _dataset_already_processed) whether a dataset
    has already been handled.

    r_groups_artifact_id is filled in later in
    register_artifacts_and_finalize_params, once the R-groups artifact has
    been registered against each task (artifacts.task_id is NOT NULL, so
    the artifact can only be created after its owning task exists).
    """
    datasets: list = context["ti"].xcom_pull(task_ids="discover_datasets", key="datasets") or []
    experiment_id: int = context["ti"].xcom_pull(
        task_ids="create_or_get_experiment", key="experiment_id"
    )

    if not datasets:
        logger.info("No datasets to process — skipping task creation.")
        context["ti"].xcom_push(key="molecule_tasks", value=[])
        return

    bucket = os.environ["S3_BUCKET"]
    hook = _get_s3_hook()

    molecule_tasks = []
    session = _get_session()
    try:
        for dataset in datasets:
            dataset_id = dataset["dataset_id"]
            scaffolds_key = dataset["scaffolds_key"]
            r_groups_key = dataset["r_groups_key"]

            raw = hook.read_key(key=scaffolds_key, bucket_name=bucket)
            scaffold_smiles_list = [
                line.strip() for line in raw.strip().splitlines()[1:] if line.strip()
            ]

            if not scaffold_smiles_list:
                raise ValueError(f"No scaffolds found in {scaffolds_key}")

            for scaffold_smiles in scaffold_smiles_list:
                task_id = str(uuid.uuid4())
                params = json.dumps({
                    "scaffold": scaffold_smiles,
                    "dataset_id": dataset_id,
                })
                session.execute(
                    text(
                        "INSERT INTO tasks (id, task_type, status, params, experiment_id, created_by) "
                        "VALUES (:id, :task_type, :status, CAST(:params AS jsonb), :experiment_id, :created_by)"
                    ),
                    {
                        "id": task_id,
                        "task_type": "MOLECULE_GENERATION",
                        "status": "created",
                        "params": params,
                        "experiment_id": experiment_id,
                        "created_by": DEFAULT_USER_ID,
                    },
                )
                molecule_tasks.append({
                    "task_id": task_id,
                    "dataset_id": dataset_id,
                    "scaffolds_key": scaffolds_key,
                    "r_groups_key": r_groups_key,
                })

            logger.info(
                "Created %d MOLECULE_GENERATION tasks for dataset '%s'",
                len(scaffold_smiles_list),
                dataset_id,
            )
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    logger.info("Created %d MOLECULE_GENERATION tasks in total", len(molecule_tasks))
    context["ti"].xcom_push(key="molecule_tasks", value=molecule_tasks)


def register_artifacts_and_finalize_params(**context) -> None:
    """
    For every task created in create_molecule_generation_tasks:
      1. register that task's scaffolds CSV and R-groups CSV as artifacts
         linked to that task
      2. patch the task's params with the resulting r_groups_artifact_id
    """
    molecule_tasks: list = context["ti"].xcom_pull(
        task_ids="create_molecule_generation_tasks", key="molecule_tasks"
    ) or []

    if not molecule_tasks:
        logger.info("No tasks to register artifacts for — skipping.")
        return

    session = _get_session()
    try:
        for entry in molecule_tasks:
            task_id = entry["task_id"]
            dataset_id = entry["dataset_id"]
            scaffolds_key = entry["scaffolds_key"]
            r_groups_key = entry["r_groups_key"]

            scaffold_artifact_id = str(uuid.uuid4())
            session.execute(
                text(
                    "INSERT INTO artifacts (id, task_id, s3_key, filename, content_type, meta) "
                    "VALUES (:id, :task_id, :s3_key, :filename, :content_type, CAST(:meta AS jsonb))"
                ),
                {
                    "id": scaffold_artifact_id,
                    "task_id": task_id,
                    "s3_key": scaffolds_key,
                    "filename": f"{dataset_id}_scaffolds.csv",
                    "content_type": "text/csv",
                    "meta": "{}",
                },
            )

            r_groups_artifact_id = str(uuid.uuid4())
            session.execute(
                text(
                    "INSERT INTO artifacts (id, task_id, s3_key, filename, content_type, meta) "
                    "VALUES (:id, :task_id, :s3_key, :filename, :content_type, CAST(:meta AS jsonb))"
                ),
                {
                    "id": r_groups_artifact_id,
                    "task_id": task_id,
                    "s3_key": r_groups_key,
                    "filename": f"{dataset_id}_r_groups.csv",
                    "content_type": "text/csv",
                    "meta": "{}",
                },
            )

            session.execute(
                text(
                    "UPDATE tasks SET params = params || CAST(:patch AS jsonb) WHERE id = :id"
                ),
                {
                    "patch": json.dumps({"r_groups_artifact_id": r_groups_artifact_id}),
                    "id": task_id,
                },
            )

        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    logger.info("Registered artifacts and finalized params for %d tasks", len(molecule_tasks))


def run_molecule_generation(**context) -> None:
    molecule_tasks: list = context["ti"].xcom_pull(
        task_ids="create_molecule_generation_tasks", key="molecule_tasks"
    ) or []

    if not molecule_tasks:
        logger.info("No tasks to run — skipping.")
        return

    for entry in molecule_tasks:
        task_id = entry["task_id"]
        result = subprocess.run(
            [
                "docker", "run", "--rm",
                "--network", "local_deployment_default",
                "-e", f"DATABASE_URL={os.environ['DATABASE_URL']}",
                "-e", f"S3_ENDPOINT_URL={os.environ['S3_ENDPOINT_URL']}",
                "-e", f"S3_ACCESS_KEY={os.environ['S3_ACCESS_KEY']}",
                "-e", f"S3_SECRET_KEY={os.environ['S3_SECRET_KEY']}",
                "-e", f"S3_BUCKET={os.environ['S3_BUCKET']}",
                "-e", f"S3_REGION={os.environ.get('S3_REGION', 'us-east-1')}",
                "pipeline_worker",
                "python", "run.py", "--task-id", task_id,
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"molecules_generation worker failed for task {task_id} "
                f"(dataset '{entry['dataset_id']}'):\n{result.stderr}"
            )

        logger.info(
            "molecules_generation worker completed for task %s (dataset '%s')",
            task_id,
            entry["dataset_id"],
        )


def quality_checks_molecules(**context) -> None:
    """
    Post-generation quality checks on each molecules.csv output:
    - File exists in S3
    - File is non-empty
    - CSV has the expected 'smiles' column
    - All rows have a non-empty smiles value
    - At least one molecule was generated per task
    """
    molecule_tasks: list = context["ti"].xcom_pull(
        task_ids="create_molecule_generation_tasks", key="molecule_tasks"
    ) or []

    if not molecule_tasks:
        logger.info("No tasks to quality-check — skipping.")
        return

    bucket = os.environ["S3_BUCKET"]
    hook = _get_s3_hook()

    for entry in molecule_tasks:
        task_id = entry["task_id"]
        s3_key = f"tasks/{task_id}/artifacts/molecules.csv"

        if not hook.check_for_key(key=s3_key, bucket_name=bucket):
            raise FileNotFoundError(
                f"Quality check failed: molecules.csv not found in S3 for task {task_id} "
                f"(dataset '{entry['dataset_id']}'): s3://{bucket}/{s3_key}"
            )

        raw = hook.read_key(key=s3_key, bucket_name=bucket).strip()

        if not raw:
            raise ValueError(
                f"Quality check failed: molecules.csv is empty for task {task_id} "
                f"(dataset '{entry['dataset_id']}')"
            )

        reader = csv.DictReader(io.StringIO(raw))

        if "smiles" not in (reader.fieldnames or []):
            raise ValueError(
                f"Quality check failed: molecules.csv for task {task_id} "
                f"(dataset '{entry['dataset_id']}') is missing the 'smiles' column. "
                f"Found: {reader.fieldnames}"
            )

        rows = list(reader)

        if not rows:
            raise ValueError(
                f"Quality check failed: molecules.csv for task {task_id} "
                f"(dataset '{entry['dataset_id']}') has a header but no data rows."
            )

        empty_smiles = [i + 2 for i, row in enumerate(rows) if not row.get("smiles", "").strip()]
        if empty_smiles:
            raise ValueError(
                f"Quality check failed: molecules.csv for task {task_id} "
                f"(dataset '{entry['dataset_id']}') has empty smiles values on rows: {empty_smiles}"
            )

        logger.info(
            "Quality check passed for task %s (dataset '%s'): %d molecules generated.",
            task_id,
            entry["dataset_id"],
            len(rows),
        )


def create_properties_calculation_tasks(**context) -> None:
    """
    For each MOLECULE_GENERATION task, looks up the molecules.csv artifact
    that the generation worker already saved (via repo.save_molecules_file
    inside its own transaction — see gen/services.py in the worker) and
    creates one PROPERTIES_CALCULATION task per generation task, referencing
    that artifact via molecules_artifact_id.

    Unlike molecule generation, no separate "register artifacts" step is
    needed here: the molecules.csv artifact already exists (its owning task
    — the generation task — already exists too), so there's no NOT NULL
    ordering constraint to work around.
    """
    molecule_tasks: list = context["ti"].xcom_pull(
        task_ids="create_molecule_generation_tasks", key="molecule_tasks"
    ) or []

    if not molecule_tasks:
        logger.info("No generation tasks to create properties_calculation tasks for — skipping.")
        context["ti"].xcom_push(key="properties_tasks", value=[])
        return

    properties_tasks = []
    session = _get_session()
    try:
        for entry in molecule_tasks:
            gen_task_id = entry["task_id"]
            dataset_id = entry["dataset_id"]

            artifact_row = session.execute(
                text(
                    "SELECT id FROM artifacts "
                    "WHERE task_id = :task_id AND filename = 'molecules.csv' "
                    "ORDER BY created_at DESC LIMIT 1"
                ),
                {"task_id": gen_task_id},
            ).first()

            if artifact_row is None:
                raise ValueError(
                    f"No molecules.csv artifact found for generation task {gen_task_id} "
                    f"(dataset '{dataset_id}') — cannot create properties_calculation task."
                )
            molecules_artifact_id = artifact_row[0]

            prop_task_id = str(uuid.uuid4())
            params = json.dumps({
                "molecules_artifact_id": molecules_artifact_id,
                "dataset_id": dataset_id,
            })
            session.execute(
                text(
                    "INSERT INTO tasks (id, task_type, status, params, experiment_id, created_by) "
                    "SELECT :id, :task_type, :status, CAST(:params AS jsonb), experiment_id, :created_by "
                    "FROM tasks WHERE id = :gen_task_id"
                ),
                {
                    "id": prop_task_id,
                    "task_type": "PROPERTIES_CALCULATION",
                    "status": "created",
                    "params": params,
                    "created_by": DEFAULT_USER_ID,
                    "gen_task_id": gen_task_id,
                },
            )
            properties_tasks.append({
                "task_id": prop_task_id,
                "dataset_id": dataset_id,
                "gen_task_id": gen_task_id,
            })

        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    logger.info("Created %d PROPERTIES_CALCULATION tasks", len(properties_tasks))
    context["ti"].xcom_push(key="properties_tasks", value=properties_tasks)


def run_properties_calculation(**context) -> None:
    properties_tasks: list = context["ti"].xcom_pull(
        task_ids="create_properties_calculation_tasks", key="properties_tasks"
    ) or []

    if not properties_tasks:
        logger.info("No properties_calculation tasks to run — skipping.")
        return

    for entry in properties_tasks:
        task_id = entry["task_id"]
        result = subprocess.run(
            [
                "docker", "run", "--rm",
                "--network", "local_deployment_default",
                "-e", f"DATABASE_URL={os.environ['DATABASE_URL']}",
                "-e", f"S3_ENDPOINT_URL={os.environ['S3_ENDPOINT_URL']}",
                "-e", f"S3_ACCESS_KEY={os.environ['S3_ACCESS_KEY']}",
                "-e", f"S3_SECRET_KEY={os.environ['S3_SECRET_KEY']}",
                "-e", f"S3_BUCKET={os.environ['S3_BUCKET']}",
                "-e", f"S3_REGION={os.environ.get('S3_REGION', 'us-east-1')}",
                "pipeline_worker",
                "python", "run.py", "--task-id", task_id,
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"properties_calculation worker failed for task {task_id} "
                f"(dataset '{entry['dataset_id']}'):\n{result.stderr}"
            )

        logger.info(
            "properties_calculation worker completed for task %s (dataset '%s')",
            task_id,
            entry["dataset_id"],
        )


def quality_checks_properties(**context) -> None:
    """
    Post-calculation quality checks on each properties.csv output:
    - File exists in S3
    - File is non-empty
    - CSV has the expected property columns
    - At least one row of results
    """
    properties_tasks: list = context["ti"].xcom_pull(
        task_ids="create_properties_calculation_tasks", key="properties_tasks"
    ) or []

    if not properties_tasks:
        logger.info("No properties_calculation tasks to quality-check — skipping.")
        return

    bucket = os.environ["S3_BUCKET"]
    hook = _get_s3_hook()
    required_columns = {"mol_weight", "log_p", "tpsa", "hba", "hbd"}

    for entry in properties_tasks:
        task_id = entry["task_id"]
        s3_key = f"tasks/{task_id}/artifacts/properties.csv"

        if not hook.check_for_key(key=s3_key, bucket_name=bucket):
            raise FileNotFoundError(
                f"Quality check failed: properties.csv not found in S3 for task {task_id} "
                f"(dataset '{entry['dataset_id']}'): s3://{bucket}/{s3_key}"
            )

        raw = hook.read_key(key=s3_key, bucket_name=bucket).strip()

        if not raw:
            raise ValueError(
                f"Quality check failed: properties.csv is empty for task {task_id} "
                f"(dataset '{entry['dataset_id']}')"
            )

        reader = csv.DictReader(io.StringIO(raw))
        found_columns = set(reader.fieldnames or [])
        missing = required_columns - found_columns
        if missing:
            raise ValueError(
                f"Quality check failed: properties.csv for task {task_id} "
                f"(dataset '{entry['dataset_id']}') is missing columns: {sorted(missing)}. "
                f"Found: {reader.fieldnames}"
            )

        rows = list(reader)
        if not rows:
            raise ValueError(
                f"Quality check failed: properties.csv for task {task_id} "
                f"(dataset '{entry['dataset_id']}') has a header but no data rows."
            )

        logger.info(
            "Quality check passed for task %s (dataset '%s'): %d rows with properties.",
            task_id,
            entry["dataset_id"],
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
                "new dataset pairs (<id>_scaffolds.csv + <id>_r_groups.csv) "
                "since they were last processed."
            ),
        ),
        "overwrite": Param(
            default=False,
            type="boolean",
            description=(
                "If True, reprocess dataset(s) even if they were already "
                "processed before. If False (default), already-processed "
                "datasets are skipped."
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

    t_experiment = PythonOperator(
        task_id="create_or_get_experiment",
        python_callable=create_or_get_experiment,
    )

    t_create_tasks = PythonOperator(
        task_id="create_molecule_generation_tasks",
        python_callable=create_molecule_generation_tasks,
    )

    t_register_artifacts = PythonOperator(
        task_id="register_artifacts_and_finalize_params",
        python_callable=register_artifacts_and_finalize_params,
    )

    t_run = PythonOperator(
        task_id="run_molecule_generation",
        python_callable=run_molecule_generation,
    )

    t_quality_checks_molecules = PythonOperator(
        task_id="quality_checks_molecules",
        python_callable=quality_checks_molecules,
    )

    t_create_properties_tasks = PythonOperator(
        task_id="create_properties_calculation_tasks",
        python_callable=create_properties_calculation_tasks,
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
        >> t_experiment
        >> t_create_tasks
        >> t_register_artifacts
        >> t_run
        >> t_quality_checks_molecules
        >> t_create_properties_tasks
        >> t_run_properties
        >> t_quality_checks_properties
        >> t_finish
    )
