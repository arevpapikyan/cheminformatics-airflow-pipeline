from gen.decorators import send_exceptions_to_teams, timer
from gen.molecules_generation_service import MoleculeGenerationService
from gen.properties_calculation_service import PropertiesCalculationService
from gen.repository import TaskRepository, get_session

import argparse
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

TEAMS_WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL", "")

# Maps a task's task_type (already stored on the task row) to the service
# that knows how to run it. Adding a new pipeline step later just means
# adding one entry here plus its service module — no new image/Dockerfile
# needed.
SERVICE_MAP = {
    "MOLECULE_GENERATION": MoleculeGenerationService,
    "PROPERTIES_CALCULATION": PropertiesCalculationService,
}


@timer
@send_exceptions_to_teams(TEAMS_WEBHOOK_URL)
def run():
    parser = argparse.ArgumentParser(description="Run a pipeline task by task-id.")
    parser.add_argument(
        "--task-id",
        required=True,
        help="UUID of the task in the database",
    )
    args = parser.parse_args()

    with get_session() as session:
        repo = TaskRepository(session)
        task = repo.get_by_id(args.task_id)

        service_cls = SERVICE_MAP.get(task.task_type)
        if service_cls is None:
            raise ValueError(
                f"No worker registered for task_type '{task.task_type}'. "
                f"Known types: {sorted(SERVICE_MAP.keys())}"
            )

        repo.mark_running(task)
        session.flush()

        try:
            service_cls(task, repo, session).run()
            repo.mark_done(task)
        except Exception as e:
            logging.exception(e)
            repo.mark_failed(task, str(e))
            raise  # added raise so send_exceptions_to_teams sees errors in here too


if __name__ == "__main__":
    run()
