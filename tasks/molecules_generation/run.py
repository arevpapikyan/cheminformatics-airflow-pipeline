import argparse
import logging
import os
import sys

from dotenv import load_dotenv

from gen.decorators import send_exceptions_to_teams, timer
from gen.repository import TaskRepository, get_session
from gen.services import MoleculeGenerationService

load_dotenv()

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

TEAMS_WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL", "")

@timer
@send_exceptions_to_teams(TEAMS_WEBHOOK_URL)
def run():
    parser = argparse.ArgumentParser(description="Run molecule_generation task.")
    parser.add_argument(
        "--task-id",
        required=True,
        help="UUID of the task in the database",
    )
    args = parser.parse_args()

    with get_session() as session:
        repo = TaskRepository(session)
        task = repo.get_by_id(args.task_id)

        repo.mark_running(task)
        session.flush()

        try:
            MoleculeGenerationService(task, repo, session).run()
            repo.mark_done(task)
        except Exception as e:
            logging.exception(e)
            repo.mark_failed(task, str(e))
            raise # added raise so send_exceptions_to_teams sees errors in here too


if __name__ == "__main__":
    run()
