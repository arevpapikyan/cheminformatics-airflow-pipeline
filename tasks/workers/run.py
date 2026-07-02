import argparse
import logging
import sys

from dotenv import load_dotenv

from gen.decorators import timer
from gen.molecules_generation_service import MoleculeGenerationService
from gen.properties_calculation_service import PropertiesCalculationService

load_dotenv()

logging.basicConfig(level=logging.INFO, stream=sys.stdout)


@timer
def run_generate(dataset_id: str) -> None:
    MoleculeGenerationService(dataset_id).run()


@timer
def run_properties(dataset_id: str) -> None:
    PropertiesCalculationService(dataset_id).run()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a cheminformatics pipeline stage.")
    subparsers = parser.add_subparsers(dest="stage", required=True)

    generate_parser = subparsers.add_parser(
        "generate", help="Generate molecules from scaffolds + r_groups for a dataset."
    )
    generate_parser.add_argument("--dataset-id", required=True)

    properties_parser = subparsers.add_parser(
        "properties", help="Calculate properties for a dataset's generated molecules."
    )
    properties_parser.add_argument("--dataset-id", required=True)

    args = parser.parse_args()

    try:
        if args.stage == "generate":
            run_generate(args.dataset_id)
        elif args.stage == "properties":
            run_properties(args.dataset_id)
    except Exception as e:
        logging.exception(e)
        raise


if __name__ == "__main__":
    main()
