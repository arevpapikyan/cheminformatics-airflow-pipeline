import argparse
import logging
import sys

from dotenv import load_dotenv

from gen.chemprop_prediction_service import ChemPropPredictionService
from gen.decorators import timer
from gen.molecules_clustering_service import MoleculesClusteringService
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


@timer
def run_cluster(dataset_id: str, n_clusters: int | None) -> None:
    MoleculesClusteringService(dataset_id, n_clusters=n_clusters).run()


@timer
def run_chemprop(dataset_id: str, epochs: int) -> None:
    ChemPropPredictionService(dataset_id, epochs=epochs).run()


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

    cluster_parser = subparsers.add_parser(
        "cluster", help="Cluster a dataset's generated molecules with K-means."
    )
    cluster_parser.add_argument("--dataset-id", required=True)
    cluster_parser.add_argument(
        "--n-clusters",
        type=int,
        default=None,
        help="Optional. If omitted, k is chosen automatically via a sqrt(n/2) heuristic.",
    )

    chemprop_parser = subparsers.add_parser(
        "chemprop",
        help="Train a multitask ChemProp model on a dataset's properties.csv and predict back on it.",
    )
    chemprop_parser.add_argument("--dataset-id", required=True)
    chemprop_parser.add_argument(
        "--epochs",
        type=int,
        default=5,
        help="Training epochs. Kept low by default so the stage runs quickly in a pipeline "
        "context; increase for a more meaningful model.",
    )

    args = parser.parse_args()

    try:
        if args.stage == "generate":
            run_generate(args.dataset_id)
        elif args.stage == "properties":
            run_properties(args.dataset_id)
        elif args.stage == "cluster":
            run_cluster(args.dataset_id, args.n_clusters)
        elif args.stage == "chemprop":
            run_chemprop(args.dataset_id, args.epochs)
    except Exception as e:
        logging.exception(e)
        raise


if __name__ == "__main__":
    main()
