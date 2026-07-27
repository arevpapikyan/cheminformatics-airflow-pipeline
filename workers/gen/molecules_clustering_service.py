import csv
import io
import logging
import math
from typing import Any

import numpy as np
from rdkit import Chem
from rdkit.Chem import rdFingerprintGenerator
from sklearn.cluster import KMeans

from .utils import download_file_from_s3, file_exists_in_s3, upload_file_to_s3

logger = logging.getLogger(__name__)

# Molecules aren't numbers, so before K-means can run, each one gets
# encoded into a fixed-length binary vector (a "fingerprint").
FINGERPRINT_RADIUS = 2 # RDKit's default
FINGERPRINT_BITS = 2048 # RDKit's default

# Bounds for the auto-picked k (tunable defaults, not derived from any analysis.)
MIN_CLUSTERS = 2
MAX_CLUSTERS = 20


class MoleculesClusteringService:
    def __init__(self, dataset_id: str, n_clusters: int | None = None, smiles_column: str = "smiles") -> None:
        self._dataset_id = dataset_id
        self._smiles_column = smiles_column
        self._n_clusters_override = n_clusters
        self._molecules_key = f"outputs/{dataset_id}/molecules.csv"
        self._output_key = f"outputs/{dataset_id}/clusters.csv"

    def _validate_inputs(self) -> None:
        if not file_exists_in_s3(self._molecules_key):
            raise ValueError(
                f"molecules.csv not found for dataset '{self._dataset_id}': "
                f"{self._molecules_key}. Has the molecule generation stage run yet?"
            )

    def _load_molecules(self) -> list[dict[str, str]]:
        raw = download_file_from_s3(self._molecules_key)
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))

        if self._smiles_column not in (reader.fieldnames or []):
            raise ValueError(
                f"SMILES column '{self._smiles_column}' not found in {self._molecules_key}. "
                f"Available columns: {reader.fieldnames}"
            )

        rows = []
        skipped = 0
        for row in reader:
            smi = row.get(self._smiles_column, "").strip()
            if smi and Chem.MolFromSmiles(smi) is not None:
                rows.append(row)
            else:
                logger.warning(f"Skipping invalid SMILES: {smi!r}")
                skipped += 1

        if skipped:
            logger.warning(f"Skipped {skipped} rows with invalid SMILES")

        return rows

    def _fingerprint(self, smi: str) -> np.ndarray:
        mol = Chem.MolFromSmiles(smi)
        generator = rdFingerprintGenerator.GetMorganGenerator(
            radius=FINGERPRINT_RADIUS, fpSize=FINGERPRINT_BITS
        )
        fp = generator.GetFingerprint(mol)
        arr = np.zeros((FINGERPRINT_BITS,), dtype=np.int8)
        Chem.DataStructs.ConvertToNumpyArray(fp, arr)
        return arr

    def _resolve_n_clusters(self, n_molecules: int) -> int:
        if self._n_clusters_override is not None:
            if self._n_clusters_override < 1:
                raise ValueError(f"n_clusters must be >= 1, got {self._n_clusters_override}")
            k = self._n_clusters_override
        else:
            # Rule-of-thumb heuristic: k ~= sqrt(n / 2), has a range.
            k = round(math.sqrt(n_molecules / 2))
            k = max(MIN_CLUSTERS, min(MAX_CLUSTERS, k))

        # KMeans requires n_clusters <= n_samples.
        if k > n_molecules:
            logger.warning(
                f"Requested n_clusters={k} exceeds n_molecules={n_molecules}; "
                f"capping k to {n_molecules} (one cluster per molecule)."
            )
            k = n_molecules
        return k

    def _cluster(self, rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], dict]:
        smiles_list = [row[self._smiles_column].strip() for row in rows]
        fingerprints = np.array([self._fingerprint(smi) for smi in smiles_list])

        n_clusters = self._resolve_n_clusters(len(smiles_list))

        if n_clusters == 1:
            labels = np.zeros(len(smiles_list), dtype=int)
        else:
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            labels = kmeans.fit_predict(fingerprints)

        result = [
            {"smiles": smi, "cluster": int(label)} for smi, label in zip(smiles_list, labels, strict=True)
        ]

        stats = {
            "total_input": len(rows),
            "n_clusters": n_clusters,
        }
        logger.info(f"Clustering complete: {stats}")
        return result, stats

    def run(self) -> dict:
        self._validate_inputs()
        rows = self._load_molecules()

        if not rows:
            raise ValueError(f"No valid molecules found in {self._molecules_key}.")

        result_rows, stats = self._cluster(rows)

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=["smiles", "cluster"])
        writer.writeheader()
        writer.writerows(result_rows)
        csv_bytes = buf.getvalue().encode("utf-8")

        upload_file_to_s3(csv_bytes, self._output_key, content_type="text/csv")

        logger.info(
            f"Dataset '{self._dataset_id}': {stats['total_input']} molecules -> "
            f"{stats['n_clusters']} clusters -> {self._output_key}"
        )
        return {**stats, "output_key": self._output_key}
