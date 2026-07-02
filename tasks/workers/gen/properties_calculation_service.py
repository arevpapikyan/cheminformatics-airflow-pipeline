from __future__ import annotations

import csv
import io
import logging
from typing import Any

from rdkit import Chem
from rdkit.Chem import Descriptors, rdMolDescriptors

from .repository import TaskRepository
from .utils import download_file_from_s3, file_exists_in_s3, upload_file_to_s3

logger = logging.getLogger(__name__)


class PropertiesCalculationService:
    PROPERTIES: dict[str, Any] = {
        "mol_weight": Descriptors.MolWt,
        "log_p": Descriptors.MolLogP,
        "tpsa": Descriptors.TPSA,
        "hba": rdMolDescriptors.CalcNumHBA,
        "hbd": rdMolDescriptors.CalcNumHBD,
        "rotatable_bonds": rdMolDescriptors.CalcNumRotatableBonds,
        "aromatic_rings": rdMolDescriptors.CalcNumAromaticRings,
        "lipinski_pass": lambda mol: bool(
            # Lipinski's Rule of Five
            Descriptors.MolWt(mol) <= 500
            and Descriptors.MolLogP(mol) <= 5
            and rdMolDescriptors.CalcNumHBA(mol) <= 10
            and rdMolDescriptors.CalcNumHBD(mol) <= 5
        )}

    def __init__(self, task, repo: TaskRepository, session) -> None:
        self._task = task
        self._repo = repo
        self._session = session
        self._smiles_column = self._task.params.get("smiles_column", "smiles")

    def _validate_params(self) -> None:
        params = self._task.params

        if not params.get("molecules_artifact_id"):
            raise ValueError("molecules_artifact_id is required.")

        artifact = self._repo.get_artifact_by_id(params["molecules_artifact_id"])
        if not file_exists_in_s3(artifact.s3_key):
            raise ValueError(f"Molecules file not found in S3: {artifact.s3_key}")

    def _load_molecules(self) -> list[dict[str, str]]:
        artifact = self._repo.get_artifact_by_id(self._task.params["molecules_artifact_id"])
        raw = download_file_from_s3(artifact.s3_key)

        reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))

        if self._smiles_column not in (reader.fieldnames or []):
            raise ValueError(
                f"SMILES column '{self._smiles_column}' not found in CSV. "
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

    def _calculate_properties(self, rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], dict]:
        result = []
        failed = 0

        for row in rows:
            smi = row[self._smiles_column].strip()
            mol = Chem.MolFromSmiles(smi)
            if mol is None:
                failed += 1
                continue

            # Exclude bool properties (e.g. lipinski_pass) from rounding
            props = {}
            for name, fn in self.PROPERTIES.items():
                value = fn(mol)
                props[name] = round(value, 4) if isinstance(value, float) else value

            result.append({**row, **props})

        stats = {
            "total_input": len(rows),
            "total_calculated": len(result),
            "failed": failed,
        }
        logger.info(f"Properties calculation complete: {stats}")
        return result, stats

    def _upload_results(self, rows: list[dict], stats: dict) -> None:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        csv_bytes = buf.getvalue().encode("utf-8")

        csv_key = f"tasks/{self._task.id}/artifacts/properties.csv"
        upload_file_to_s3(csv_bytes, csv_key, content_type="text/csv")
        self._repo.save_molecules_file(
            task=self._task,
            s3_key=csv_key,
            filename="properties.csv",
            content_type="text/csv",
            meta={"total_calculated": stats["total_calculated"]},
        )

    def run(self) -> None:
        self._validate_params()
        rows = self._load_molecules()

        if not rows:
            raise ValueError("No valid molecules found in input file.")

        result_rows, stats = self._calculate_properties(rows)

        if not result_rows:
            raise ValueError("Properties calculation produced 0 results.")

        self._upload_results(result_rows, stats)
        self._session.flush()
