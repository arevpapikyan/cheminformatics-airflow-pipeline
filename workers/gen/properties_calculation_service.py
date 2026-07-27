import csv
import io
import logging
from typing import Any

from rdkit import Chem
from rdkit.Chem import Descriptors, rdMolDescriptors

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
        ),
    }

    def __init__(self, dataset_id: str, smiles_column: str = "smiles") -> None:
        self._dataset_id = dataset_id
        self._smiles_column = smiles_column
        self._molecules_key = f"outputs/{dataset_id}/molecules.csv"
        self._output_key = f"outputs/{dataset_id}/properties.csv"

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

    def _calculate_properties(self, rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], dict]:
        result = []
        failed = 0

        for row in rows:
            smi = row[self._smiles_column].strip()
            mol = Chem.MolFromSmiles(smi)
            if mol is None:
                failed += 1
                continue

            props = {}
            for name, fn in self.PROPERTIES.items():
                value = fn(mol)
                props[name] = round(value, 4) if isinstance(value, float) else value

            result.append({"smiles": smi, **props})

        stats = {
            "total_input": len(rows),
            "total_calculated": len(result),
            "failed": failed,
        }
        logger.info(f"Properties calculation complete: {stats}")
        return result, stats

    def run(self) -> dict:
        self._validate_inputs()
        rows = self._load_molecules()

        if not rows:
            raise ValueError(f"No valid molecules found in {self._molecules_key}.")

        result_rows, stats = self._calculate_properties(rows)

        if not result_rows:
            raise ValueError(f"Properties calculation produced 0 results for dataset '{self._dataset_id}'.")

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(result_rows[0].keys()))
        writer.writeheader()
        writer.writerows(result_rows)
        csv_bytes = buf.getvalue().encode("utf-8")

        upload_file_to_s3(csv_bytes, self._output_key, content_type="text/csv")

        logger.info(
            f"Dataset '{self._dataset_id}': {stats['total_calculated']} molecules -> {self._output_key}"
        )
        return {**stats, "output_key": self._output_key}
