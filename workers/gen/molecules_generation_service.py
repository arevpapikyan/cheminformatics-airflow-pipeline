import csv
import io
import itertools
import logging
from typing import Any

from rdkit import Chem

from .utils import download_file_from_s3, upload_file_to_s3

logger = logging.getLogger(__name__)


class MoleculeGenerationService:
    MAX_MOLECULES_PER_SCAFFOLD_DEFAULT = 50_000

    def __init__(self, dataset_id: str, max_molecules_per_scaffold: int | None = None) -> None:
        self._dataset_id = dataset_id
        self._scaffolds_key = f"inputs/{dataset_id}_scaffolds.csv"
        self._r_groups_key = f"inputs/{dataset_id}_r_groups.csv"
        self._output_key = f"outputs/{dataset_id}/molecules.csv"
        self._max_molecules = max_molecules_per_scaffold or self.MAX_MOLECULES_PER_SCAFFOLD_DEFAULT

    def _load_scaffolds(self) -> list[str]:
        raw = download_file_from_s3(self._scaffolds_key)
        scaffolds = [
            line.strip() for line in raw.decode("utf-8").strip().splitlines()[1:] if line.strip()
        ]
        if not scaffolds:
            raise ValueError(f"No scaffolds found in {self._scaffolds_key}")
        return scaffolds

    def _load_r_groups(self) -> dict[str, list[str]]:
        raw = download_file_from_s3(self._r_groups_key)
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))
        result: dict[str, list[str]] = {col: [] for col in (reader.fieldnames or [])}
        skipped_count = 0
        for row in reader:
            cleaned = {col: smi.strip() for col, smi in row.items()}
            if all(smi and Chem.MolFromSmiles(smi) is not None for smi in cleaned.values()):
                for col, smi in cleaned.items():
                    result[col].append(smi)
            else:
                logger.warning(f"Skipping invalid row: {cleaned}")
                skipped_count += 1
        if skipped_count:
            logger.warning(f"Skipped {skipped_count} invalid rows during R-group parsing")

        logger.info(f"Loaded R-groups: { {k: len(v) for k, v in result.items()} }")
        return result

    def _generate_for_scaffold(
        self, scaffold_smiles: str, r_groups: dict[str, list[str]]
    ) -> tuple[list[dict[str, Any]], dict]:
        scaffold_mol = Chem.MolFromSmiles(scaffold_smiles)
        if scaffold_mol is None:
            raise ValueError(f"Invalid scaffold: {scaffold_smiles!r}")

        attachment_points = [a for a in scaffold_mol.GetAtoms() if a.GetAtomicNum() == 0]
        if not attachment_points:
            raise ValueError(
                f"Scaffold {scaffold_smiles!r} has no [*] attachment points. "
                "Use [*] to mark substitution positions."
            )

        ordered_labels = sorted(r_groups.keys())
        if len(attachment_points) != len(ordered_labels):
            raise ValueError(
                f"Scaffold {scaffold_smiles!r} has {len(attachment_points)} attachment "
                f"point(s) but {len(ordered_labels)} R-group column(s) were provided: "
                f"{ordered_labels}."
            )

        rows: list[dict[str, Any]] = []
        total_attempted = skipped_invalid = 0

        for combo in itertools.product(*[r_groups[lbl] for lbl in ordered_labels]):
            if total_attempted >= self._max_molecules:
                logger.warning(
                    f"Reached max_molecules_per_scaffold cap ({self._max_molecules}) "
                    f"for scaffold {scaffold_smiles!r}. Stopping early."
                )
                break
            total_attempted += 1

            try:
                tm = Chem.RWMol(scaffold_mol)
                for smi in combo:
                    r_mol = Chem.MolFromSmiles(smi)
                    if r_mol is None:
                        raise ValueError(f"Invalid R-group SMILES: {smi}")
                    tm.InsertMol(r_mol)
                prod = Chem.molzip(tm)
                Chem.SanitizeMol(prod)
            except Exception as exc:
                logger.debug(f"Failed to generate molecule for combo {combo}: {exc}")
                skipped_invalid += 1
                continue

            row: dict[str, Any] = {
                "scaffold": scaffold_smiles,
                "smiles": Chem.MolToSmiles(prod),
            }
            for label, smi in zip(ordered_labels, combo, strict=True):
                row[label] = smi
            rows.append(row)

        stats = {
            "total_attempted": total_attempted,
            "total_valid": len(rows),
            "skipped_invalid": skipped_invalid,
        }
        logger.info(f"Generation complete for scaffold {scaffold_smiles!r}: {stats}")
        return rows, stats

    def run(self) -> dict:
        scaffolds = self._load_scaffolds()
        r_groups = self._load_r_groups()

        all_rows: list[dict[str, Any]] = []
        totals = {"total_attempted": 0, "total_valid": 0, "skipped_invalid": 0}

        for scaffold_smiles in scaffolds:
            rows, stats = self._generate_for_scaffold(scaffold_smiles, r_groups)
            all_rows.extend(rows)
            for key in totals:
                totals[key] += stats[key]

        if not all_rows:
            raise ValueError(
                f"Generation produced 0 valid molecules across {len(scaffolds)} "
                f"scaffold(s) for dataset '{self._dataset_id}'."
            )

        fieldnames = ["scaffold", "smiles"] + sorted(r_groups.keys())
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
        csv_bytes = buf.getvalue().encode("utf-8")

        upload_file_to_s3(csv_bytes, self._output_key, content_type="text/csv")

        logger.info(
            f"Dataset '{self._dataset_id}': {len(scaffolds)} scaffold(s) -> "
            f"{totals['total_valid']} total molecules -> {self._output_key}"
        )
        return {**totals, "scaffolds_processed": len(scaffolds), "output_key": self._output_key}
