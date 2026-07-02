from __future__ import annotations

import csv
import io
import itertools
import logging
from typing import Any

from rdkit import Chem

from .repository import TaskRepository
from .utils import download_file_from_s3, file_exists_in_s3, upload_file_to_s3

logger = logging.getLogger(__name__)


class MoleculeGenerationService:
    MAX_MOLECULES_DEFAULT = 50_000

    def __init__(self, task, repo: TaskRepository, session) -> None:
        self._task = task
        self._repo = repo
        self._session = session
        self._scaffold = self._task.params["scaffold"]
        self._max_molecules = int(self._task.params.get("max_molecules", self.MAX_MOLECULES_DEFAULT))
        self._r_groups = None

    def _validate_params(self) -> None:
        params = self._task.params

        if not params.get("scaffold"):
            raise ValueError("scaffold is required.")
        if not params.get("r_groups_artifact_id"):
            raise ValueError("r_groups_artifact_id is required.")

        try:
            scaffold_mol = Chem.MolFromSmiles(params["scaffold"])
        except Exception:
            scaffold_mol = None

        if not scaffold_mol:
            raise ValueError(f"Invalid scaffold: {params['scaffold']!r}")

        attachment_points = [a for a in scaffold_mol.GetAtoms() if a.GetAtomicNum() == 0]
        if not attachment_points:
            raise ValueError(
                "Scaffold contains no [*] attachment points. "
                "Use [*] to mark substitution positions."
            )

        artifact = self._repo.get_artifact_by_id(params["r_groups_artifact_id"])
        if not file_exists_in_s3(artifact.s3_key):
            raise ValueError(f"R-Groups file not found in S3: {artifact.s3_key}")

    def _load_r_groups(self) -> dict[str, list[str]]:
        artifact = self._repo.get_artifact_by_id(self._task.params["r_groups_artifact_id"])
        raw = download_file_from_s3(artifact.s3_key)

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
        self._r_groups = result

    def _generate(self) -> tuple[list[dict[str, Any]], dict]:
        scaffold_mol = Chem.MolFromSmiles(self._scaffold)
        ordered_labels = sorted(self._r_groups.keys())
        attachment_points = [a for a in scaffold_mol.GetAtoms() if a.GetAtomicNum() == 0]

        if len(attachment_points) != len(ordered_labels):
            raise ValueError(
                f"Scaffold has {len(attachment_points)} attachment point(s) but "
                f"{len(ordered_labels)} R-group(s) were provided: {ordered_labels}."
            )

        rows: list[dict[str, Any]] = []
        total_attempted = skipped_invalid = 0

        for combo in itertools.product(*[self._r_groups[lbl] for lbl in ordered_labels]):
            if total_attempted >= self._max_molecules:
                logger.warning(f"Reached max_molecules cap ({self._max_molecules}). Stopping early.")
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

            row: dict[str, Any] = {"smiles": Chem.MolToSmiles(prod)}
            for label, smi in zip(ordered_labels, combo, strict=True):
                row[label] = smi
            rows.append(row)

        logger.info(
            f"Generation complete: "
            f"- attempted={total_attempted} "
            f"- valid={len(rows)} "
            f"- skipped={skipped_invalid}"
        )
        return rows, {
            "total_attempted": total_attempted,
            "total_valid": len(rows),
            "skipped_invalid": skipped_invalid,
        }

    def _upload_results(self, rows: list[dict], stats: dict) -> None:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        csv_bytes = buf.getvalue().encode("utf-8")

        csv_key = f"tasks/{self._task.id}/artifacts/molecules.csv"
        upload_file_to_s3(csv_bytes, csv_key, content_type="text/csv")
        self._repo.save_molecules_file(
            task=self._task,
            s3_key=csv_key,
            filename="molecules.csv",
            content_type="text/csv",
            meta={
                "total_generated": stats["total_valid"],
            },
        )

    def run(self) -> None:
        self._validate_params()
        self._load_r_groups()
        rows, stats = self._generate()

        if not rows:
            raise ValueError(
                "Generation produced 0 valid molecules. Check scaffold and R-groups."
            )

        self._upload_results(rows, stats)
        self._session.flush()
