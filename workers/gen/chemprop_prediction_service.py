import csv
import io
import logging
import shutil
import subprocess
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .properties_calculation_service import PropertiesCalculationService
from .utils import download_file_from_s3, file_exists_in_s3, upload_file_to_s3

logger = logging.getLogger(__name__)

_CLASSIFICATION_TARGET_COLUMN = "lipinski_pass"
_CLASSIFICATION_DECISION_THRESHOLD = 0.5
_COUNT_TARGET_COLUMNS = {"aromatic_rings", "hba", "hbd", "rotatable_bonds"}
_NON_NEGATIVE_CONTINUOUS_COLUMNS = {"mol_weight", "tpsa"}
_CLASSIFICATION_LOSS_FUNCTION = "binary-mcc"


def _parse_bool(value: str) -> bool:
    return str(value).strip().lower() in ("true", "1", "yes")


def _bool_to_binary_label(_column: str, value: str) -> str:
    """value_fn for _train: converts a True/False column into the "1"/"0" labels
    ChemProp expects for a binary classification target column."""
    return "1" if _parse_bool(value) else "0"


class ChemPropPredictionService:
    """
    Trains two ChemProp (D-MPNN) models per dataset, directly from SMILES:
    1. A multitask regression model predicting every continuous property in
       properties.csv (mol_weight, log_p, tpsa, hba, hbd, rotatable_bonds,
       aromatic_rings) at once.
    2. A binary classification model predicting lipinski_pass. This has to be
       a separate model/training run from the regression one because ChemProp's
       task-type is set per training run, so a boolean target can't be mixed
       into a "regression" multitask run alongside continuous ones.
    """

    def __init__(
        self,
        dataset_id: str,
        epochs: int = 5,
        smiles_column: str = "smiles",
    ) -> None:
        self._dataset_id = dataset_id
        self._epochs = epochs
        self._smiles_column = smiles_column
        self._properties_key = f"outputs/{dataset_id}/properties.csv"
        self._output_key = f"outputs/{dataset_id}/chemprop_predictions.csv"
        self._classification_target = _CLASSIFICATION_TARGET_COLUMN
        self._regression_targets = sorted(
            set(PropertiesCalculationService.PROPERTIES.keys()) - {self._classification_target}
        )

    def _validate_inputs(self) -> None:
        if not file_exists_in_s3(self._properties_key):
            raise ValueError(
                f"properties.csv not found for dataset '{self._dataset_id}': "
                f"{self._properties_key}. Has the properties calculation stage run yet?"
            )

    def _load_properties(self) -> list[dict[str, str]]:
        raw = download_file_from_s3(self._properties_key)
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))

        found_columns = set(reader.fieldnames or [])
        required = {self._smiles_column, self._classification_target, *self._regression_targets}
        missing = required - found_columns
        if missing:
            raise ValueError(
                f"properties.csv for dataset '{self._dataset_id}' is missing expected "
                f"columns: {sorted(missing)}. Found: {reader.fieldnames}"
            )

        rows = [row for row in reader if row.get(self._smiles_column, "").strip()]
        return rows

    def _write_csv(self, path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def _train(
        self,
        work_dir: Path,
        rows: list[dict[str, str]],
        task_type: str,
        target_columns: list[str],
        run_name: str,
        value_fn: Callable[[str, str], str] | None = None,
        extra_args: list[str] | None = None,
    ) -> Path:
        train_csv = work_dir / f"train_{run_name}.csv"
        out_rows = []
        for r in rows:
            entry: dict[str, Any] = {self._smiles_column: r[self._smiles_column]}
            for col in target_columns:
                entry[col] = value_fn(col, r[col]) if value_fn else r[col]
            out_rows.append(entry)
        self._write_csv(train_csv, out_rows, [self._smiles_column, *target_columns])

        model_dir = work_dir / f"model_{run_name}"
        cmd = [
            "chemprop", "train",
            "--data-path", str(train_csv),
            "--task-type", task_type,
            "--output-dir", str(model_dir),
            "--epochs", str(self._epochs),
            "--smiles-columns", self._smiles_column,
            "--target-columns", *target_columns,
            *(extra_args or []),
        ]
        logger.info(f"Training ChemProp '{run_name}' model for dataset '{self._dataset_id}': {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"chemprop train ({run_name}) failed for dataset '{self._dataset_id}' "
                f"(exit code {result.returncode}):\n{result.stderr[-4000:]}"
            )

        checkpoint = model_dir / "model_0" / "best.pt"
        if not checkpoint.exists():
            candidates = sorted(model_dir.rglob("*.pt")) + sorted(model_dir.rglob("*.ckpt"))
            if not candidates:
                raise RuntimeError(
                    f"chemprop train ({run_name}) did not produce a checkpoint under "
                    f"{model_dir} for dataset '{self._dataset_id}'."
                )
            checkpoint = candidates[0]
            logger.warning(f"Expected checkpoint at model_0/best.pt, using {checkpoint} instead.")

        return checkpoint

    def _train_classification(self, work_dir: Path, rows: list[dict[str, str]]) -> Path:
        target_columns = [self._classification_target]

        try:
            return self._train(
                work_dir,
                rows,
                "classification",
                target_columns,
                "classification",
                value_fn=_bool_to_binary_label,
                extra_args=["--loss-function", _CLASSIFICATION_LOSS_FUNCTION],
            )
        except RuntimeError as exc:
            logger.warning(
                f"Dataset '{self._dataset_id}': chemprop train with "
                f"--loss-function {_CLASSIFICATION_LOSS_FUNCTION} failed, retrying with "
                f"the default classification loss instead. Original error: {exc}"
            )
            return self._train(
                work_dir,
                rows,
                "classification",
                target_columns,
                "classification_retry",
                value_fn=_bool_to_binary_label,
            )

    def _predict(
        self,
        work_dir: Path,
        checkpoint: Path,
        rows: list[dict[str, str]],
        run_name: str,
    ) -> Path:
        test_csv = work_dir / f"test_{run_name}.csv"
        self._write_csv(
            test_csv,
            [{self._smiles_column: r[self._smiles_column]} for r in rows],
            [self._smiles_column],
        )

        preds_csv = work_dir / f"predictions_{run_name}.csv"
        cmd = [
            "chemprop", "predict",
            "--test-path", str(test_csv),
            "--model-path", str(checkpoint),
            "--preds-path", str(preds_csv),
            "--smiles-columns", self._smiles_column,
        ]
        logger.info(f"Running ChemProp '{run_name}' predictions for dataset '{self._dataset_id}': {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"chemprop predict ({run_name}) failed for dataset '{self._dataset_id}' "
                f"(exit code {result.returncode}):\n{result.stderr[-4000:]}"
            )

        if not preds_csv.exists():
            raise RuntimeError(
                f"chemprop predict ({run_name}) reported success but {preds_csv} was not "
                f"created for dataset '{self._dataset_id}'."
            )

        return preds_csv

    def _merge_regression(self, rows: list[dict[str, str]], preds_csv: Path) -> list[dict[str, Any]]:
        with preds_csv.open() as f:
            pred_reader = csv.DictReader(f)
            pred_fieldnames = pred_reader.fieldnames or []
            pred_rows = list(pred_reader)

        if len(pred_rows) != len(rows):
            raise RuntimeError(
                f"chemprop predict (regression) returned {len(pred_rows)} rows but "
                f"{len(rows)} molecules were submitted for dataset '{self._dataset_id}'."
            )

        pred_columns = [c for c in pred_fieldnames if c != self._smiles_column]
        if len(pred_columns) != len(self._regression_targets):
            raise RuntimeError(
                f"chemprop predict (regression) returned {len(pred_columns)} prediction "
                f"column(s) {pred_columns}, but {len(self._regression_targets)} target(s) "
                f"{self._regression_targets} were requested for dataset '{self._dataset_id}'."
            )

        merged = []
        for row, pred_row in zip(rows, pred_rows, strict=True):
            entry: dict[str, Any] = {self._smiles_column: row[self._smiles_column]}
            for target, pred_col in zip(self._regression_targets, pred_columns, strict=True):
                actual = float(row[target])
                predicted = float(pred_row[pred_col])
                if target in _COUNT_TARGET_COLUMNS:
                    predicted = max(0, round(predicted))
                elif target in _NON_NEGATIVE_CONTINUOUS_COLUMNS:
                    predicted = max(0.0, predicted)
                entry[f"actual_{target}"] = actual
                entry[f"predicted_{target}"] = predicted
                entry[f"abs_error_{target}"] = abs(actual - predicted)
            merged.append(entry)

        return merged

    def _merge_classification(
        self, rows: list[dict[str, str]], preds_csv: Path, result_rows: list[dict[str, Any]]
    ) -> None:
        """Mutates result_rows in place, adding lipinski_pass columns alongside the regression ones."""
        with preds_csv.open() as f:
            pred_reader = csv.DictReader(f)
            pred_fieldnames = pred_reader.fieldnames or []
            pred_rows = list(pred_reader)

        if len(pred_rows) != len(rows):
            raise RuntimeError(
                f"chemprop predict (classification) returned {len(pred_rows)} rows but "
                f"{len(rows)} molecules were submitted for dataset '{self._dataset_id}'."
            )

        pred_columns = [c for c in pred_fieldnames if c != self._smiles_column]
        if len(pred_columns) != 1:
            raise RuntimeError(
                f"chemprop predict (classification) returned {len(pred_columns)} prediction "
                f"column(s) {pred_columns}, expected exactly 1 for dataset '{self._dataset_id}'."
            )
        pred_col = pred_columns[0]

        target = self._classification_target
        for entry, row, pred_row in zip(result_rows, rows, pred_rows, strict=True):
            actual = _parse_bool(row[target])
            probability = float(pred_row[pred_col])
            predicted = probability >= _CLASSIFICATION_DECISION_THRESHOLD
            entry[f"actual_{target}"] = actual
            entry[f"predicted_{target}_probability"] = probability
            entry[f"predicted_{target}"] = predicted
            entry[f"correct_{target}"] = actual == predicted

    def _classification_summary(self, result_rows: list[dict[str, Any]]) -> dict:
        target = self._classification_target
        n = len(result_rows)
        correct = sum(1 for r in result_rows if r[f"actual_{target}"] == r[f"predicted_{target}"])

        tp = sum(1 for r in result_rows if r[f"actual_{target}"] and r[f"predicted_{target}"])
        fn = sum(1 for r in result_rows if r[f"actual_{target}"] and not r[f"predicted_{target}"])
        tn = sum(1 for r in result_rows if not r[f"actual_{target}"] and not r[f"predicted_{target}"])
        fp = sum(1 for r in result_rows if not r[f"actual_{target}"] and r[f"predicted_{target}"])

        true_recall = tp / (tp + fn) if (tp + fn) else None
        false_recall = tn / (tn + fp) if (tn + fp) else None
        balanced_accuracy = (
            (true_recall + false_recall) / 2 if true_recall is not None and false_recall is not None else None
        )

        return {
            "accuracy": correct / n if n else None,
            "balanced_accuracy": balanced_accuracy,
            "true_class_recall": true_recall,
            "false_class_recall": false_recall,
        }

    def _regression_summary(self, result_rows: list[dict[str, Any]]) -> dict:
        n = len(result_rows)
        summary: dict[str, dict[str, float | None]] = {}

        for target in self._regression_targets:
            actual = [r[f"actual_{target}"] for r in result_rows]
            predicted = [r[f"predicted_{target}"] for r in result_rows]
            errors = [a - p for a, p in zip(actual, predicted, strict=True)]

            mae = sum(abs(e) for e in errors) / n
            rmse = (sum(e**2 for e in errors) / n) ** 0.5

            mean_actual = sum(actual) / n
            ss_res = sum(e**2 for e in errors)
            ss_tot = sum((a - mean_actual) ** 2 for a in actual)
            r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else None

            summary[target] = {"mae": mae, "rmse": rmse, "r2": r2}

        return summary

    def run(self) -> dict:
        self._validate_inputs()
        rows = self._load_properties()

        if not rows:
            raise ValueError(f"No molecules found in {self._properties_key}.")

        if len(rows) < 3:
            raise ValueError(
                f"Dataset '{self._dataset_id}' has only {len(rows)} molecule(s) — "
                f"ChemProp needs a train/validation split, so at least a handful of "
                f"molecules are required. Skipping is likely more appropriate than "
                f"forcing a split here."
            )

        work_dir = Path(tempfile.mkdtemp(prefix=f"chemprop_{self._dataset_id}_"))
        classification_ran = False
        try:
            reg_checkpoint = self._train(work_dir, rows, "regression", self._regression_targets, "regression")
            reg_preds_csv = self._predict(work_dir, reg_checkpoint, rows, "regression")
            result_rows = self._merge_regression(rows, reg_preds_csv)
            regression_summary = self._regression_summary(result_rows)

            distinct_classes = {_parse_bool(r[self._classification_target]) for r in rows}
            classification_summary: dict | None = None
            if len(distinct_classes) < 2:
                logger.warning(
                    f"Dataset '{self._dataset_id}': every molecule has "
                    f"{self._classification_target}={distinct_classes.pop()} — nothing for a "
                    f"classifier to distinguish. Skipping the classification stage."
                )
            else:
                clf_checkpoint = self._train_classification(work_dir, rows)
                clf_preds_csv = self._predict(work_dir, clf_checkpoint, rows, "classification")
                self._merge_classification(rows, clf_preds_csv, result_rows)
                classification_ran = True
                classification_summary = self._classification_summary(result_rows)
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

        fieldnames = [self._smiles_column]
        for target in self._regression_targets:
            fieldnames += [f"actual_{target}", f"predicted_{target}", f"abs_error_{target}"]
        if classification_ran:
            target = self._classification_target
            fieldnames += [
                f"actual_{target}",
                f"predicted_{target}_probability",
                f"predicted_{target}",
                f"correct_{target}",
            ]

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(result_rows)
        csv_bytes = buf.getvalue().encode("utf-8")

        upload_file_to_s3(csv_bytes, self._output_key, content_type="text/csv")

        stats = {
            "total_input": len(rows),
            "regression_targets": self._regression_targets,
            "regression_summary": regression_summary,
            "classification_target": self._classification_target if classification_ran else None,
            "classification_summary": classification_summary if classification_ran else None,
            "epochs": self._epochs,
            "output_key": self._output_key,
        }
        logger.info(f"Dataset '{self._dataset_id}': ChemProp predictions written -> {stats}")
        return stats
