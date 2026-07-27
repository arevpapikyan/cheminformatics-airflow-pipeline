import csv
import io
import logging
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from faerun import Faerun
from rdkit import Chem
from tmap import TMAP
from tmap.utils.chemistry import fingerprints_from_smiles

from .properties_calculation_service import PropertiesCalculationService
from .utils import download_file_from_s3, file_exists_in_s3, upload_file_to_s3

logger = logging.getLogger(__name__)

# lipinski_pass is boolean, not something meaningful to colour a continuous
# scatter plot by, so it's excluded from the color candidate list even though
# it's a valid properties.csv column.
_COLOR_CANDIDATE_COLUMNS = [
    c for c in PropertiesCalculationService.PROPERTIES if c != "lipinski_pass"
]

# tmap's n_neighbors should scale with dataset size
_N_NEIGHBORS_TIERS = [(10_000, 10), (100_000, 20), (500_000, 50)]
_N_NEIGHBORS_DEFAULT = 100

_ARCHIVE_FILE_NAME = "tmap"


def _pick_n_neighbors(n: int) -> int:
    for threshold, k in _N_NEIGHBORS_TIERS:
        if n < threshold:
            return k
    return _N_NEIGHBORS_DEFAULT


class FaerunGraphService:
    def __init__(
        self,
        dataset_id: str,
        fingerprint_type: str = "morgan",
        fingerprint_radius: int = 2,
        fingerprint_bits: int = 2048,
        smiles_column: str = "smiles",
    ) -> None:
        self._dataset_id = dataset_id
        self._fingerprint_type = fingerprint_type
        self._fingerprint_radius = fingerprint_radius
        self._fingerprint_bits = fingerprint_bits
        self._smiles_column = smiles_column
        self._properties_key = f"outputs/{dataset_id}/properties.csv"
        self._clusters_key = f"outputs/{dataset_id}/clusters.csv"
        self._archive_key = f"outputs/{dataset_id}/tmap_graph.zip"
        self._invalid_key = f"outputs/{dataset_id}/faerun_invalid.csv"

    def _validate_inputs(self) -> None:
        if not file_exists_in_s3(self._properties_key):
            raise ValueError(
                f"properties.csv not found for dataset '{self._dataset_id}': "
                f"{self._properties_key}. Has the properties calculation stage run yet?"
            )

    def _load_rows(self) -> list[dict[str, str]]:
        raw = download_file_from_s3(self._properties_key)
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))
        rows = [row for row in reader if row.get(self._smiles_column, "").strip()]

        if not file_exists_in_s3(self._clusters_key):
            return rows

        cluster_raw = download_file_from_s3(self._clusters_key)
        cluster_rows = list(csv.DictReader(io.StringIO(cluster_raw.decode("utf-8"))))
        if len(cluster_rows) != len(rows):
            logger.warning(
                f"Dataset '{self._dataset_id}': clusters.csv has {len(cluster_rows)} rows "
                f"but properties.csv has {len(rows)}; skipping the cluster color merge "
                f"(clustering may have run on a different set of molecules)."
            )
            return rows

        for row, cluster_row in zip(rows, cluster_rows, strict=True):
            row["cluster"] = cluster_row["cluster"]
        return rows

    def _separate_valid_invalid(
        self, rows: list[dict[str, str]]
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        valid, invalid = [], []
        for row in rows:
            smi = row[self._smiles_column].strip()
            if Chem.MolFromSmiles(smi) is not None:
                valid.append(row)
            else:
                invalid.append(row)

        logger.info(
            f"Dataset '{self._dataset_id}': molecule validation — "
            f"valid={len(valid)}, invalid={len(invalid)}"
        )
        return valid, invalid

    def _pick_color_column(self, rows: list[dict[str, str]]) -> str | None:
        if rows and "cluster" in rows[0]:
            return "cluster"
        for candidate in _COLOR_CANDIDATE_COLUMNS:
            if rows and candidate in rows[0]:
                return candidate
        return None

    def _color_values(self, rows: list[dict[str, str]], color_column: str | None) -> list[float]:
        if color_column is None:
            return list(range(len(rows)))

        raw = [float(row[color_column]) for row in rows]

        # Clip to 1st-99th percentile so a handful of extreme outliers don't
        # compress the rest of the colormap into an unreadably narrow band.
        sorted_vals = sorted(raw)
        n = len(sorted_vals)
        p1 = sorted_vals[max(0, int(n * 0.01))]
        p99 = sorted_vals[min(n - 1, int(n * 0.99))]
        return [min(max(v, p1), p99) for v in raw]

    def _build_layout(self, rows: list[dict[str, str]]) -> tuple[list[float], list[float], list[int], list[int]]:
        smiles_list = [row[self._smiles_column].strip() for row in rows]
        fingerprints = fingerprints_from_smiles(
            smiles_list,
            fp_type=self._fingerprint_type,
            radius=self._fingerprint_radius,
            n_bits=self._fingerprint_bits,
        )

        n_neighbors = min(_pick_n_neighbors(len(smiles_list)), len(smiles_list) - 1)
        model = TMAP(metric="jaccard", n_neighbors=n_neighbors, seed=42).fit(fingerprints)

        x = model.embedding_[:, 0].tolist()
        y = model.embedding_[:, 1].tolist()
        s = model.tree_.edges[:, 0].tolist()
        t = model.tree_.edges[:, 1].tolist()
        return x, y, s, t

    def _build_faerun_graph(
        self,
        rows: list[dict[str, str]],
        layout: tuple[list[float], list[float], list[int], list[int]],
        output_dir: Path,
    ) -> None:
        x, y, s, t = layout
        color_column = self._pick_color_column(rows)
        colors = self._color_values(rows, color_column)
        labels = [f"{row[self._smiles_column].strip()}__{i}" for i, row in enumerate(rows)]

        logger.info(
            f"Dataset '{self._dataset_id}': coloring faerun graph by "
            f"{color_column or 'molecule index (no property/cluster available)'}"
        )

        f = Faerun(
            title=f"{self._dataset_id} chemical space",
            clear_color="#111111",
            coords=False,
            view="front",
            alpha_blending=True,
        )
        f.add_scatter(
            "molecules",
            {"x": x, "y": y, "c": colors, "labels": labels},
            shader="smoothCircle",
            point_scale=2.5,
            max_point_size=10,
            has_legend=True,
            colormap="rainbow",
        )
        f.add_tree("molecules_tree", {"from": s, "to": t}, point_helper="molecules")
        f.plot(file_name=_ARCHIVE_FILE_NAME, path=str(output_dir), template="smiles")

    def _upload_graph_archive(self, output_dir: Path) -> None:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for path in output_dir.rglob("*"):
                if path.is_file():
                    zf.write(path, arcname=path.relative_to(output_dir))
        buf.seek(0)
        upload_file_to_s3(buf.read(), self._archive_key, content_type="application/zip")

    def _upload_invalid_csv(self, invalid_rows: list[dict[str, Any]]) -> None:
        if not invalid_rows:
            return

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(invalid_rows[0].keys()))
        writer.writeheader()
        writer.writerows(invalid_rows)
        upload_file_to_s3(buf.getvalue().encode("utf-8"), self._invalid_key, content_type="text/csv")

    def run(self) -> dict:
        self._validate_inputs()
        rows = self._load_rows()

        if not rows:
            raise ValueError(f"No molecules found in {self._properties_key}.")

        valid_rows, invalid_rows = self._separate_valid_invalid(rows)

        if len(valid_rows) < 3:
            raise ValueError(
                f"Dataset '{self._dataset_id}' has only {len(valid_rows)} valid molecule(s); "
                f"TMAP needs a handful of points to build a meaningful graph."
            )

        layout = self._build_layout(valid_rows)

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            self._build_faerun_graph(valid_rows, layout, output_dir)
            self._upload_graph_archive(output_dir)

        self._upload_invalid_csv(invalid_rows)

        stats = {
            "total_input": len(rows),
            "valid": len(valid_rows),
            "invalid": len(invalid_rows),
            "color_column": self._pick_color_column(valid_rows),
            "fingerprint_type": self._fingerprint_type,
            "output_key": self._archive_key,
        }
        logger.info(f"Dataset '{self._dataset_id}': faerun graph written -> {stats}")
        return stats
