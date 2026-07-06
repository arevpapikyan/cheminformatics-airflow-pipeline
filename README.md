# Cheminformatics Airflow Pipeline

An Airflow pipeline for a cheminformatics drug-discovery workflow. Scientists drop `<id>_scaffolds.csv` / `<id>_r_groups.csv` pairs into an S3 bucket. The pipeline generates molecules, calculates their properties, clusters them, trains a ChemProp model to predict their properties, and builds an interactive chemical-space graph, fully S3-driven, with no database involved.

## Pipeline stages

```
inputs/<id>_scaffolds.csv ─┐
inputs/<id>_r_groups.csv ──┴─▶ generate ─▶ properties ─▶ cluster ─▶ chemprop ─▶ faerun
                                  │             │            │          │          │
                                  ▼             ▼            ▼          ▼          ▼
                          molecules.csv  properties.csv clusters.csv chemprop_  tmap_graph.zip
                                                                    predictions.csv
```

Every stage writes to `outputs/<id>/` in the same S3 bucket, and every stage is followed by a data-quality-check task before the next one runs.

1. **Molecule generation**: combines each scaffold's attachment points (`[*:1]`, `[*:2]`, ...) with every combination of R-groups from the matching `r_groups.csv`, positionally matched by column (`R1`, `R2`, ...). Invalid combinations are skipped, not fatal. Writes `outputs/<id>/molecules.csv`.
2. **Properties calculation**: RDKit-calculated `mol_weight`, `log_p`, `tpsa`, `hba`, `hbd`, `rotatable_bonds`, `aromatic_rings`, and a `lipinski_pass` flag. Writes `outputs/<id>/properties.csv`.
3. **Clustering**: Morgan/ECFP4 fingerprints plus K-means. `k` is auto-picked via a `sqrt(n_molecules / 2)` heuristic (clamped to `[2, 20]`), or set explicitly with the `n_clusters` DAG param. Writes `outputs/<id>/clusters.csv`.
4. **ChemProp prediction**: trains a multitask D-MPNN regression model predicting every numeric property directly from SMILES, plus a separate binary classifier for `lipinski_pass` (using ChemProp's `binary-mcc` loss to handle class imbalance, with a fallback to the default loss if unsupported). Writes `outputs/<id>/chemprop_predictions.csv` with actual vs. predicted values, error metrics, and classification accuracy, logged in full, including `regression_summary` (MAE/RMSE/R²) and `classification_summary` (accuracy, balanced accuracy, per-class recall).
5. **Faerun graph build**: fingerprints the same molecules, lays them out with TMAP, and renders an interactive chemical-space graph with `faerun` (hover to see structures, colored by cluster if available). Writes `outputs/<id>/tmap_graph.zip` (and `faerun_invalid.csv` if any molecules failed SMILES validation at this stage).

## Two ways to trigger it

- **Manual**: set the `dataset_id` param to a specific `<id>` and trigger the DAG; only that dataset is (re)processed.
- **Scheduled**: the DAG also runs `@weekly`. With no `dataset_id` set, it scans `s3://<bucket>/inputs/` for every `<id>_scaffolds.csv` + `<id>_r_groups.csv` pair and processes any dataset that doesn't already have `outputs/<id>/molecules.csv`, i.e. only genuinely new datasets, not everything every week.

## DAG params

| Param               | Default     | Purpose                                                                      |
| ------------------- | ----------- | ---------------------------------------------------------------------------- |
| `dataset_id`      | *(empty)* | Process only this dataset; empty = scan for all new datasets                 |
| `overwrite`       | `False`   | Reprocess a dataset even if its outputs already exist                        |
| `n_clusters`      | *(auto)*  | Force a specific K-means`k`; empty = auto heuristic                        |
| `chemprop_epochs` | `5`       | Training epochs for ChemProp (kept low so the stage runs quickly by default) |

## Data quality checks

Every stage's output is validated before the next stage runs: file exists, non-empty, has the expected columns, has data rows, and (where relevant) that row counts match the previous stage's output. See the `quality_checks_*` functions in `dags/cheminformatics_pipeline/dag.py` for the exact checks per stage. These are structural/completeness checks; they catch a broken pipeline run, not a weak model. Model-quality signals (R², balanced accuracy, etc.) are computed and logged separately by the ChemProp stage.

## Notifications

Any task failure posts to an MS Teams channel via an incoming webhook (`TEAMS_WEBHOOK_URL` env var). If it's not set, this is skipped with a warning rather than failing the pipeline.

## Repository layout

```
dags/cheminformatics_pipeline/
  .airflowignore
  .env.example
  dag.py                        # the DAG: discovery, stages, quality checks, alerts
  docker-compose.airflow.yml    # Airflow (webserver/scheduler) stack
infra/
  fixtures/                     # sample datasets loaded into MinIO on startup
  .env.example
  docker-compose.local.yml      # local MinIO (S3) + bootstrap
workers/
  gen/
    __init__.py
    chemprop_prediction_service.py
    decorators.py
    faerun_graph_service.py
    molecules_clustering_service.py
    molecules_generation_service.py
    properties_calculation_service.py
    utils.py                    # S3 helpers
  Dockerfile                    # pipeline_worker image
  pyproject.toml                # ruff/mypy config
  requirements.txt
  run.py                        # CLI: generate | properties | cluster | chemprop | faerun
.gitignore
README.md
```

`dags/` is volume-mounted read-only into the Airflow container, so DAG changes are picked up automatically. `workers/` is **not** mounted; it's a separate Docker image invoked via `docker run pipeline_worker ...` from inside the Airflow container (using the Docker socket), so any change under `workers/` needs an explicit rebuild:

```bash
docker build -t pipeline_worker ./workers
```

## Running locally

```bash
# 1. Start MinIO (S3), also loads infra/fixtures/*_scaffolds.csv + *_r_groups.csv
docker compose -f infra/docker-compose.local.yml up -d --build

# 2. Build the pipeline worker image
docker build -t pipeline_worker ./workers

# 3. Start Airflow
docker compose -f dags/cheminformatics_pipeline/docker-compose.airflow.yml up -d --build
```

Airflow's webserver comes up on its usual port. Trigger `cheminformatics_pipeline` from the UI, either with `dataset_id` set to one of the fixtures (e.g. `ABC123`) or left empty to pick up everything under `inputs/`.

To fully reset local state (MinIO data included):

```bash
docker compose -f dags/cheminformatics_pipeline/docker-compose.airflow.yml down
docker compose -f infra/docker-compose.local.yml down -v
docker compose -f infra/docker-compose.local.yml up -d --build
docker compose -f dags/cheminformatics_pipeline/docker-compose.airflow.yml up -d --build
```

`down -v` is required to actually clear MinIO's stored data. A plain `down` intentionally leaves volumes in place.

## Environment variables

Required by both the Airflow container and the `pipeline_worker` image (see `infra/.env.example` and `dags/cheminformatics_pipeline/.env.example`):

| Variable                              | Purpose                                             |
| ------------------------------------- | --------------------------------------------------- |
| `S3_ENDPOINT_URL`                   | MinIO endpoint (local) or S3 endpoint               |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | Credentials                                         |
| `S3_BUCKET`                         | Bucket name for`inputs/` and `outputs/`         |
| `S3_REGION`                         | Defaults to`us-east-1` if unset                   |
| `TEAMS_WEBHOOK_URL`                 | Optional; enables failure notifications to MS Teams |

## Notes on a couple of dependencies

- **ChemProp** pulls in PyTorch and PyTorch Lightning, by far the heaviest dependency in `requirements.txt`, and it noticeably slows down the `pipeline_worker` image build.
- **TMAP**: the original `reymond-group/tmap` package is no longer maintained and has no reliable pip install path (it depends on OGDF, a separate C++ library, with no documented from-source pip build). This project uses **`tmap2`**, the maintainers' own actively-maintained successor, which installs as a plain compiled wheel with no build toolchain needed.
- **faerun** pulls in `matplotlib`, which is pinned to `<3.11`. `faerun`'s own code calls `matplotlib.cm.get_cmap()`, an API matplotlib deprecated in 3.7 and removed outright in 3.11.

## Sample datasets

- `ABC123`: a small (12-molecule) fixture, mainly useful for quickly checking the pipeline runs end-to-end. Too small for the ChemProp stage to produce a meaningful model (expect negative R² across the board; this is underfitting from too little data, not a bug), and its `lipinski_pass` is `True` for every molecule, so the ChemProp classification stage is skipped for this dataset (logged, not an error).
- `BIGSET1`: a larger (605-molecule), more diverse fixture: 5 scaffolds x 2 attachment points x 11 R-groups per position, deliberately including substituents that push some molecules past Lipinski's thresholds and give `hbd` a real spread of values (0-4) rather than just 0/1. Large and varied enough for clustering and ChemProp to have something real to learn from.
