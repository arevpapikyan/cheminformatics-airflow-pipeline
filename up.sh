#!/usr/bin/env bash
# One-command startup for local development.
# Brings up shared infra, builds worker images (only if missing), then starts Airflow.
#
# Usage: ./up.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Starting shared infra (Postgres + MinIO)..."
(cd "$REPO_ROOT/tasks/local_deployment" && docker compose -f docker-compose.local.yml up -d)

echo "==> Building worker image (skipped if already built)..."
if ! docker image inspect pipeline_worker >/dev/null 2>&1; then
  echo "    Building pipeline_worker..."
  (cd "$REPO_ROOT/tasks/workers" && docker build -t pipeline_worker .)
else
  echo "    pipeline_worker already built, skipping (run ./rebuild.sh to force)."
fi

echo "==> Starting Airflow..."
(cd "$REPO_ROOT/dags/cheminformatics_pipeline" && docker compose -f docker-compose.airflow.yml up -d)

echo "==> Done. Airflow UI: http://localhost:8080"
