#!/usr/bin/env bash
# Force-rebuilds the worker image after code changes.
# up.sh skips building if the image already exists — use this instead
# whenever you've changed anything under tasks/workers/.
#
# Usage: ./rebuild.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Rebuilding pipeline_worker..."
(cd "$REPO_ROOT/tasks/workers" && docker build -t pipeline_worker .)

echo "==> Done."
