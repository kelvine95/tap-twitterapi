#!/bin/sh
set -euo pipefail
unset VIRTUAL_ENV

TOML_DIR=$(cd "$(dirname "$0")" && pwd)
cd "$TOML_DIR"

poetry install 1>&2
poetry run tap-twitterapi "$@"
