#!/bin/bash
set -ex

exec python -m pytest --container --remote --cov hither2 --cov-report=term --cov-report=xml:cov.xml -s -rA --durations=0 "$@"