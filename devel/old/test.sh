#!/bin/bash
set -ex

exec python -m pytest --pyargs hither --container --remote --cov hither --cov-report=term --cov-report=xml:cov.xml -s -rA --durations=0 "$@"