#!/bin/bash
set -ex

exec python -m pytest --cov hither2 --cov-report=term --cov-report=xml:cov.xml -s "$@"