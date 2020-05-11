#!/bin/bash

set -ex

cd .devcontainer
docker build -t magland/hither-dev .
docker push magland/hither-dev
