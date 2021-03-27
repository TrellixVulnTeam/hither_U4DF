#!/bin/bash

set -ex

cd .devcontainer
docker build -t magland/hither-dev:p2p .
docker push magland/hither-dev:p2p
