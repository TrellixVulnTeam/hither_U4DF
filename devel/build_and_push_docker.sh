#!/bin/bash

set -ex

cd .devcontainer
docker build -t magland/hither2-dev .
docker push magland/hither2-dev