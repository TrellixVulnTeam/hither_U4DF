#!/bin/bash

set -ex

exec docker run \
  -v $PWD:/workspaces/hither2 \
  -v /etc/passwd:/etc/passwd:ro \
  -v /etc/group:/etc/group:ro \
  -v /run/docker.sock:/run/docker.sock \
  -e DOCKER_HOST=unix:///run/docker.sock \
  -e KACHERY_STORAGE_DIR=$KACHERY_STORAGE_DIR \
  --group-add docker \
  -u $(id -u):$(id -g) \
  -v /tmp:/tmp \
  -e HOST_IP="$(ip -4 addr show docker0 | grep -Po 'inet \K[\d.]+')" \
  -w /workspaces/hither2 \
  -it magland/hither2-dev \
  python -m pytest --cov hither2 --cov-report=term --cov-report=xml:cov.xml -s 
