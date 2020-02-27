#!/bin/bash

set -ex

exec docker run \
  -v $PWD:/workspaces/hither2 \
  -v /etc/passwd:/etc/passwd -u `id -u`:`id -g` \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp:/tmp \
  -e HOST_IP="$(ip -4 addr show docker0 | grep -Po 'inet \K[\d.]+')" \
  -w /workspaces/hither2 \
  -it magland/hither2-dev \
  python -m pytest --cov hither2 --cov-report=xml:cov.xml -s 
