#!/bin/bash

exec docker run \
  -v $PWD:/workspaces/hither2 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /tmp:/tmp \
  -e HOST_IP="$(ip -4 addr show docker0 | grep -Po 'inet \K[\d.]+')" \
  -w /workspaces/hither2 \
  -it magland/hither2-dev \
  python -m pytest --cov hither2 --cov-report=xml:cov.xml -s 
