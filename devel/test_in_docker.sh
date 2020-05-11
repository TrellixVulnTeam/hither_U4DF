#!/bin/bash

set -ex

allargs="$@"
exec docker run \
  -v $PWD:/workspaces/hither \
  -v /etc/passwd:/etc/passwd:ro \
  -v /etc/group:/etc/group:ro \
  -v /run/docker.sock:/run/docker.sock \
  -e DOCKER_HOST=unix:///run/docker.sock \
  --group-add docker \
  -u $(id -u):$(id -g) \
  -v /tmp:/tmp \
  --net=host \
  -w /workspaces/hither \
  -it magland/hither-dev \
  /bin/bash -c "PYTHONPATH=/workspaces/hither PATH=/workspaces/hither/bin:\$PATH devel/test.sh $allargs"

#-e HOST_IP="$(ip -4 addr show docker0 | grep -Po 'inet \K[\d.]+')" \
