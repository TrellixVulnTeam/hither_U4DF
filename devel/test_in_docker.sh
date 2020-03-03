#!/bin/bash

set -ex

allargs="$@"
exec docker run \
  -v $PWD:/workspaces/hither2 \
  -v /etc/passwd:/etc/passwd:ro \
  -v /etc/group:/etc/group:ro \
  -v /run/docker.sock:/run/docker.sock \
  -e DOCKER_HOST=unix:///run/docker.sock \
  --group-add docker \
  -u $(id -u):$(id -g) \
  -v /tmp:/tmp \
  --net=host \
  -w /workspaces/hither2 \
  -it magland/hither2-dev \
  /bin/bash -c "PYTHONPATH=/workspaces/hither2 PATH=/workspaces/hither2/bin:\$PATH devel/test.sh $allargs"

#-e HOST_IP="$(ip -4 addr show docker0 | grep -Po 'inet \K[\d.]+')" \
