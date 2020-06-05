#!/bin/bash

docker build -t hither_test_deployed .

exec docker run \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v /tmp:/tmp \
    --net host \
    -it hither_test_deployed \
    /working/runtest.sh