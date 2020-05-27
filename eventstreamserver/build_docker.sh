#!/bin/bash

set -ex

for i in "$@" ; do
    if [[ $i == "--push" ]] ; then
        PUSH="true"
    fi
done

IMAGE_NAME="magland/eventstreamserver:0.1.1"

docker build -t $IMAGE_NAME .

if [ "$PUSH" = "true" ]; then
    docker push $IMAGE_NAME
fi