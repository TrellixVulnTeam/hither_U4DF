#!/bin/bash

sudo usermod -aG docker vscode
newgrp docker
pip install -e .

# MONGO_URL should be set in .devcontainer/devcontainer.env to be
# mongodb://172.17.0.1:27017 where 172.17.0.1 is replaced by the
# ip address of the host as determined by the `ip a` command if on linux
# or host.docker.internal if on a mac
# See: https://nickjanetakis.com/blog/docker-tip-65-get-your-docker-hosts-ip-address-from-in-a-container
echo MONGO_URL=$MONGO_URL