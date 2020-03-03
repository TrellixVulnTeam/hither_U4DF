#!/bin/bash

sudo usermod -aG docker vscode
newgrp docker
pip install -e .
