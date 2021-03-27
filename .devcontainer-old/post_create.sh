#!/bin/bash

sudo usermod -aG docker vscode
newgrp docker
pip install -e .

cat <<EOT >> ~/.bashrc
alias gs="git status"
alias gpl="git pull"
alias gps="git push"
alias gpst="git push && git push --tags"
alias gc="git commit"
alias ga="git add -u"
EOT
