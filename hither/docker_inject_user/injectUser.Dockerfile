# Inject a user with the same uid/gid as the current host user
#
# Example Usage:
#
# The below script will create a new docker image called image-name-nonroot
# that is the same as the image-name image, except with a new non-root
# user named labbox. The container will then run as the non-root user.
#
# USER_ID="$(id -u)"
# GROUP_ID="$(id -g)"
# USER_INSIDE_CONTAINER="labbox"
# BASE_IMAGE="image-name" # docker image name
# NEW_IMAGE="image-name-nonroot"
# INJECT_USER_DIR="path to directory of this injectUser.Dockerfile file"
# docker build \
#     -f ${INJECT_USER_DIR}/injectUser.Dockerfile \
#     -t ${NEW_IMAGE} \
#     --build-arg BASE_IMAGE=${BASE_IMAGE} \
#     --build-arg NEW_USER=${USER_INSIDE_CONTAINER} \
#     --build-arg NEW_UID=${USER_ID} \
#     --build-arg NEW_GID=${GROUP_ID} \
#     ${UPDATE_UID_DIR}
#
#
ARG BASE_IMAGE
FROM $BASE_IMAGE

USER root

#########################################
## Create the non-root user

ARG NEW_USER
ARG NEW_UID
ARG NEW_GID
RUN groupadd --gid ${NEW_GID} ${NEW_USER} \
    && useradd -s /bin/bash --uid ${NEW_UID} --gid ${NEW_GID} -m ${NEW_USER} \
    # Add sudo support
    && apt-get update && apt-get install -y sudo \
    && echo ${NEW_USER} ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/${NEW_USER} \
    && chmod 0440 /etc/sudoers.d/${NEW_USER} \
    && groupadd docker && usermod -aG docker ${NEW_USER} && newgrp docker

USER ${NEW_USER}