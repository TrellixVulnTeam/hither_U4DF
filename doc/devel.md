## Installation for developers

Here we describe how to open the hither2 project in a containerized development environment within vscode.

## Prerequisites

* Install vscode
* Install docker

## Create a kachery storage directory

Create a directory where (potentially large) temporary data files will be stored. For example:

```bash
mkdir /data/kachery-storage
```

TODO: explain about the kachery storage directory.

## Environment variables

Set the following environment variables in your `~/.bashrc` or `~/.bash_profile`. Note that if any of these change, you may need to logout and log back in again [more details needed].

```bash
# Replace /data/kachery-storage by the appropriate path on your system
export KACHERY_STORAGE_DIR=/data/kachery-storage
```

## Open the project in a development container

Install the vscode "Remote-Containers" extension if not already installed.

Use the green button in the lower-left corner of the vscode window and click: "Remote-Containers: Reopen-in-container".

