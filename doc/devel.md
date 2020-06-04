# Hither for developers

Here we describe how to open the hither source code in a containerized development environment within Visual Studio Code.

## Prerequisites

* Linux
* [Visual Studio Code](https://code.visualstudio.com/) with the [Remote-Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension
* Docker

## Create a kachery storage directory

Create a directory where (potentially large) temporary data files will be stored. For example:

```bash
mkdir /home/user/kachery-storage
```

and set a corresponding environment variable in your `.bashrc`

```bash
export KACHERY_STORAGE_DIR="..."
```

Note that when your `.bashrc` file changes, you may need to logout and log back in again prior to opening Visual Studio Code.

## Open the project in a development container

Clone the source and open in Visual Studio Code:

```
git clone [this-repo-url]
cd hither
code .
```

Use the green button in the lower-left corner of the Visual Studio Code window and click: "Remote-Containers: Reopen-in-container".

This will build the development container and reopen the project in that container.

Note: if the process hangs on the final step of launching the container, you may need to close Visual Studio Code and try again.

## Unit tests

The first thing to try would be the [unit tests]('./unit_tests.md).