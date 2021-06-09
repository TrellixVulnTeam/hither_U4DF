# Hither for developers

Here we describe how to open the hither source code in a containerized development environment within Visual Studio Code.

## Prerequisites

* Linux
* [Visual Studio Code](https://code.visualstudio.com/) with the [Remote-Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension
* Docker

## Create a kachery storage directory

Create a directory where (potentially large) temporary data files will be stored. For example (replace `<user>` by your user name):

```bash
mkdir /home/<user>/kachery-storage
```

and set a corresponding environment variable in your `.bashrc`

```bash
export KACHERY_STORAGE_DIR="..."
```

Note that when your `.bashrc` file changes in this way, you may need to restart VS Code and/or log out and log back in again.

## Open the project in a development container

Clone the source and open in Visual Studio Code:

```bash
git clone [this-repo-url]
cd hither
code .
```

Use the green button in the lower-left corner of the Visual Studio Code window and click: "Remote-Containers: Reopen-in-container".

This will build the development container and reopen the project in that container.

Note: if the process hangs on the final step of launching the container, you may need to close Visual Studio Code and try again.

## Diagnostic tests

The first thing to try would be the [unit and integration tests]('./tests.md).

## Versioning and deploying to PyPI

Proposal

Use the following pattern for versioning

* `x.x.x-alpha.x` - don't deploy these to PyPI, except for the unique case of `0.2.0-alpha.x`
* `x.x.x-beta.x` - don't deploy these either, except for `0.2.0-beta.x`
* `x.x.x` - deploy these

To release a new version, use the following flow (subject to change):

* Make sure master branch is what you want to release
* Change to release branch `git checkout release`
* Merge the changes from master `git merge master`
* Increment the version string in `hither/__init__.py` and `devel/test_deployed/Dockerfile` and commit the changes
* Tag the commit with the version string: e.g., `git tag 0.2.0`
* Push to github (including tags): `git push && git push --tags`
* Switch back to master branch: `git checkout master`
* Merge the changes from release `git merge release`
* Push master `git push`

These steps should trigger travis to deploy to PyPI after unit tests have all passed