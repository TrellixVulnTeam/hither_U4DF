# Hosting a hither compute resource server

Researchers often have access to several machines of varying computational
capabilities. It is not uncommon for a researcher to have primary access to
a personal laptop with modest hardware, while also having rights to
log in to a shared resource with greater power.

In these circumstances, a remote compute resource is ideal. [This documentation
provides more information on the architecture and rationale for remote
compute resources.](./remote-compute-resource.md)

Note that the instructions below are for a standalone remote compute resource;
for a compute cluster with job management through a tool such as Slurm,
see [TODO SLURM DOCUMENTATION]().

## Prerequisites

* hither
* Docker
* NodeJS >=12
* kachery-daemon

## Configuration

First, you must be running a kachery daemon on the machine where the compute resource will run.

Then, create a new directory on the computer where the compute resource server will run. For
convenience, we'll assume that the environment variable HITHER_COMPUTE_RESOURCE_SERVER_DIR has
been set to this directory on the resource. (Note that
all commands below should be executed on the intended remote compute resource. Furthermore,
for persistent setups or more-than-incidental use, it is recommended that the compute resource
server be run by a service account with separate credentials from any individual user.)

```bash
# make new directory if needed
mkdir $HITHER_COMPUTE_RESOURCE_SERVER_DIR

# change to the compute resource directory
cd $HITHER_COMPUTE_RESOURCE_SERVER_DIR

# run the configuration utility
hither-compute-resource config
```

This will guide you through some questions and create a `compute_resource.json` file with the configuration
information. To edit the settings, you can either directly edit this .json file, or you
can rerun the utility in the same directory.

## Running the server

After you answer all of the questions, you can start the compute resource
by running:

```bash
# terminal 3
hither-compute-resource start
```

Make a note of the compute resource URI for use in your scripts on remote machines.

Keep this running in a terminal or a tmux session.

## Using the compute resource

You may now use this compute resource from any machine that has a kachery-p2p daemon running with
at least one channel in common with the compute resource. Here is an example script:

```python
import os
import hither2 as hi
from .integrate_bessel import integrate_bessel

# Adjust as needed
compute_resource_uri = os.environ['COMPUTE_RESOURCE_URI']

# Configure hither to use this job handler
with hi.RemoteJobHandler(uri=compute_resource_id) as jh:
    with hi.Config(job_handler=jh, container=True):
        x = integrate_bessel.run(v=2.5, a=0, b=4.5).wait()
        print(f'Result = {x}')
```

See:

* [examples/example_remote_job_handler.py](../examples/example_remote_job_handler.py)
