# Hosting a hither compute resource server

Researchers often have acecss to several machines of varying computational
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

## Configuration

The first step is to configure your compute resource. Create a new directory on the computer where the compute resource server will run. For convenience, we'll assume that the environment variable HITHER_COMPUTE_RESOURCE_SERVER_DIR has been set to this directory on the resource. (Note that
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

This will guide you through some questions and create a `compute_resource.json` file with the configuration information. To edit the settings, you can either directly edit this .json file, or you can rerun the utility in the same directory.

If you want to host the three servers (kachery server, event stream server, and compute resource server) all on the same machine, then you can simply accept all of the defaults.

If you elect to host the kachery server on the same machine, then a `kachery-server` directory will also be created along with a `kachery-server/kachery.json` file which is editable.

Similarly, if you elect to host the event stream server on the same machine, then a `event-stream-server` directory will also be created along with a `event-stream-server/eventstreamserver.json` file which is editable.

## Running the server

After you answer all of the questions, the configuration utility will display commands that you can run to start the compute resource server. In the case that you are hosting all three servers on the same machine, you will be instructed to run the following three commands **in separate terminals**

```bash
# terminal 1
# only run this if you are hosting the kachery server on the same machine
hither-compute-resource start-kachery-server
```

```bash
# terminal 2
# only run this if you are hosting the event stream server on the same machine
hither-compute-resource start-event-stream-server
```

```bash
# terminal 3
hither-compute-resource start
```

Explain how to use tmux to keep these services running.

## Using the compute resource

You may now use this compute resource from any machine that is able to connect to the kachery server and the event stream server. Here is an example script that can run on the same machine as these servers:

```python
import os
import hither as hi
from integrate_bessel import integrate_bessel

# Adjust as needed
port = 15402

# Adjust as needed
compute_resource_id = os.environ['COMPUTE_RESOURCE_ID']

# Create the remote job handler
jh = hi.RemoteJobHandler(
    event_stream_client=hi.EventStreamClient(
        url=f'http://localhost:{port}',
        channel='readwrite',
        password='readwrite'
    ),
    compute_resource_id=compute_resource_id
)

# Configure hither to use this job handler
with hi.Config(job_handler=jh, container=True):
    x = integrate_bessel.run(v=2.5, a=0, b=4.5).wait()
    print(f'Result = {x}')
```

See:
* [examples/example_remote_job_handler.py](../examples/example_remote_job_handler.py)
* [examples/integrate_bessel.py](../examples/integrate_bessel.py)

