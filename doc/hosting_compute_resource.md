# Hosting a hither compute resource server

Explain why you would want to host a compute resource.

Describe the architecture with a diagram... kachery-server, event-stream-server, compute-resource-server, compute-resource client.

Explain that these services can all be running in different locations. Or, for simplicity, they can all be running on the same machine.

## Prerequisites

* hither
* Docker

## Configuration

The first step is to configure your compute resource. Create a new directory on the computer where the compute resource server will run. For convenience, we'll assume that the environment variable HITHER_COMPUTE_RESOURCE_SERVER_DIR has been set.

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

