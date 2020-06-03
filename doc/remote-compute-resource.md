# Remote Compute Resources

## Definition

A "remote compute resource" is a **compute resource** (physical or virtual
machine) which is configured to receive and run hither Jobs dispatched over
the network from another location. Typically this will be a machine with
more computational power than the researcher's own development machine,
such as a central workstation vs. a laptop.

Note that a different remote setup should be used for cluster
environments using Slurm; see [TODO SLURM DOCUMENTATION]().

## Benefits

Setting up a remote compute resource facilitiates the use of more powerful
(usually shared) resources to run hither Jobs. It also allows multiple
researchers with multiple machines to run hither jobs against
a single compute resource, centralizing the acquisition and maintenance
of higher-capacity compute resources.

It can also be useful for computational results to be available
on demand to a resource whose primary responsibility is
something like serving web pages. There's no reason to provision a web
server to do high-powered computations for a monthly or weekly update of results, but
it would also be burdensome to attempt those computations on an underpowered
machine, or to rqeuire scaling a virtual resource up and down in order to
handle occasional processing jobs.


## Architecture

Describe the architecture with a diagram... kachery-server, event-stream-server, compute-resource-server, remote job handler.

As the above diagram shows, a distributed hither setup suggests four or five main components. In the simplest
setup (such as for testing) these could all be running on the same machine, though in real
deployments, the remote job handler (at a minimum) should be separate from the others--otherwise
a simpler `ParallelJobHandler` could be used. Alternatively, each component
can be run separately, on hardware particularly suited to its needs.

- **Remote Job Handler**

This is usually running on what you'd think of as 'your' machine or a 'local' machine, like
a researcher's individual laptop; or a lightweight resource that needs to coordinate hither
Jobs and interact with their results, like a web server with a scheduled job that updates
the results of several functions on a regularly updated data set.

- **kachery server**

The [kachery server](./faq.md#what-is-kachery) provides a central repository for archiving and
distributing data files and processing results. Deployment can be as simple as the local
file system of a remote compute resource, or as sophisticated as a dedicated resource with
high-throughput drives and an individualized backup plan.

- **event-stream server**

The event stream server [LINK?] coordinates communication among the different compute resources
handling hither Jobs. This server stores serialized Jobs and dispatches them to their scheduled
remote compute resources, as well as informing the remote job handler when results are available.
A server operating in this role is unlikely to need substantial processing power, but the
underlying filesystem will probably see substantial file turnover.

- **compute resource server**

This is the server (or servers) which will actually execute hither Jobs. Its provisioning needs
will be determined by the requirements of the underlying Jobs being run on it.

- **job cache** (optional)

The [job cache improves efficiency](./job-cache.md) by recording the results of Jobs
which have already been executed, and returning the recorded result instead of re-running
Job whose outcome has already been determined.

Use of a job cache is optional--if it is omitted, a Job will be rerun every time it
is requested--but if it is used, for the sake of clarity, it is highly advisable to
ensure that all configured compute resource servers point to the same job cache,
unless there is a compelling reason to do otherwise.



## Setup

The recommended way to set up a remote compute resource is
to use the `hither-compute-resource` tool distributed with this
package. See [full instructions](./hosting_compute_resource.md) here.

The only requirements are that the remote resource have stable network access
and that the user or service account running the compute resource manager
have rights to run python and Docker on the resource.

Under the hood, this tool reads information from a configuration file and
executes a more sophisticated version of the following:
```python
#!/usr/bin/env python

import os
import hither as hi

def main():
    event_stream_client = hi.EventStreamClient(url=eventstreamserver_url, channel=eventstreamserver_channel,
        password=eventstreamserver_password)
    jh = hi.ParallelJobHandler(num_workers=8)
    jc = hi.JobCache(path=job_cache_dir)
    cr = hi.ComputeResource(database=db, compute_resource_id='resource1', job_handler=jh,
        kachery='default_readwrite', job_cache=jc)
    cr.run()

if __name__ == '__main__':
    main()
```

This code creates an instance of `ComputeResource` (the hither class that
manages activity on a remote resource) and initiates its main loop, so
that it is waiting for jobs.

Communication between your local hither functions (dispatched by the
`RemoteJobHandler`) and the `ComputeResource` is handled by a communication
bus, the `EventStreamServer`. The `ComputeResource` polls the
job dispatch channel at regular intervals to retrieve any Jobs which have
been assigned to it, and then launches these on its own local hither job handler
(in the above example, it is using a `ParallelJobHandler` set to 8 worker threads;
a `ParallelJobHandler` is used so that the Jobs will be run local to the
`ComputeResource` managing their execution).


## What runs on the Remote Compute Resource?

The remote compute resource will run at least the following:
- An instance of the compute resource server, which communicates with the
Event Stream Server to coordinate Job dispatch and return results;
- A `ParallelJobHandler` (usually) which actually manages execution
of individual Jobs on the comute resource; and
- A container execution environment (Docker or Singularity).

It is expected that the container images supplied with the hither functions
will contain a python interpreter or whatever other environment is needed
to run the Jobs themselves, and the compute resource will need access
to a kachery server and event stream server, but these do not need to run
locally on the compute resource.


## Remote Compute Resources and Job Cache

Any job cache configured for the compute resource must be set up
in the `compute_resource.json` configuration file on the compute
resource itself. The local machine--the one running the
`RemoteJobHandler`--does not directly configure the remote compute
resource.

__TODO: IS THAT CORRECT? IT LOOKS LIKE THE SYSTEM RIGHT NOW JUST
READS A DIRECTORY--ARE WE EVEN SUPPORTING A REMOTE CACHE ATM?__

Note that for clarity, it is highly recommended that if multiple
compute resources (local or remote) are to be used, they all
point to the same job cache.


