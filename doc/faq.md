# Hither frequently asked questions

* [Hither functions](#hither-functions)
* [Containerization](#containerization)
* [Parallelization](#parallelization-and-pipelining)
* [Memoization](#memoization)
* [Remote execution](#remote-execution)
* [Reproducibility](#reproducibility)
* [General](#general)

<!--- Hither functions --->

## Hither functions

### What is a hither function?

A hither function is a Python function that has been decorated using `@hi.function()`. The function should be a *pure function* in that it produces no side effects and the return value is uniquely determined from the input arguments. The input arguments and return value are expected to conform to [certain requirements](#what-are-the-allowed-types-for-input-arguments-and-return-values-for-hither-functions-why).

Some examples would include the following:
```python
    @hi.function('ones', '0.1.0')
    def ones(shape):
        return np.ones(shape=shape)

    @hi.function('add', '0.1.0')
    def add(x, y):
        return x + y
```

As these examples show, to be a hither function, at a minimum, the function must be decorated
with a *name* and a *version number*. The name can be anything the author likes, but must
match the actual name of the function defined in the code. So the following would **not** work:

```python
    # THIS IS INCORRECT AND WILL CAUSE AN ERROR
    @hi.function('MySpecialFunction', '0.1.0')
    def boring_function:
        return 6
```

Of course, most nontrivial functions also depend on other code. A hither function can call
code outside itself. Generally-available packages from a package repository should be available
on the environment where the hither function is run (most likely by including them in the
container image). There are also mechanisms for including any
[local dependencies](#how-does-hither-manage-dependencies-on-python-modules-or-other-code-outside-my-function).

### What are the allowed types for input arguments and return values for hither functions? Why?
A value `x` is a hither-allowed input value if any of the following are true:

* `x` is a jsonable item
* `x` is a numpy array
* `x` is a list of hither-allowed input values
* `x` is a tuple of hither-allowed input values
* `x` is a Python dict where the values are hither-allowed input values

The definition for hither-allowed output values is the same, except that, additionally, `hi.File()` objects are also allowed. Note that these values may be nested within list, tuple, or dict values.

Because the types allowed for the output of a hither function are the same as the types allowed for
input, during execution one hither function can also be used as input for another. This allows creation
of pipelines.
For example, using the functions defined above:
```python
    with hi.Config(job_handler=jh):
        a = ones.run(shape=(4, 3))
    
    b = add.run(x=a, y=a)
    # b now has a Numpy array representing a 4x3 matrix of '2' values
```
in which the `ones` function will be stored in `a`, and `a` can be used as input for both parameters of
the `add` function. In the example above, with proper configuration, the `ones` function will actually
only be executed once; because its output is uniquely determined from its inputs, its result value
can be cached.

Custom objects or data structures that cannot be serialized to JSON are __not__ presently supported,
although these may be supported in the future in cases where a serialization mechanism
is also provided.

### What are other requirements for a hither function?

While it is not strictly forbidden, it is strongly encouraged that hither functions
be [pure functions](https://en.wikipedia.org/wiki/Pure_function):
functions whose results are determined only by their inputs and do
not rely upon, or change, any external state. This is to support hither's
[caching mechanisms](#how-can-i-use-hither-to-cache-the-results-of-python-jobs),
as well as Job-level parallelism (since hither Jobs are
not guaranteed to execute in a particular order, code with side effects can have
consequences which are difficult to reason about).

Stochastic algorithms (whose results may vary from one execution to the next, even
on the same inputs) can be used, but the job cache, if used, will return
the same result every time the function's result is checked. If you would like
to see varying possible results returned, it is possible to
[disable caching](#is-the-job-cache-always-used).

### What is the difference between calling a hither function directly and using the `.run()` method?

The `.run()` method returns a `Job`, which we can wait on. For example:

```python
import hither as hi

@hi.function('get_the_answer', '0.1.0')
def get_the_answer():
    return 42

x = get_the_answer()
job = get_the_answer.run()
y = job.wait()

assert x is 42
assert y is 42
```

`Job`s can be run asynchronously and in parallel (so long as they don't depend on the results of other `Job`s).
Calling a hither function directly will result in synchronous execution. Additionally, the result of a direct call to a hither function will not be cached.

### How can I call a hither function by name?

Every hither function must be decorated with a name and a version (these parameters ensure that functions
are rerun when their definitions may have changed). Any named hither function can then be called with the
following syntax, by calling `.run(FUNCTION_NAME)` on the hither package directly:

```python
    import hither as hi

    x = hi.run('add', x=1, y=2).wait()
    assert x is 3
```
`hi.run(FUNCTION_NAME)` returns a `Job`, just like calling `function.run()` directly; to get the
result, you will need to call `.wait()` on that `Job`.

Note that a hither function's registered name must match the function's actual name in the code.

### What is the version of a hither function used for?

hither is designed to support scientific and data-analytic processing pipelines requiring potentially
very expensive computations. Part of this support is avoiding unnecessary re-runs with known values.
To do this, most hither configurations will maintain a *job cache* which records the results of a
function on a known set of arguments (an example of [memoization](https://en.wikipedia.org/wiki/Memoization)).

However, developing improved algorithms is part of research. hither uses the cached version of a
function output only if the version number matches. By incrementing the version number, a function's
author can indicate that it should be rerun where it appears in any existing pipelines.

### Does all my code have to be in one big file?

No! When a hither function is prepared for execution, hither packages the function along
with the contents of its local code environment in a dictionary structure. This structure
is used to reproduce the hither function's environment when it is run in a
[container](#containerization)
or on a remote resource. The process should be transparent, and there should be no
need to alter your code in order to make it work with hither.

To build the code environment, hither determines the location of the file in which
the hither function is defined. hither then builds a tree that stores the names,
locations, and contents of all python files
in that directory, or any of its subdirectories.
Files ending in `.py` are included, except for the top-level `__init__.py`
(if one exists)--a new one, immporting all modules encountered during this procedure,
will be created.

### What is the local_modules parameter in a hither function?

The `local_modules` parameter is used to tell hither about locally installed
python modules that live outside the hither function's directory. Any directories included
in this parameter will be included in the hither function's code environment, just like
the hither function's own directory.


### What is the additional_files parameter in a hither function?

The `additional_files` parameter is used to identify dependency files that 
don't end in `.py`. For example, your function might have required configuration
files that need to be included for the code to run correctly. Their names should
be added to this parameter.

The values are interpreted as a shell glob using the python
`fnmatch` module, so something like
'`*.config`' should work, in addition to an explicit list of file names.
The comparison is done on file name only; values for this parameter should not
include the path. Any files matching any of the patterns in this parameter
will be included.

The `additional_files` patterns are applied only to files in the hither function's
directory subtree; they are not applied to directories imported because of their
inclusion in `local_modules`.


<!--- Containerization --->

## Containerization

### How can I use hither to run Python code in a docker container?

[See the containerization docs](./containerization.md)

### What is a docker image? How is that different from a docker container?

A "container" is an instance of a virtual computing resource, while an "image" is the
fixed specification describing the initial state of that resource. You can think of the
"image" as the picture of a desk in a catalog, and the "container" as the actual desk
you personally are working on.

Note that [containers](https://en.wikipedia.org/wiki/OS-level_virtualization) are distinct
from true [virtual machines](https://en.wikipedia.org/wiki/Virtual_machine)
because containers (also known as 'operating-system level virtualization') are much lighter-weight.
While a virtual machine would run its own operating system on simulated hardware,
down to potentially specifying machine provisioning, this is not required for containers,
which access hardware and file systems through controlled calls to the host operating system.
Containers provide, for abritrary software, the kind of independence and reproducibility that
a conda environment would provide for Python.


### What is the relationship between docker and singularity?

[Docker](https://en.wikipedia.org/wiki/Docker_(software)) is both a company and
one of the best-known containerization tools. The company also maintains a library
of [container images](https://hub.docker.com/).

[Singularity](https://en.wikipedia.org/wiki/Singularity_(software)) is a containerization
tool that is specifically designed for high-performance computing clusters. Its design
 offers improved security over Docker's container implementation, and it also offers
 specific support for high-performance networking and communication standards
 (e.g. Infiniband) and libraries (e.g. OpenMPI) common in HPC environments.

### Are docker and singularity required in order to use hither?

While most interesting use cases for hither will depend on containers, they are not required
to run functions; a hither function declared with no container will be run on the host system
by default.

### Can I use local docker images with hither, or do they need to be stored on Docker Hub?

Container images do not need to be stored on Docker Hub specifically; so long as the
hither process can resolve the URL pointing to the container image, that image can be used.

### How does Hither manage dependencies on Python modules or other code outside my function?

When the hither function is packaged, a function is run to bring along all code from its
directory and all child directories. Specifically, any files ending in `.py` (except the top-level
`__init__.py`, if any) are added to the code environment of the function, as well as
any files whose names match the `additional_files` parameter (interpreted as a glob
using the python `fnmatch` module). These code environment files will be unpacked into
the container when the function is run.

If python modules from non-local sources are required, they should be built into the container
image.

### When using a remote compute resource, where are docker images downloaded to?

Docker images will be downloaded to the remote compute resource (which will be running
a copy of hither). Their exact location will depend on the user account which invokes
hither on that resource--container image download is achieved by executing a `docker pull`
command.

### How can I create a docker image with the appropriate dependencies for my hither function?

A simple container image definition such as the following shows
a solution for a dependency on `scipy`:

```docker
FROM python:3.8
RUN pip install scipy
```

Docker supports a rich array of features which are beyond the scope of this document.
[Docker's own documentation](https://docs.docker.com/get-started/) may be of further help.

[Dependencies on local packages](#does-all-my-code-have-to-be-in-one-big-file)
 are also supported, outside the container image.

<!--- Parallelization and pipelining --->

## Parallelization and pipelining

### How can I use hither to run jobs in parallel?

Here is an example that runs `8` jobs with `4` parallel workers
(see [parallel_example.py](./parallel_example.py))

```python
import hither as hi
import time

# Define a hither function that squares the input
# and simulates a long computation with a delay
@hi.function('sqr_with_delay', '0.1.0')
def sqr_with_delay(x, delay=None):
    if delay is not None:
        time.sleep(delay)
    return x * x

# Create a parallel job handler
job_handler = hi.ParallelJobHandler(num_workers=4)

# Time the overall execution
timer = time.time()

# Accumulate the results in an array
results = []

# Configure hither to use the parallel job handler
with hi.Config(job_handler=job_handler):
    for j in range(8):
        # Create the job and append output to results
        y = sqr_with_delay.run(x=j, delay=1)
        results.append(y)

    # Wait for all jobs to complete
    hi.wait()

# Print the results
print('Results:', [y.get_result() for y in results])
print(f'Elapsed time: {time.time() - timer} sec')
```

### What is a simple example of a hither pipeline?

Consider this example:
```python
import numpy as np

@hi.function('mult', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def mult(x, y):
    return x * y

@hi.function('invert', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def invert(x):
    # You should probably be more careful about types than this in real code
    if isinstance(x, np.ndarray):
        return np.linalg.inv(x)
    return (1/x)

@hi.function('dot', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def dot(x, y):
    return np.dot(x, y)

jh = hi.ParallelJobHandler(num_workers=2)
with hi.Config(job_handler=jh):
    # solve for x in xA = B, with extra computation to find x from its roots
    xroot = np.array([4, 5])
    a = np.array([[3, 4], [5, 6]])
    b = np.array([173, 214])
    
    ainv_job = invert.run(x=a)
    product_job = dot.run(x=b, y=ainv_job)
    x_job = mult.run(x=xroot, y=xroot)
    
    x = x_job.wait()
    product = product_job.wait()

    assert np.allclose(x, product)
```
Here we define three functions for standard mathematical operations, and
do some basic matrix algebra. The Job created by invoking `invert` on
the matrix A is fed as input to the Job which computes the dot product of
B and A<sup>-1</sup>. Meanwhile the value of `x` is computed by squaring
its roots.

Because a `ParallelJobHandler` is used, the independent operations of
finding `x` from `xroot` and solving for x with A and B can be done in parallel.
hither is aware that a pipeline has been formed in which the output of
`invert` is needed as input to the call to `dot`, so the `dot`
Job's execution will be delayed until that result is available.


### How can I use hither to submit jobs to a Slurm cluster?

__Use a SlurmJobHandler! (This deserves more thorough treatment)__

### What is the difference between a hither function and a hither job?

A hither *function* is a particular section of executable code which has been
decorated to make hither aware of it and describe its required runtime environment.
A hither *Job* is a (usually asynchronously-run) instance of that code's execution,
combined with its environment (parameters, job handlers, etc). In concrete terms:

```python
    @hi.function('add', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
    def add(x, y):
        return x + y

    # assume job handler jh has been set
    with hi.Config(job_handler=jh):
        sum = add.run(x=3, y=5)
        result = sum.wait()
```

In this example, `add(x, y)` is a hither function: it describes an operation and
a container satisfying its dependencies. `sum` is a Job:
it is an object that represents the `add` operation called on the specific
values 3 and 5, attached to `JobHandler` jh, potentially run in some
container environment, and so on.

Because of the asynchronous model, the results of `sum`'s computation are not
guaranteed to be complete until `.wait()` is called, at which time `sum` will
record that its result is 8 and return that value when interacted with.

Also, if the job cache has been configured, hither will ensure that any other
Job which depends on `sum` will receive its result value (8) directly, rather
than recomputing this value.


### How is hither different from dask?

[dask](https://dask.org/) is a tool which facilitates easy and seamless
parallelization of Python code, particularly for data science and machine learning
applications. It provides largely drop-in replacements for numpy and pandas data
structures that facilitate [tiled/blocked](https://en.wikipedia.org/wiki/Loop_nest_optimization)
approaches to computations, breaking
down large (potentially larger-than-memory) data sets into smaller units that can
be parallelized.
dask also facilitates pipeline use, in that it is aware of the operations performed
on matrix data and creates a graph of which operations depend on the results of
other operations, with considerable work put into scheduling independent operations
to run in parallel and ensuring that dependent operations' dependencies have been
completed before execution of the code which consumes their results.

hither (and its toolkit ecosystem), by contrast, is focused on reproducibility
through containerization and data file centralization. Happily, this provides
performance benefits by facilitating the use of 
more powerful computing resources and pipeline-level parallelism.
hither also improves pipeline performance through memoization.
Ultimately, though, hither operates in a completely algorithm-agnostic
manner: each hither function is treated as a black box, and no rewrites to existing
code should be needed to run that code within hither.

In short, dask is a highly regarded tool to parallelize a computation;
hither is used to run independent computations in parallel.

Of course, if you
*want* to write your code with hither in mind, you can certainly take advantage
of pipeline construction, Job-level parallelism, and memoization features to write
code that can potentially execute faster than the same computations performed
linearly. But hither is not intended to speed up specific computationally
intensive calculations the way dask is.

### What are the different job handlers that may be used with hither?

The following job handlers are currently defined:

* `DefaultJobHandler` -- entirely local and has very little interaction with Jobs
beyond starting them.
* `ParallelJobHandler` -- runs Jobs locally, either in or outside a container. Will run
multiple jobs at the same time in their own separate threads. Supports cancelling Jobs.
* `RemoteJobHandler` -- manages running Jobs on a remote compute resource, with or without
containerization. Supports cancelling Jobs.
* `SlurmJobHandler` -- runs Jobs in a cluster environment, with or without containerization.

### Can I use both local and remote job handlers within the same pipeline?

Sure. Every Job can be assigned its own job handler, determined
by the surrounding `with hi.Config()` environment block as in the code example
above.

<!--- Memoization --->

## Memoization

[Memoization](https://en.wikipedia.org/wiki/Memoization)
 is a computational technique in which previously computed values of
an expensive, but [pure](https://en.wikipedia.org/wiki/Pure_function),
function are recorded for later reuse.

### How can I use hither to cache the results of Python jobs?

[See the job cache documentation.](./job-cache.md)

## Is the job cache always used?

The job cache is only used if `job_cache` has been configured in the hither context and the `force_run` option has not been set to `True`.

TODO: figure out where the `force_run=True` can be set.

### What information does hither use to form the job hash for purposes of job caching?
Jobs in the job cache are identified by a hash value. This is the result of
applying the `sha1` hashing algorithm to an input string composed of the hither
function name and version, and its arguments.

Since one of hither's design goals is to promote scientific reproducibility,
it is expected that job caches may be quite long-lived (on the order of
weeks or months in some cases). If a new version of a hither function is
developed, it is important to update the version number. Changing the
version number ensures that the function's result is recomputed and stored
separately from the result of the old version.

<!--- Reproducibility --->

## Reproducibility

### What is kachery?

The hither ecosystem interacts with several other tools to ensure reproducibility
across environments.
[kachery](https://github.com/flatironinstitute/kachery) is a content-addressable
storage system for files. kachery provides a centralized and universal content
store for files and directories, such as those used in hither functions. kachery
provides several key features:

* Any file can be retrieved by a universal identifier based on its
`sha1` or `md5` hash, which operates like a [DOI](https://www.doi.org/)
for your data files--easy to store and uniquely associated with your input
* The kachery server can be run locally, on a container,
on a remote server, or in the cloud
* Consumers of the file data do not need
to make any assumptions about directory structure in order to locate the
files needed to rerun a pipeline
* The centralized server allows tools
to request file inputs from any environment

### What files are stored in the KACHERY_STORAGE_DIR directory?

kachery downloads needed files from the remote server to a cache on
the local filesystem. The location of this cache should be identified
by the KACHERY_STRORAGE_DIR environment variable.

In hither applications, this directory is used to store any sort of input
files required by processing pipelines, and the outputs of
job runs. Additionally, hither supports
operations on potentially very large mathematical structures, which
can include numpy arrays of several gigabytes in size. For performance
and data integrity reasons, any numpy array which is the input to
or output from a hither function will be serialized to disk and
stored in kachery if it would otherwise be shipped across a server
or Job boundary. __(QUERY: Is that part about job boundaries true?)__

<!--- Remote execution --->

## Remote execution

### How can I use hither to run Python code on a remote machine?

First, ensure that you are actually able to run Python code on
the remote machine--hither can't help you run on servers to which
you do not have access!

Next, [configure a named compute
resource](#how-can-i-host-my-own-hither-compute-resource-server).
Make sure to record the id associated with the remote compute
resource you would like to use.

Now, consider this example:
```python
import os
import numpy as np
import hither as hi

@hi.function('sumsqr', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def sumsqr(x):
    return np.sum(x**2)

def main():
    mongo_url = os.getenv('MONGO_URL', 'mongodb://localhost:27017')
    db = hi.Database(mongo_url=mongo_url, database='hither')
    # Remote compute resource triggered by the compute_resource_id parameter
    renmote_job_handler = hi.RemoteJobHandler(database=db, compute_resource_id='resource1')
    with hi.Config(job_handler=remote_job_handler, container=True):
        delay = 15
        val1 = sumsqr.run(x=np.array([1]))
        val2 = sumsqr.run(x=np.array([1,2]))
        val3 = sumsqr.run(x=np.array([1,2,3]))
        print(val1.wait(), val2.wait(), val3.wait())
        assert val1.wait() == 1
        assert val2.wait() == 5
        assert val3.wait() == 14
```

By setting the `compute_resource_id` parameter in the call to `hi.Config`, this code
specifies that the `RemoteJobHandler` should talk to a remote compute resource
named `resource1`. This communication is mediated by a job dispatch bus,
presently implemented in MongoDB. The `RemoteJobHandler` serializes any Job to
be run (the hither function, its arguments, etc) and passes this to the job resource
bus, along with the ID of the remote compute resource which is intended to handle
that Job. The remote compute resource manager running on the remote server
listens for its name and initiates execution of any Job assigned to it.

While it would be possible for a hither instance to run jobs against multiple
remote compute resources simultaneously, we do not presently support
load-balancing among multiple remote compute resources from within a single
configuration context.

__IT WOULD BE NONTRIVIAL BUT MAYBE WE SHOULD LOOK INTO THIS__

### How can I host my own hither compute resource server?

__TODO: This may be changing as we migrate away from the
strong MongoDB depdencny__

You will need to run something like the following on the remote resource:
```python
#!/usr/bin/env python

import os
import hither as hi

def main():
    mongo_url = os.getenv('MONGO_URL', 'mongodb://localhost:27017')
    db = hi.Database(mongo_url=mongo_url, database='hither')
    jh = hi.ParallelJobHandler(num_workers=8)
    jc = hi.JobCache(database=db)
    CR = hi.ComputeResource(database=db, compute_resource_id='resource1', job_handler=jh, kachery='default_readwrite', job_cache=jc)
    CR.clear()
    CR.run()

if __name__ == '__main__':
    main()
```

This code creates an instance of `ComputeResource` (the hither class that
manages activity on a remote resource) and initiates its main loop, so
that it is waiting for jobs.

Communication between your local hither functions (dispatched by the
`RemoteJobHandler`) and the `ComputeResource` is handled by a communication
bus, currently implemented in MongoDB. The `ComputeResource` polls the
job dispatch database at regular intervals to retrieve any Jobs which have
been assigned to it, and then launches these on its own local hither job handler
(in the above example, it is using a `ParallelJobHandler` set to 8 worker threads).

### Which job handlers can be used by a compute resource server?

A (remote) compute resource server can use the RemoteJobHandler or the
SlurmJobHandler (if it is a cluster which makes use of Slurm). The
ParallelJobHandler runs jobs in parallel on the local compute resource.

### When using a remote job handler, do results get cached locally or remotely?

Results of Job runs will be cached to the configured job cache. As with the other
components of the hither ecosystem, this is loosely coupled and can be run anywhere.

### How does hither decide when to upload/download files when using a remote compute resource?

When possible, hither tries to avoid sending large files over the network unnecessarily.
hither will push files to kachery under the following circumstances:

* If the function is part of a pipeline (the Job appears as a parameter for another
hither Job), and the two Jobs do not have access to the same file system
* If the configuration `hi.Config(download_results=True)` is set

In other cases, hither will avoid transmitting files.

__TRIED TO VERIFY THIS IN THE CODE BUT I GOT LOST AND WAS CHASING MY TAIL FOR
A WHILE. IS THIS CORRECT?__

<!--- General --->

## General

### How does hither handle large numpy arrays?

Large numpy arrays are serialized to the filesystem and stored in
kachery when they would otherwise need to be shipped over the network. 

### What is a hither `File` object?

A `File` object in hither represents a regular file that has been stored in the
kachery data store. Think of it as "anything kachery tracks."

### What is a hither `Job` object?

A hither `Job` is an instance of a hither function with specified arguments. It can
be executed asynchronously and, with the correct job handler setup, several Jobs
can be run concurrently, even across different compute resource environments.

### How can I retrieve the console output for a hither job that has already run?

__Don't know:__ Is this asking as a user, how can I do those things? Like
my code is running and I'm sitting at a console? Or programmatically how can
I access the Job._runtime_info field?

### How can I monitor the status of a running hither job?

__Don't know__

### How can I retrieve runtime information for a hither job that has already run?

__Don't know__
