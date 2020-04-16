# Hither2 reference documentation

Talk about security.

### How to define and run a hither2 function

A hither function is just a regular Python function with decorators. For example:

```python
import hither2 as hi

@hi.function('sumsqr', '0.1.0')
def sumsqr(x):
    return np.sum(x**2)
```

Explain this code snippet

Explain that we can either call the function directly, or by using the .run() function

Need to reword this. Explain that arguments can be of type str, number, or any JSON-serializable list or dict, plus numpy arrays. Also numpy arrays can be embedded in lists or dicts.

Cannot pass python objects that are not of this type. The reason is that the arguments need to be 
serialized in order to pass them to containers and
to remote compute resources. They also need to be hashed.

There's a lot to explain about dependencies, how code gets injected into containers.

Explain. Important to import modules inside functions rather than at the top (normal). Because
the imports may not be available until the code is
running inside the container. But we still need
to import the function on the host.

### How to run a function in a container

Explain

### How to use a job cache

Explain

### How to run a pipeline


### How to run jobs in parallel

(What about dask? How is hither2 different, why needed?)

For example, parallel job handler -- run multiple jobs in parallel

### How to use a remote compute resource

### How to run a hither2 compute resource server

### Job handlers

Job handlers determine when, where, and how hither jobs are run. There are three built-in job handlers:

* DefaultJobHandler
    - Runs jobs synchronously on the local machine
* ParallelJobHandler
    - Runs jobs in parallel on the local machine
* RemoteJobHandler
    - Sends jobs in parallel to a remote compute resource
* SlurmJobHandler
    - Sends job in parallel to a SLURM compute cluster

### Local modules

Show how to specify that the function depends on a local module.