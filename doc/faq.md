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

A hither function is a Python function that has been decorated using `@hi.function()`. The function should be a *pure function* in that it produces no side effects and the return value is uniquely determined from the input arguments. The input arguments and return value are expected to conform to certain requirements.

A value `x` is a hither-allowed input value if any of the following are true:

* `x` is a jsonable item
* `x` is a numpy array
* `x` is a list of hither-allowed input values
* `x` is a tuple of hither-allowed input values
* `x` is a Python dict where the values are hither-allowed input values

The definition for hither-allowed output values is the same, except that, additionally, `hi.File()` objects are also allowed. Note that these values may be nested within list, tuple, or dict values.

TODO: describe requirements for code dependencies in hither functions.

TODO: provide some examples of basic hither functions.

### What are the allowed types for input arguments and return values for hither functions? Why?

### What are other requirements for a hither function?

### What is the difference between calling a hither function directly and using the `.run()` method?

The `.run()` method returns a job, which we can wait on. For example:

```python
import hither as hi

@hi.function('foo', '0.1.0')
def foo():
    return 42

x = foo()
job = foo.run()
y = job.wait()

assert x is 42
assert y is 42
```

### How can I call a hither function by name?

Once a function is decorated

### What is the version of a hither function used for?

### What is the local_modules parameter in a hither function?

### What is the additional_files parameter in a hither function?


<!--- Containerization --->

## Containerization

### How can I use hither to run Python code in a docker container?

Ensure that Docker is installed in the target system and accessible to the user who will be
running hither.

Then, when declaring a hither function, simply decorate it with a notation like the following:
```python
@hi.function('my_function', '0.1.0')
@hi.container('docker://image_url')
def my_function():
    # do your computations
```
`my_function` will then be wrapped as a hither function. When the job is run by calling the
`.run()` method on the function, the job will be run in the container, provided that the
active configuration includes `container=True`.

(Under the hood, whatever value is given to the `container` decorator will be passed on
to `docker pull` and subsequently to `docker run`.)

### What is a docker image? How is that different from a docker container?

A "container" is an instance of a virtual computing resource, while an "image" is the
fixed specification describing the initial state of that resource. You can think of the
"image" as the picture of a desk in a catalog, and the "container" as the actual desk
you personally are working on.

Note that containers are distinct from true virtual machines [EXPLAIN--or is this too much?]

### What is the relationship between docker and singularity?

### Are docker and singularity required in order to use hither?

While most interesting use cases for hither will depend on containers, they are not required
to run functions; a hither function declared with no container will be run on the host system
by default.

### Can I use local docker images with hither, or do they need to be stored on Docker Hub?

### How does Hither manage dependencies on Python modules or other code outside my function?

### When using a remote compute resource, where are docker images downloaded to?

### How can I create a docker image with the appropriate dependencies for my hither function?

<!--- Parallelization and pipelining --->

## Parallelization and pipelining

### How can I use hither to run jobs in parallel?


Here is an example that runs `8` jobs with `4` parallel workers (see [parallel_example.py](./parallel_example.py))

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

### How can I use hither to submit jobs to a Slurm cluster?

### What is the difference between a hither function and a hither job?

### How is hither different from dask?

Go through some of the basic dask examples and show how the hither approach compares. What I am guessing: in some cases hither is simpler to use, in other cases, dask is better (more flexible), and there are some things that hither can do that dask cannot.

I think it may be possible to use dask as a job handler (DaskJobHandler), but I'm not certain.

### What are the different job handlers that may be used with hither?

### Can I use both local and remote job handlers within the same pipeline?

<!--- Memoization --->

## Memoization

### How can I use hither to cache the results of Python jobs?

### What information does hither use to form the job hash for purposes of job caching?

<!--- Reproducibility --->

## Reproducibility

### What is kachery?

### What files are stored in the KACHERY_STORAGE_DIR directory?

<!--- Remote execution --->

## Remote execution

### How can I use hither to run Python code on a remote machine?

### How can I host my own hither compute resource server?

### Which job handlers can be used by a compute resource server?

### When using a remote job handler, do results get cached locally or remotely?

### How does hither decide when to upload/download files when using a remote compute resource?



<!--- General --->

## General

### How does hither handle large numpy arrays?

### What is a hither `File` object?

### What is a hither `Job` object?

### How can I retrieve the console output for a hither job that has already run?

### How can I monitor the status of a running hither job?

### How can I retrieve runtime information for a hither job that has already run?
