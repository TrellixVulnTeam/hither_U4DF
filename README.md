[![Build Status](https://travis-ci.org/flatironinstitute/hither.svg?branch=master)](https://travis-ci.org/flatironinstitute/hither)
[![codecov](https://codecov.io/gh/flatironinstitute/hither/branch/master/graph/badge.svg)](https://codecov.io/gh/flatironinstitute/hither)

[![PyPI version](https://badge.fury.io/py/hither.svg)](https://badge.fury.io/py/hither)
[![license](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![Python](https://img.shields.io/badge/python-%3E=3.6-blue.svg)

# hither

## What is it?

hither is a Python tool that makes it easier for you to focus on doing research consistently,
reproducibly, and objectively: in a word, *scientifically*.

It offers:

* An easy declarative interface to [containerized](./containerization.md)
execution of your computing tasks defined as Python functions
* Built-in support for [task-level data parallelism](./parallel-computing.md),
across multiple algorithms and pipelines
* Automatic [result caching](./doc/job-cache.md), so that lengthy computations only need to run once
* The ability to run scripts locally (*hither*) and have jobs execute on a [remote compute resource](./doc/remote-compute-resource.md) (*thither*).
* A unified and intuitive approach to [job pipelining](./doc/pipelines.md) and batch processing

Learn more:

* [How does hither compare with other tools?](./doc/overview.md)
* [Basic usage examples](#basic-usage)
    - [Containerization](#containerization)
    - [Job cache](#job-cache)
    - [Parallel computing](#parallel-computing)
    - [Pipelines](#pipelines)
    - [Using remote compute resources](#remote-compute-resources)
* [Installation](#installation)
* [Other frequently asked questions](./doc/faq.md)

Continue to scroll down to view some examples

## Installation

**Prerequisites**

* Linux (preferred) or macOS
* Python >= 3.6
* [Docker](https://docs.docker.com/engine/) and/or [Singularity](https://sylabs.io/singularity/) (optional)

**Note:** It is recommended that you use either a [conda environment](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) or a [virtualenv](https://virtualenv.pypa.io/en/latest/) when using the `pip` and `python` commands. This will prevent ambiguities and conflicts between different Python installations.

```bash
# Install from PyPI
pip install --upgrade hither
```

## Basic usage

### Containerization

Decorate your Python function to specify a Docker image from Docker Hub.

```python
# integrate_bessel.py

import hither as hi

@hi.function('integrate_bessel', '0.1.0', container='docker://jsoules/simplescipy:latest')
def integrate_bessel(v, a, b):
    # Definite integral of bessel function of first kind
    # of order v from a to b
    import scipy.integrate as integrate
    import scipy.special as special
    return integrate.quad(lambda x: special.jv(v, x), a, b)[0]
```

You can then run the function either inside or outside the container.

```python
import hither as hi

# Import the hither function from a .py file
from integrate_bessel import integrate_bessel

# call function directly
val1 = integrate_bessel(v=2.5, a=0, b=4.5)

# call using hither pipeline
job = integrate_bessel.run(v=2.5, a=0, b=4.5)
val2 = job.wait()

# run inside container
with hi.Config(container=True):
    job = integrate_bessel.run(v=2.5, a=0, b=4.5)
    val3 = job.wait()

print(val1, val2, val3)
```

It is also possible to select between using Docker or Singularity for running the containerization.

[See containerization documentation for more details.](./doc/containerization.md)


### Job cache

Hither will remember the outputs of jobs if a job cache is used:

```python
import hither as hi
from expensive_calculation import expensive_calculation

# Create a job cache that uses /tmp
# You can also use a different location
jc = hi.JobCache(use_tempdir=True)

with hi.Config(job_cache=jc):
    # subsequent runs will use the cache
    val = expensive_calculation.run(x=4).wait()
    print(f'result = {val}')
```

[See the job cache documentation for more details.](./doc/job-cache.md)

### Parallel computing

You can run jobs in parallel by using a parallel job handler:

```python
import hither as hi
from expensive_calculation import expensive_calculation

# Create a job handler than runs 4 jobs simultaneously
jh = hi.ParallelJobHandler(num_workers=4)

with hi.Config(job_handler=jh):
    # Run 4 jobs in parallel
    jobs = [
        expensive_calculation.run(x=x)
        for x in [3, 3.3, 3.6, 4]
    ]
    # Wait for all jobs to finish
    hi.wait()
    # Collect the results from the finished jobs
    results = [job.get_result() for job in jobs]
    print('results:', results)
```

It is also possible to achieve parallelization using SLURM or remote resources.

[See the parallel computing documentation for more details.](./doc/parallel-computing.md)

### Pipelines

Hither provides tools to generate pipelines of chained functions, so that the output of one processing step can be fed seamlessly as input to another, and to coordinate execution of jobs.

```python
import hither as hi
from expensive_calculation import expensive_calculation
from arraysum import arraysum

# Create a job handler than runs 4 jobs simultaneously
jh = hi.ParallelJobHandler(num_workers=4)

with hi.Config(job_handler=jh):
    # Run 4 jobs in parallel
    jobs = [
        expensive_calculation.run(x=x)
        for x in [3, 3.3, 3.6, 4]
    ]
    # we don't need to wait for these
    # jobs to finish. Just pass them in
    # to the next function
    sumjob = arraysum.run(x=jobs)
    # wait for the arraysum job to finish
    result = sumjob.wait()
    print('result:', result)
```

[See the pipelines documentation for more details.](./doc/pipelines.md)


## Remote compute resources

One of the most powerful capabilities of hither is to use a remote computer (or compute cluster) as a resource for running individual jobs. To achieve this, use a `hi.RemoteJobHandler()` job handler.

[See the remote compute resource documentation for more details.](./doc/remote-compute-resource.md)

[We also have instructions for setting up a remote compute resource on Linode](./doc/howto_compute_resource_on_linode.md)

## Reference

For a complete list of hither capabilities, see the [reference documentation](./doc/reference.md)

## For developers

[Opening the hither source code in a development environment](./doc/devel.md)

[Running diagnostic tests](./doc/tests.md)

## Authors

Jeremy Magland and Jeff Soules<br>
Center for Computational Mathematics<br>
Flatiron Institute, Simons Foundation
