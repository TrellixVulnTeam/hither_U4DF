[![PyPI version](https://badge.fury.io/py/hither.svg)](https://badge.fury.io/py/hither)
[![license](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

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
* A unified and intuitive approach to [job pipelining](./doc/pipelines.md) and batch processing

Learn more:

* [How does hither compare with other tools?](./doc/overview.md)
* [Basic usage examples](#basic-usage)
    - [Containerization](#containerization)
    - [Job cache](#job-cache)
    - [Parallel computing](#parallel-computing)
    - [Pipelines](#pipelines)
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

# integrate_bessel.py

import hither2 as hi

@hi.function(
    'integrate_bessel',
    '0.1.0',
    image=hi.RemoteDockerImage('docker://jsoules/simplescipy:latest'),
    modules=['simplejson']
)
def integrate_bessel(v, a, b):
    # Definite integral of bessel function of first kind
    # of order v from a to b
    import scipy.integrate as integrate
    import scipy.special as special
    return integrate.quad(lambda x: special.jv(v, x), a, b)[0]
```

You can then run the function either inside or outside the container.

```python
# example integrate_bessel.py

import hither2 as hi
from integrate_bessel import integrate_bessel

# call function directly
val1 = integrate_bessel(v=2.5, a=0, b=4.5)

# call using hither pipeline
job: hi.Job = integrate_bessel.run(v=2.5, a=0, b=4.5)
val2 = job.wait().return_value

# run inside container
with hi.Config(use_container=True):
    job: hi.Job = integrate_bessel.run(v=2.5, a=0, b=4.5)
    val3 = job.wait().return_value

print(val1, val2, val3)

```

It is also possible to select between using Docker or Singularity for running the containerization.

[See containerization documentation for more details.](./doc/containerization.md)


### Job cache

Hither will remember the outputs of jobs if a job cache is used:

```python
# example_job_cache.py

import hither2 as hi
from expensive_calculation import expensive_calculation

# Create a job cache
jc = hi.JobCache(feed_name='example')

with hi.Config(job_cache=jc):
    # subsequent runs will use the cache
    job: hi.Job = expensive_calculation.run(x=4)
    print(f'result = {job.wait().return_value}')
```

[See the job cache documentation for more details.](./doc/job-cache.md)

### Parallel computing

You can run jobs in parallel by using a parallel job handler:

```python
# example_job_handler.py

from typing import List
import hither2 as hi
from expensive_calculation import expensive_calculation

# Create a job handler than runs 4 jobs simultaneously
jh = hi.ParallelJobHandler(num_workers=4)

with hi.Config(job_handler=jh):
    # Run 4 jobs in parallel
    jobs: List[hi.Job] = [
        expensive_calculation.run(x=x)
        for x in [3, 3.3, 3.6, 4]
    ]
    # Wait for all jobs to finish
    hi.wait()
    # Collect the results from the finished jobs
    results = [job.result.return_value for job in jobs]
    print('results:', results)
```

It is also possible to achieve parallelization using SLURM or remote resources.

[See the parallel computing documentation for more details.](./doc/parallel-computing.md)

### Pipelines

Hither provides tools to generate pipelines of chained functions, so that the output of one processing step can be fed seamlessly as input to another, and to coordinate execution of jobs.

```python
# example_pipeline.py

from typing import List
import hither2 as hi
from expensive_calculation import expensive_calculation
from arraysum import arraysum

# Create a job handler than runs 4 jobs simultaneously
jh = hi.ParallelJobHandler(num_workers=4)

with hi.Config(job_handler=jh):
    # Run 4 jobs in parallel
    jobs: List[hi.Job] = [
        expensive_calculation.run(x=x)
        for x in [3, 3.3, 3.6, 4]
    ]
    # we don't need to wait for these
    # jobs to finish. Just pass them in
    # to the next function
    sumjob: hi.Job = arraysum.run(x=jobs)
    # wait for the arraysum job to finish
    result = sumjob.wait()
    print('result:', result.return_value)
```

[See the pipelines documentation for more details.](./doc/pipelines.md)

## Reference

For a complete list of hither capabilities, see the [reference documentation](./doc/reference.md)

## For developers

[Opening the hither source code in a development environment](./doc/devel.md)

[Running diagnostic tests](./doc/tests.md)

## Authors

Jeremy Magland and Jeff Soules<br>
Center for Computational Mathematics<br>
Flatiron Institute, Simons Foundation
