[![Build Status](https://travis-ci.org/flatironinstitute/hither.svg?branch=master)](https://travis-ci.org/flatironinstitute/hither)
[![codecov](https://codecov.io/gh/flatironinstitute/hither/branch/master/graph/badge.svg)](https://codecov.io/gh/flatironinstitute/hither)

[![PyPI version](https://badge.fury.io/py/hither.svg)](https://badge.fury.io/py/hither)
[![license](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![Python](https://img.shields.io/badge/python-%3E=3.6-blue.svg)

# hither

Hither is a flexible, platform-agnostic job manager that allows researchers to easily deploy code across local, remote, and cluster environments with minimal changes. It ensures that different scientific tools can be run through a consistent pipeline, even when they have different configurations or conflicting dependencies. This works through universal containerization. Because jobs are run through a universal interface, code becomes much more portable between labs; the same pipelines can be run locally, or even on a cluster environment. In this way, development on small datasets can take place on your laptop, with the confidence that the code will work on large datasets in the cluster with minimal modifications.

TODO: Need to describe other tools, how hither differs, and why it is needed.

[Frequently asked questions](doc/faq.md)

## Installation

**Prequisites**

* Python >= 3.6
* Docker (optional)
* Singularity (optional)

```bash
# Install from PyPI
pip install --upgrade hither
```

## Basic usage

### Containerization

Decorate your Python function to specify a docker image from DockerHub.

```python
# integrate_bessel.py

import hither as hi

@hi.function('integrate_bessel', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:dc57157d6316')
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

[See containerization documentation for more details.](./doc/containerization.md)


### Job cache

Hither will remember the outputs of jobs if a job cache is used:

```
import hither as hi
from expensive_calculation import expensive_calculation

# Create a job cache that uses /tmp
# You can also use a different location
# or a mongo database
jc = hi.JobCache(use_tempdir=True)

with hi.Config(job_cache=jc):
    # subsequent runs will use the cache
    val = expensive_calculation.run(x=4).wait()
    print(f'result = {val}')
```

[See job cache documentation for more details.](./doc/job-cache.md)

### Pipelines

Hither also provides tools to generate pipelines of chained functions, so that the output of one processing step can be fed seamlessly as input to another, and to coordinate execution of jobs.

TODO: provide a basic example

### Parallel computing

TODO: provide a basic example

### Batch processing

TODO: Give another full example of looping through a list of arguments, accumulating a list of job results, and then aggregating the outputs after processing completes.

## Reference documentation

[Reference documentation](doc/reference.md)

## Authors

Jeremy Magland and Jeff Soules<br>
Center for Computational Mathematics<br>
Flatiron Institute, Simons Foundation
