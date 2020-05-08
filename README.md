[![Build Status](https://travis-ci.org/flatironinstitute/hither.svg?branch=master)](https://travis-ci.org/flatironinstitute/hither)
[![codecov](https://codecov.io/gh/flatironinstitute/hither/branch/master/graph/badge.svg)](https://codecov.io/gh/flatironinstitute/hither)

[![PyPI version](https://badge.fury.io/py/hither2.svg)](https://badge.fury.io/py/hither2)
[![license](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![Python](https://img.shields.io/badge/python-%3E=3.6-blue.svg)

# hither2

Run Python functions and pipelines in containers and on remote servers.

## Overview

Needs to describe other tools, how hither2 differs, and why it is needed.

[Frequently asked questions](doc/faq.md)

## A first example

Add an interesting example. Should be something that computes something useful and illustrates the various functionalities.

```python
# Explain that this can be replaced by other job handlers, including sending jobs to a remote compute resource
job_handler = hi.ParallelJobHandler()

# Make sure the function uses a container
# e.g., docker://jupyter/scipy-notebook:678ada768ab1

# point out that it is not necessary to install scipy locally if docker is present

# Set this to false if you don't want
# to use docker. You must have the python
# libraries installed on your machine
use_container = True

# Explain what a job cache does
# TODO: change database argument to database_name, or something else appropriate
# Explain that to use a job cache, you need to have a mongo database running.
# Otherwise, use job_cache = None
db = hi.Database(
    mongo_url='mongodb://localhost:27017',
    database='hither'
)
job_cache = hi.JobCache(
    database=db,
    cache_failing=False,
    rerun_failing=False
)
# TEST
# TODO: maybe replace this with some useful computation that uses something from scipy - think about this
@hi.function('sumsqr', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def sumsqr(x):
    return np.sum(x**2)

with hi.Config(
    job_handler=job_handler,
    job_cache=job_cache,
    container=use_container
):
    # TODO: think about doing something more interesting
    x = np.array([1, 2, 3, 4])
    result = sumsqr.run(x=x).wait()
    print(f'Result: {result}')

```

## Pipeline example

Give another full example showing how to pass the output of one function as an input to another.

## Parallel computing example

Give another full example of looping through a list of arguments, accumulating a list of job results, and then aggregating the outputs after processing completes.


## Reference documentation

[Reference documentation](doc/reference.md)

## Authors

* Jeremy Magland
* Jeff Soules
