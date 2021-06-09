# Job cache

## Overview of the job cache

TODO: Explain what the job cache is.

See also:

* [Is the job cache always used?](./faq.md#is-the-job-cache-always-used)
* [What information does hither use to form the job hash for purposes of job caching?](./faq.md#what-information-does-hither-use-to-form-the-job-hash-for-purposes-of-job-caching)

## How to use a hither job cache

Configure the hither environment to use a job cache as follows:

```python
import hither2 as hi

# Set up a job cache in the temporary directory
jc = hi.JobCache(use_tempdir=True)

with hi.Config(job_cache=jc):
    # Results of any hither jobs run
    # within this context will be cached.
    # They will not need to re-execute on
    # subsequent runs.
```

You can specify a different location for the job cache via:

```python
# Set up a job cache in a custom directory
jc = hi.JobCache(path='/path/to/job/cache')
```

Before running any Job, hither will first check the cache to see if that result
has already been computed; if it has, the cached result will be returned instead
of redoing the computation.

By default, hither does not cache failing jobs. But if you want to remember that a job threw an exception, then you can set `cache_failing=True` in the constructor of the job cache as follows:

```python
jc = hi.JobCache(use_tempdir=True, cache_failing=True)
```

TODO: explain `rerun_failing`

TODO: figure out how to force a particular job to run. Use `force_run`?
