import hither as hi
from expensive_calculation import expensive_calculation

# Create a job cache that uses /tmp
# You can also use a different location
jc = hi.JobCache(use_tempdir=True)

with hi.Config(job_cache=jc):
    # subsequent runs will use the cache
    val = expensive_calculation.run(x=4).wait()
    print(f'result = {val}')