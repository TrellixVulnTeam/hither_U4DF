import hither2 as hi
from expensive_calculation import expensive_calculation

# Create a job cache
jc = hi.JobCache(feed_name='example')

with hi.Config(job_cache=jc):
    # subsequent runs will use the cache
    job: hi.Job = expensive_calculation.run(x=4)
    print(f'result = {job.wait().return_value}')