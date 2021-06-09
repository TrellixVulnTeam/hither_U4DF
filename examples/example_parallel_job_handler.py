import hither2 as hi
from .expensive_calculation import expensive_calculation

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