import hither2 as hi
from .expensive_calculation import expensive_calculation
from .arraysum import arraysum

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