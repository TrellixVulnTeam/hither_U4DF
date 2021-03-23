import time
from typing import List
from hither.job import Job
import hither2 as hi
import numpy as np

@hi.function('multiply_arrays', '0.1.1')
def multiply_arrays(x: np.ndarray, y: np.ndarray, delay: float):
    if delay > 0: time.sleep(delay)
    return x * y

def test_parallel_cache():
    # Define the job cache
    jc = hi.JobCache(feed_name='default-job-cache')

    # Define the parallel job handler
    jh = hi.ParallelJobHandler(num_workers=4)

    # Collect the jobs in the pipeline in this list
    jobs: List[hi.Job] = []
    with hi.Config(job_cache=jc, job_handler=jh):
        for i in range(8):
            print(f'Creating job {i}')
            j = hi.Job(multiply_arrays, dict(x=np.array([i, i]), y=np.array([2, 2]), delay=4))
            jobs.append(j)
    
    # Wait for jobs to complete
    print('Waiting for jobs to complete')
    hi.wait(None)

    # Accumulate the results
    all_results: List[np.ndarray] = []
    for j in jobs:
        if j.status == 'finished':
            print('RESULT:', j.status, j.result.return_value)
            all_results.append(j.result.return_value)
        elif j.status == 'error':
            print('ERROR', j.result.error)
    
    print(f'Accumulated {len(all_results)} results')

if __name__ == '__main__':
    test_parallel_cache()