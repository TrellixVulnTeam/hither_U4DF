#!/usr/bin/env python

import hither as hi
import time

# Define a hither function that squares the input
# and simulates a long computation with a delay
@hi.function('sqr_with_delay', '0.1.0')
def sqr_with_delay(x, delay=None):
    if delay is not None:
        time.sleep(delay)
    return x * x

# Create a parallel job handler
job_handler = hi.ParallelJobHandler(num_workers=4)

# Time the overall execution
timer = time.time()

# Accumulate the results in an array
results = []

# Configure hither to use the parallel job handler
with hi.Config(job_handler=job_handler):
    for j in range(8):
        # Create the job and append output to results
        y = sqr_with_delay.run(x=j, delay=1)
        results.append(y)

    # Wait for all jobs to complete
    hi.wait()

# Print the results
print('Results:', [y.get_result() for y in results])
print(f'Elapsed time: {time.time() - timer} sec')
