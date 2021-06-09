# Parallel computing

While hither itself will not make your code parallel, hither
natively supports job-level and some forms of data-level
parallelism, which can be used in many different pipelines.

## Definition

In computing, [parallelism](https://en.wikipedia.org/wiki/Parallel_computing) is
structuring a task so that multiple useful computations can be carried out
simultaneously. Implementations can range from having multiple processor
cores with their own instruction pipelines, to the
[Single Instruction Multiple Data](https://en.wikipedia.org/wiki/SIMD) paradigm
of modern GPUs, to coordinating distributed jobs among different physical machines
over a network.

## Advantages and Limitations

The obvious advantage of parallelism is speed: by splitting a task into units
which can be done independently and doing those units simultaneously, more
work can be completed in the same amount of clock time. The vast majority
of performance improvements in computer processing since the early 2000s
have been due to increased use of parallel processing.

Parallel approaches are not applicable for every problem. It is difficult
to parallelize different stages of a pipeline
where the later stages depend on the results of earlier steps,
or where results must be evaluated to choose the next step.

Instead, parallelism is easiest to achieve when the same set of operations can be
applied to different data simultaneously: like a group of students completing
a homework assignment faster by assigning one problem to each student and
sharing the results. This is known as *data-*, *job-*, or *task-level* parallelism.

## Support with hither

The fundamental unit of work for hither is the function, combined with its
arguments to form a Job. Since hither interacts with hither functions as a black
box (without changing their internals), it cannot make a serial function
parallel on its own. However, where hither excels is in offering the ability
to leverage task-level parallelism: since each Job is a function wrapped
with its own separate data, you can achieve parallel execution by running
multiple Jobs at the same time against an appropriate job handler (such
as the `ParallelJobHandler`, `RemoteJobHandler`, or `SlurmJobHandler`).

Moreover, because hither does not change the contents of your functions,
if you do make a hither function from code that already executes
in parallel, this should be supported in most cases. The exception is
code that is intended to run on a cluster environment and spawn
subprocesses; **hither does not currently have a way to ensure that
child processes will also be spawned in their own containers on
separate nodes. [IS THIS TRUE?]**

## Example

Here is an example that runs `8` jobs with `4` parallel workers
(see [parallel_example.py](./parallel_example.py))

```python
import hither2 as hi
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
```
