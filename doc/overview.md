# hither Overview

## What is hither?

hither is a flexible, platform-agnostic job manager that allows researchers
to easily deploy code across local, remote, and cluster environments with
minimal changes. It ensures that different scientific tools can be run
through a consistent pipeline, even when they have different configurations
or conflicting dependencies. This works through universal containerization.
Because jobs are run through a universal interface, code becomes much more
portable between labs; the same pipelines can be run locally, or even on a
cluster environment. In this way, development on small datasets can take
place on your laptop, with the confidence that the code will work on large
datasets in the cluster with minimal modifications.

## Why use hither?

- **Consistency**. The goal of any containerized system is *consistency:* ensuring that code will
run with a stable environment, and thus give reliable results. By leveraging
containerization, hither brings this consistency to scientific code. Moreover,
by giving scientific code a well-defined, portable environment in which to
execute, hither makes it easy to leverage multiple-core or multiple-machine
resources to run data processing pipelines in parallel.

- **Portability**. Code in a well-defined container will run the same
 way everywhere that container runs. An execution environment only needs
 a correctly configured instance of the Docker or Singularity container
 manager, rather than whatever complex requirements are mandated by
 the actual code to be executed.

- **Performance**. Because every function runs in its own container,
 and hither manages setting up and running the containers, it is trivial
 to add more compute resources and achieve faster clock-time performance.

- **Reliability**. Container systems are defined in human-readable descriptor
 files that operate the same way regardless of the underlying system. If
 code works correctly on your laptop, it will work correctly on your
 cluster (or on your reviewers' cluster); if it works correctly today,
 it will also work correctly for future researchers building off of it.

- **Simplicity**. hither is designed to deliver these benefits while
 requiring as little re-engineering as possible for the underlying methods.
 Whether you are using methods developed in your lab, your collaborators',
 or even elsewhere, you can create hither-based pipelines without having
 to spend days of research time trying to re-implement or write glue for
 code that's already been published. If you can set up an environment
 where the code runs at all, you can use it in hither.

## How does hither compare with other tools?

### For containerization

The conventional approach to running a process in a container is to describe the
container through a container definition file, which may include the actual
commands to be run within the container, or else write OS-level scripts which
spin up a container and execute commands within it. Both these approaches
are *imperative*: you have to tell the container what to do. Moreover, they
require the user to deal with the complications of ensuring that data files
will be available to the containerized processes, that appropriate networking
connections have been configured, and so on.

By contrast, hither offers a *declarative* approach to containerization: you
define an environment in which your code can run (which you can verify
by testing with conventional interactive shells), and then decorate whatever
function you wish to run in the container with a reference to your environment.
You can then use the function like normal elsewhere in your program, and
hither will take care of all the challenges of running the code within the
container, making sure that data files are available, and communicating the
results back to the calling code.

hither's approach to containerization also greatly simplifies dependency
management. Most code of any complexity has other software packages it
depends upon: particular libraries or versions of libraries, etc. These may
or may not be appropriate for the system as a whole. The usual solution to
ensure that these dependencies are available is an execution environment,
like the ones provided by [`virtualenv`](https://virtualenv.pypa.io/en/latest/)
or [Anaconda](https://www.anaconda.com/). But as with containers, the usual
paradigm requires setting up the environment, then entering it and
executing actions. With hither, individual functions are injected into
an appropriate environment from within your own Python code. You can call
your own wrapper code from whatever environment you wish, so long as it
has access to the network and an appropriate containerization engine,
and hither will take care of setting up the environment you have
requested for the functions you wish to run.

Moreover, this works for many different functions at once. If you have several
different functions that should operate on the same data set--such as the
quantitative comparisons of spike-sorting algorithms featured in
[SpikeForest](https://www.simonsfoundation.org/spikeforest/)--each function
can be given its own appropriate container environment, even when some of
the environments are mutually contradictory. Code that couldn't even run
on the same system can now be called from within the same program!

### For multiprocessing and parallelism

There's no shortage of tools in python for speeding up code by making it run in
parallel. However, hither is operating at a higher level of abstraction than most
of these tools.

- **[`concurrent.futures`](https://docs.python.org/3/library/concurrent.futures.html) &
 [`multiprocessing`](https://docs.python.org/3/library/multiprocessing.html)**
 are native Python libraries that allow a developer to write code using low-level
 multiprocessing or parallel-processing features, like spawning child processes,
 interacting with a thread pool, or performing asynchronous communication between
 independent processes running in parallel. These are the tools you would use to write
 a system like a web server that spawns new threads to respond to incoming requests,
 or to separate user interface code from data processing code. The features are very powerful,
 but can be challenging to use correctly, especially for non-specialists.
 Moreover, much of this utility is not needed for commonplace batch operations
 in scientific data processing. hither aims to provide performance improvements without
 requiring developers to dive into the complex internals of multiprocess coordination.

- **[dask](https://dask.org/)** has attracted a great deal of attention as a means of
 bringing the performance improvements of distributed processing to pipelines built
 around standard data science tools like `numpy`, `pandas`, and `sklearn`. The dask
 package offers drop-in replacements for basic matrix/array/dataframe data structures
 which are monitored internally so that operations involving them can be
 transparently distributed across cores.
 This creates the opportunity for substantial speed-up within the implementation
 of a particular algorithm. By contrast, hither's atomic unit is the function
 itself, as the functions it wraps can be treated as a black-box. You might use
 dask to parallelize particular computations on the data within a processing step,
 and use hither to coordinate multiple runs of that processing step simultaneously
 with different data sets.

- **[Celery](https://github.com/celery/celery)** is a task scheduler and manager.
Parts of hither's interprocess communication use a similar idea of adding
tasks to a queue; however, hither is simpler to use and provides an entire
framework for containerized execution of jobs. Celery's use cases focus on
coordinating distinct parts of a distributed system of discrete components,
while hither, again, is dealing with parallelism by distributing different data
over similar pipelines--like a more flexible version of the SIMD approach used
in graphics processing units.

- **[Apache Airflow](https://airflow.apache.org/)**, much like Celery, is
 more about task management. Building data science pipelines in Airflow is a
 common use case. But this tool is quite complex and is certainly overkill when one wants to simply run many non-interacting jobs simultaneously. Hither has a much lower barrier to entry and has a simpler interface with minimal overhead.

### For reproducibility

Reproducibility of analysis is an extremely important topic in modern research.
Unfortunately, code is often kept private, or depends on idiosyncratic local
executioon environments, making it difficult to verify published claims. Other
researchers may have access to build/execution scripts or makefiles; in the
best case, [Jupyter notebooks](https://jupyter.org/)
are available, but they present their own challenges to version control,
collaboration, and code-sharing as well. hither makes it easy to define an
exectuion environment (in the container definition) and a set of steps to produce
a result; and with [kachery](https://github.com/flatironinstitute/kachery), the hither ecosystem can even make it easy to
distribute data files to interested parties, leading to truly reproducible
research.
