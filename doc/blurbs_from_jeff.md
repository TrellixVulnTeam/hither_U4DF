# Blurbs

## What is hither?

Hither is a flexible, platform-agnostic job manager that allows researchers to easily deploy code across local, remote, and cluster environments with minimal changes. It ensures that different scientific tools can be run through a consistent pipeline, even when they have different configurations or conflicting dependencies. This works through universal containerization: if you would like to call a python function from a particular package, we provide a set of decorators for that function which allow the user to describe the required environment, which we can then package and deliver to any system that supports Docker or, for the more security-conscious, Singularity.

Hither also provides tools to generate pipelines of chained functions, so that the output of one processing step can be fed seamlessly as input to another, and to coordinate execution of jobs until all their dependencies have been satisfied. Finally we provide a job cache which avoids rerunning expensive processing steps, by recording the output produced by each hither function given its inputs. (Use cases like methodological comparisons and scientific benchmarking work best when the underlying functions being used with hither are deterministic, but where that is not the case, we provide an option to bypass the cache and force re-evaluation.)

Because jobs are run through a universal interface, code becomes much more portable between labs. And the same pipelines can be run locally, or even on a cluster environment (assuming the underlying code being called is compatible); so development on small datasets can take place on your laptop, with the confidence that the code will work on large datasets in the cluster with minimal modifications.

## What is a container?

For those who are a little unsure about containerization, you can think of a container as being partway between a virtual machine and a conda environment (which simply resets environment variables so that a particular version of Python with a particular package suite is visible). The container is closer to a VM, in that a container image is a recipe that can allow installation of essentially whatever software packages are required down to the OS level if necessary, but unlike a true VM it relies on the host operating system's calls where possible (rather than running a complete OS on simulated hardware).