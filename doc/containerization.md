# hither containerization

## Overview of containers

You can think of a container as being partway between a [virtual machine](https://en.wikipedia.org/wiki/Virtual_machine) (VM) and a [conda environment](https://docs.conda.io/projects/conda/en/latest/index.html). Whereas a VM emulates an entire computer operating system (on top of the host OS), a conda environment (or a virtual environment) simply resets environment variables so that certain versions of Python packages and other software become available. The *container* is closer to a VM, in that a container image is defined by a recipe that can allow installation of essentially whatever software packages are required down to the OS level if necessary. But unlike a true VM it relies on the host operating system's calls where possible (rather than running a complete OS on simulated hardware). Running code in a container is therefore very close to running directly on the host OS, both in terms of startup time and computational efficiency. At the same time, it provides a high level of isolation so that code can be reliably ported between computer systems.

See also:
* [What is a docker image? How is that different from a docker container?](./faq.md#what-is-a-docker-image-how-is-that-different-from-a-docker-container)
* [What is the relationship between docker and singularity?](./faq.md#what-is-the-difference-between-docker-and-singularity)
* [Are docker and singularity required in order to use hither?](./faq.md#are-docker-and-singularity-requires-in-order-to-use-hither)
* [Can I use local docker images with hither, or do they need to be stored on Docker Hub?](can-i-use-local-docker-images-with-hither-or-do-they-need-to-be-stored-on-docker-hub)
* [When using a remote compute resource, where are docker images downloaded to?](./faq.md#when-using-a-remote-compute-resource-where-are-docker-images-downloaded-to)
* [How does Hither manage dependencies on Python modules or other code outside my function?](./faq.md#how-does-hither-manage-dependencies-on-python-modules-or-other-code-outside-my-function)

## Using hither to run Python code in a container

First, ensure that Docker (or Singularity) is installed and accessible to the
user who will be running hither.

Then, when declaring a [hither function](./hither-functions.md), simply decorate it with a notation like the following:

```python
import hither as hi

@hi.function('my_function', '0.1.0')
@hi.container('docker://image_url')
def my_function():
    # do your computations
    # and return a value
```
`my_function` will then be wrapped as a hither function. When the job is run by calling the
`.run()` method on the function, the resulting Job will be run in the container whose
image is specified by `image_url`, provided that the
active configuration includes `container=True`. Under the hood, whatever value is given to the `container` decorator will be passed on
to `docker pull` and subsequently to `docker run`.

For example, see [example_integrate_bessel.py](../examples/example_integrate_bessel.py) which runs the hither function defined in [integrate_bessel.py](../examples/integrate_bessel.py).

The `image_url` can refer to an image on a public hosting solution such as 
DockerHub, or to any other location which can be accessed by the system which will be running hither.

The second argument to `@hi.function` is the version of the function, which is relevant to the optional [job cache](./job-cache.md).
