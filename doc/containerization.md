# hither containerization

## Overview of containers

You can think of a container as being partway between a [virtual machine](https://en.wikipedia.org/wiki/Virtual_machine) (VM) and a [conda environment](https://docs.conda.io/projects/conda/en/latest/index.html). Whereas a VM emulates an entire computer operating system (on top of the host OS), a conda environment (or a virtual environment) simply resets environment variables so that certain versions of Python packages and other software become available. The *container* is closer to a VM, in that a container image is defined by a recipe that can allow installation of essentially whatever software packages are required down to the OS level if necessary. But unlike a true VM it relies on the host operating system's calls where possible (rather than running a complete OS on simulated hardware). Running code in a container is therefore very close to running directly on the host OS, both in terms of startup time and computational efficiency. At the same time, it provides a high level of isolation so that code can be reliably ported between computer systems.

See also:

* [What is a docker image? How is that different from a Docker container?](./faq.md#what-is-a-docker-image-how-is-that-different-from-a-docker-container)
* [What is the relationship between Docker and Singularity?](./faq.md#what-is-the-difference-between-docker-and-singularity)
* [Are Docker and Singularity required in order to use hither?](./faq.md#are-docker-and-singularity-requires-in-order-to-use-hither)
* [Can I use local docker images with hither, or do they need to be stored on Docker Hub?](can-i-use-local-docker-images-with-hither-or-do-they-need-to-be-stored-on-docker-hub)
* [When using a remote compute resource, where are docker images downloaded to?](./faq.md#when-using-a-remote-compute-resource-where-are-docker-images-downloaded-to)
* [How does Hither manage dependencies on Python modules or other code outside my function?](./faq.md#how-does-hither-manage-dependencies-on-python-modules-or-other-code-outside-my-function)
* [How can I tell hither to use Singularity instead of Docker?](#using-singularity)

## Using hither to run Python code in a container

First, ensure that Docker (or Singularity) is installed and accessible to the
user who will be running hither.

Then, when declaring a [hither function](./hither-functions.md), simply decorate it with a notation like the following:

```python
import hither2 as hi

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
to `docker pull` and subsequently to `docker run` (or `singularity exec` in the case of Singularity mode, see below).

For example, see [example_integrate_bessel.py](../examples/example_integrate_bessel.py) which runs the hither function defined in [integrate_bessel.py](../examples/integrate_bessel.py).

The `image_url` can refer to an image on a public hosting solution such as 
Docker Hub, or to any other location which can be accessed by the system which will be running hither.

The second argument to `@hi.function` is the version of the function, which is relevant to the optional [job cache](./job-cache.md).

## Using Singularity

Due to security concerns in shared environments it is not always possible to access Docker on the computer where you want the computations to run. Fortunately, it is possible to use [Singularity](https://sylabs.io/singularity/), which supports running Docker images. To use Singularity instead of Docker to run the containerized functions, simply set the environment variable:

```bash
export HITHER_USE_SINGULARITY=TRUE
```

Then, behind the scenes, all `docker run` commands will be replaced by corresponding `singularity build` and `singularity exec` commands. No Python source code needs to change.

## Job Serialization

In the context of computing, [serialization](https://en.wikipedia.org/wiki/Serialization) means converting
data into a format that can be stored or transmitted between processes and machines, and then later
reconstructed. In order for a hither function to run in a container, the Job (that is, the function code,
with its specific inputs) must be serialized. hither converts function code (in text format) and
basic [data types](https://docs.python.org/3/library/stdtypes.html) to
[JSON](https://www.json.org/json-en.html).

However, the JSON format is not necessarily appropriate for large files or complex objects
such as [numpy arrays](https://numpy.org/doc/1.18/reference/generated/numpy.array.html). Numpy
arrays are written out to files on the file system. The corresponding file is then stored
in kachery, hither's universal content manager, and the file contents are retrieved when the
Job is deserialized and run in a container.

Functions that take input objects with non-serializable types are not currently supported
by hither.
