import codecs
import os.path
import setuptools

def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()

def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


pkg_name = "hither"

setuptools.setup(
    name=pkg_name,
    version=get_version("hither/__init__.py"),
    author="Jeremy Magland, Jeff Soules",
    author_email="jmagland@flatironinstitute.org",
    description="Run batches of Python functions in containers and on remote servers",
    url="https://github.com/flatironinstitute/hither",
    packages=setuptools.find_packages(),
    include_package_data=True,
    scripts=[
        "bin/hither-compute-resource"
    ],
    install_requires=[
        "click",
        "inquirer",
        "kachery_p2p>=0.5.14"
        # non-explicit dependencies: numpy
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ]
)
