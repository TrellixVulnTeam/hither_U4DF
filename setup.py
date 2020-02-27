import setuptools

pkg_name = "hither2"

setuptools.setup(
    name=pkg_name,
    version="0.1.1",
    author="Jeremy Magland",
    author_email="jmagland@flatironinstitute.org",
    description="Run batches of Python functions in containers and on remote servers",
    packages=setuptools.find_packages(),
    scripts=[],
    install_requires=[
        "pymongo"
        # non-explicit dependencies: kachery, numpy
    ],
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    )
)
