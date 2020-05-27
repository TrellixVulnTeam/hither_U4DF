import setuptools

pkg_name = "hither"

setuptools.setup(
    name=pkg_name,
    version="0.1.3",
    author="Jeremy Magland",
    author_email="jmagland@flatironinstitute.org",
    description="Run batches of Python functions in containers and on remote servers",
    packages=setuptools.find_packages(),
    include_package_data=True,
    scripts=[
        "bin/hither-compute-resource"
    ],
    install_requires=[
        "pymongo",
        "click",
        "inquirer"
        # non-explicit dependencies: kachery, numpy
    ],
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    )
)
