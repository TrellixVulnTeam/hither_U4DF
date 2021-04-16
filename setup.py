import setuptools

pkg_name = "hither"

setuptools.setup(
    packages=setuptools.find_packages(),
    include_package_data=True,
    scripts=[
        "bin/hither-compute-resource",
        "bin/hither-scriptdir-runner",
        "bin/hither-log"
    ],
    install_requires=[
        "click",
        "inquirer",
        "pyyaml",
        "dockerfile-parse",
        "kachery_p2p>=0.8.10",
        "docker"
        # non-explicit dependencies: numpy
    ]
)
