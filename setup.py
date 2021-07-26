import setuptools

pkg_name = "hither"

setuptools.setup(
    packages=setuptools.find_packages(),
    include_package_data=True,
    scripts=[
        "bin/hither-scriptdir-runner"
    ],
    install_requires=[
        "click",
        "inquirer",
        "pyyaml",
        "dockerfile-parse",
        "kachery_client",
        "docker"
        # non-explicit dependencies: numpy
    ]
)
