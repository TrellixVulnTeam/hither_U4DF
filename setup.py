import setuptools

pkg_name = "hither"

setuptools.setup(
    packages=setuptools.find_packages(),
    include_package_data=True,
    scripts=[
        "bin/hither-compute-resource"
    ],
    install_requires=[
        "click",
        "inquirer",
        "dockerfile-parse",
        "kachery_p2p>=0.8.6"
        # non-explicit dependencies: numpy
    ]
)
