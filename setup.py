from setuptools import find_packages, setup

setup(
    name="APS_Python_core_pkg",
    packages=find_packages("src"),
    package_dir={"": "src"},
    # include_package_data=True,
    # TODO Just a hack to include all the data directories for now,
    # have to use recurse to improve
)
