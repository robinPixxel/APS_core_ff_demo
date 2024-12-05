from setuptools import find_packages, setup

import versioneer

setup(
    name="ff_mission_dashboard",
    # version=versioneer.get_version(),
    # cmdclass=versioneer.get_cmdclass(),
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
)
