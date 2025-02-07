#check_change
from setuptools import find_packages, setup

setup(
    name="APS_Python_core",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires = ['highspy==1.8.1',
                        'pandas==2.2.3',
                        'PuLP==2.9.0',
                        'python-dateutil==2.9.0.post0',
                        'pytz==2024.2',
                        'six==1.16.0',
                        'tzdata==2024.2',
                        'plotly==5.24.1',
                        'matplotlib== 3.10.0',\
                        'openpyxl==3.1.5']
    # include_package_data=True,
    # TODO Just a hack to include all the data directories for now,
    # have to use recurse to improve
)
