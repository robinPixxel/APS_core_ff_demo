# FF Mission Dashboard

This project hoststhe FF mission dashboard.


To install mamba, follow the instructions on the following pages:
1. https://mamba.readthedocs.io/en/latest/installation.html
2. https://github.com/conda-forge/miniforge#mambaforge


To create an environment for local testing, use the following command:
`make build_env`

To activate the created environment, use the following command:
`mamba activate mda-core-env`

Whenever you make any changes to the codebase and want to update the installed packages in the environment, use the following command:
`make update`

To install a live copy of `mda-core` into the environment during development, use the following command:
`make develop`

If you want to create a dashboard for Day in the Life Dashboard, run the following command:
`make dashboard_build`

You can use the following command to clean and flush all dashboard data:
`make dashboard_clean`

### pre-commit ###
The project uses [pre-commit](https://pre-commit.com/index.html) to manage and maintain pre-commit hooks which enforces linting, static-code analyzing, etc.
All the managed pre-commit hooks can be found in the file `.pre-commit-config.yaml`.
After any hook addition make sure to run it against all files by `pre-commit run --all-files` to test if the new hook works.
Update the pre-commit hooks by running `pre-commit autoupdate` periodically.

### Coding style ###
This project uses [Black coding style](https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html) throughout.
Black is added as a hook of pre-commit and that handles the black installation/environment/invoking etc.

### Style guide check ###
This project uses [Flake8](https://flake8.pycqa.org/en/latest/index.html) as a pre-commit hook to enforce style guide.
All the specific flake8 config is stored in `.flake8` file and refer to [Flake8 rules](https://www.flake8rules.com/).
