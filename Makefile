.ONESHELL:

# Need to specify bash in order for conda activate to work.
SHELL=/bin/bash
ROOT_DIR=$(PWD)
PACKAGE_NAME=ff_mission_dashboard
MANAGER=mamba
ENV_NAME=ff-mission-dashboard-env
export PATH := $(HOME)/mambaforge/bin/:$(PATH)

# MAMBA requires two inits
SOURCE=source $$($(MANAGER) info --base)/etc/profile.d/conda.sh; source $$($(MANAGER) info --base)/etc/profile.d/$(MANAGER).sh
# Note that the extra activate is needed to ensure that the activate floats env to the front of PATH
# ACTIVATE=$(MANAGER) init;$(MANAGER) activate $(ENV_NAME)
ACTIVATE=$(SOURCE); $(MANAGER) activate ; $(MANAGER) activate $(ENV_NAME)

# Prerequisite: mambaforge installed
# Creates a proper environment from the environment file
create_env_from_file:
	$(MANAGER) env create -f environment.yml --force

# Builds the current repo and installs into the environment
install_pkg:
	($(ACTIVATE); pip install .)

# Builds the current repo and installs into the environment, while also tracking any new changes to it.
develop:
	($(ACTIVATE); pip install -e .)

# Removes the package from the env
uninstall_pkg:
	($(ACTIVATE); pip uninstall -y $(PACKAGE_NAME))

build_env:
	git submodule update --init
	make create_env_from_file
	($(ACTIVATE); pre-commit install)
	make install_pkg

doc:
	cd ./docs && ($(ACTIVATE); make html)

clean:
	($(SOURCE); $(MANAGER) deactivate; $(MANAGER) env remove --name $(ENV_NAME) -y)

update:
	make uninstall_pkg
	make install_pkg

test:
	($(ACTIVATE); pytest)

# Only for DIL Dashboard
# dashboard_build:
# 	($(ACTIVATE); mercury watch $(ROOT_DIR)/tutorials/FF1_DIL_Dashboard.ipynb)

# dashboard_clean:
# 	($(ACTIVATE); mercury flush)


# Activates and creates the environment file
# create_env_file:
# 	($(CONDA_ACTIVATE); conda env export | cut -f -2 -d "=" | grep -v -E "^prefix:|^name:" > environment.yml)

streamlit_serve:
	($(ACTIVATE); streamlit run app/entry.py)