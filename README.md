# drp_1dpipe

## Installation

### Requirements

Activate your virtualenv.

#### pfs-datamodel

	git clone -b develop git@gitlab.lam.fr:CPF/pfs-datamodel.git
	cd pfs-datamodel/python
	pip install -e .

#### cpf-redshift

	git clone -b develop git@gitlab.lam.fr:CPF/cpf-redshift.git
	cd cpf-redshift
	mkdir build
	cd build
	cmake .. -DBUILD_SHARED_LIBS=ON -DCMAKE_BUILD_TYPE=Release
	make all
	pip install -e ..

### drp_1dpip

    pip install -e .
