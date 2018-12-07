# drp_1dpipe

## Installation

### Requirements

Activate your virtualenv as needed.

Install Subaru-PFS datamodel. See https://github.com/Subaru-PFS/datamodel/tree/master/python/pfs

Install Amazed library for Subaru-PFS project. See https://github.com/Subaru-PFS/drp_1d

Install required python packages

	pip install -r pip-requirements.txt

### Using pip

To install Subaru PFS 1D data reduction pipeline with `pip` simply run.

    pip install .

### Testing an installed 1D DRP pipeline

> Note : To run the tests, install pytest : `pip install pytest`

To run the tests simply run from the `drp_1dpipe` directory:

	pytest

## Getting started

To run the 1d DRP pipeline on a local machine, create a new `workdir` directory including the two subdirectories `spectra` and `calibration`.

	workdir
		|-- spectra
		|-- calibration

Put all the psfObject files you want to process inside the `spectra` directory.
Put all the calibration files you need inside the `calibration` directory.

To process spectra simply run :

	drp_1dpipe --workdir=/your/working/directory

At the end, the pipeline creates `output-[N]` directories containing 1d DRP product.

	workdir
	 |-- spectra
	       |-- pfsObject-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits
	       |-- [...]
	 |-- calibration
	 |-- [...]
	 |-- output-[N]
	       |-- pfsObject-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits-1
	       |-- pfsZcandidates-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits
	       |-- [...]
	       |-- redshift.csv
	       |-- version.json

For the `pfsObject-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits` input file :
* `pfsObject-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits-1` contains all the processing output
* `pfsZcandidates-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits` is the 1d DRP product
* `redshift.csv` is a summary table of best redshift for every input spectrum
* `version.json` is the version hash of the pipeline used to process data
