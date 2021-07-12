# drp_1dpipe

## Installation

### Requirements

Activate your virtualenv as needed.

Add the Subaru-PFS datamodel to your PYTHONPATH. See https://github.com/Subaru-PFS/datamodel/tree/master/python

	export PYTHONPATH=/path/to/Subaru-PFS/datamodel/python

Install Amazed library for Subaru-PFS project. See https://github.com/Subaru-PFS/drp_1d

### Install drp_1dpipe

To install Subaru PFS 1D data reduction pipeline, simply run :

    pip install .

### Testing an installed 1D DRP pipeline

> Note : To run the tests, install pytest : `pip install pytest`

To run the tests simply run from the `drp_1dpipe` directory:

	pytest

## Getting started

To run the 1d DRP pipeline on a local machine, create a new `workdir` directory including the two subdirectories `spectra` and `calibration`.
Base directory with spectra examples and calibration directory can be found for each release at https://pfs.ipmu.jp/internal/devarch/lam-drp1d/

	workdir
		|-- spectra
		|-- calibration

Put all the psfObject files you want to process inside the `spectra` directory.
Put all the calibration files you need inside the `calibration` directory.

To process spectra simply run :

	drp_1dpipe --workdir=/your/working/directory

At the end, the pipeline creates `output/B[N]` directories containing 1d DRP product.

	workdir
	 |-- spectra
	       |-- pfsObject-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits
	       |-- [...]
	 |-- calibration
	 |-- output
         |-- config.conf
	       |-- B[N]
	            |-- pfsObject-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits-1
	            |-- pfsZcandidates-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits
				      |-- redshift.csv
				      |-- version.json

For the `pfsObject-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits` input file :
* `pfsObject-xxxxx-y,y-zzz-XXXXXXXXXXXX` contains all the temporary processing output
* `pfsZcandidates-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits` is the 1d DRP product
* `config.json` is summary file with all the input arguments
* `redshift.csv` is a summary table of best redshift for every input spectrum
* `version.json` is the version hash of the pipeline used to process data
