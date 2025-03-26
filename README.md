# drp_1dpipe

## Installation

### Requirements

Activate your virtualenv as needed.

Install Subaru-PFS datamodel. See https://github.com/Subaru-PFS/datamodel/tree/master/python

	git clone https://github.com/Subaru-PFS/datamodel.git
	cd datamodel
	pip install .

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
		|-- pfsCoaddFile.fits
		|-- calibration

Put all the psfObject files you want to process inside the `spectra` directory.
Put all the calibration files you need inside the `calibration` directory.

To process spectra simply run :

	drp_1dpipe --workdir=/your/working/directory

At the end, the pipeline creates `output/data` directory containing 1d DRP products.

	workdir
	 |-- pfsCoaddFile.fits
	 |-- calibration
	 |-- output
	       |--config.json
	       |--log
	       |   |-- B0
	       |        |-- amazed.log
	       |	|-- process_spectra.log
	       |-- data
	            |-- pfsZcandidates-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits
	            |-- pfsZcandidates-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits
		    :
		    :
		    |-- pfsZcandidates-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits
		    
For the `pfsCoadd.fits` input file :
* `pfsZcandidates-xxxxx-y,y-zzz-XXXXXXXXXXXX.fits` are the 1d DRP products
* `config.json` is summary file with all the input arguments
* `amazed.log` is drp1d log (here for all spectra of bunch 0)
* `process_spectra.log` is drp1d_pipe log (here for all spectra of bunch 0)

## Datamodel Documentation

Here is a reminder for what should be in datamodel.txt on datamodel project. It only concerns warning and error codes

### Warnings

Possible warning flags list (in order from 0 to  

**AIR_VACUUM_CONVERSION_IGNORED**
   An air to vacuum conversion has been defined in the parameters. 
   However, it is specified in the input spectrum that the data are in vacuum.
   The air to vacuum conversion is therefore ignored.

**PDF_PEAK_NOT_FOUND**
   For a given redshift candidate, in the second pass window, no pdf peak has
   been found.

**ESTIMATED_STD_FAR_FROM_INPUT**
   Residual std (obtained from rms) (spectrum - model) very different
   (ratio > 1.5 & 1/ 15 <-> 50 + or 50%-) from input spectrum std

**LINEMATCHING_REACHED_ENDLOOP**
   In linematching, when trying to match lines on the spectrum, the maximum
   number of iterations has been reached without converging.

**FORCED_IGNORELINESUPPORT_TO_FALSE**
   In line model, for continuum fitting, in the parameters fft processing is
   active and ignore line support too (lineModel.continuumFit.ignoreLineSupport).
   However masking lines to fit continuum is in this case impossible.
   To fix this, ignorelinesupport is forced to False i.e.full spectrum with lines
   is used for template fitting.

**FORCED_CONTINUUM_COMPONENT_TO_FROMSPECTRUM**
   In line model, while fitting the continuum, a negative continuum amplitude
   has been found.
   To fix this, continuumComponent is forced to fromSpectrum.

**AIR_VACUUM_REACHED_MAX_ITERATIONS**
  Wavelengths convertion from air to vacuum is made by iterating an inverse
  translation, until a minimal precision is reached or until the max number
  of iterations is reached.
  The marning means that max precision was not reached at the end ot the iterations.

**ASYMFIT_NAN_PARAMS**
   At least one asymetric fitting param is NaN. All parameters of asymetric line
   profile are therefore set to NaN.

**DELTAZ_COMPUTATION_FAILED**
   Some error appeared when trying to calculate Deltaz on a redshift candidate
   probability.

**INVALID_FOLDER_PATH**
   Absolute path to piors calibration file does not exist. No priors used

**FORCED_CONTINUUM_TO_NOCONTINUUM**
   The calculated continuum amplitude is weaker than the min continuum amplitude
   defined in parameters (`continuumFit.nullThreshold`).
   Therefore, continuum is set to noContinuum.

**FORCED_CONTINUUM_REESTIMATION_TO_NO**
  Continuum reestimation was set to "onlyextrema" but it is not possible since
  second pass is skipped. It is automatically set to no continuum reestimation.

**LESS_OBSERVED_SAMPLES_THAN_AMPLITUDES_TO_FIT**
  Deficient rank for lines fitting: when trying to fit lines with line model,
  the number of parameters to find is greater than the number of observed samples.

**LBFGSPP_ERROR**
   There was an error while fitting whith lbfgspp. We then fix line position and
   width and use the initial svd guess for the amplitude.

**PDF_INTEGRATION_WINDOW_TOO_SMALL**
   PDF integration range goes beyond redshift window size. Integration range is
   therefore cropped.
   If this warning appears many times, consider increasing in the parameter
   second pass `halfWindowSize` value.

**FORCED_POWERLAW_TO_ZERO**
   For QSO power law fitting: many samples have fluxes too low compared to SNR to be taken into account.
   Power law coefficients are set to zero.

**UNUSED_PARAMETER**
   A parameter has been defined in the parameters file but it is not used. 
   NB: Some parameters are usefull only under some conditions.

**SPECTRUM_WAVELENGTH_TIGHTER_THAN_PARAM**
   Parameters lambda range goes beyond spectrum range. This means that a part of the lambda range specified in the parameters will not be studied.

**MULTI_OBS_ARBITRARY_LSF**
  Happens in the case of multi obs, for which only one lsf is support.
  Only the lsf of the first observation is taken into account and applied
  to all observations.

**NULL_LINES_PROFILE**
   One line profile is null (zero flux) on all its samples: the corresponding line is discarded

**STD_ESTIMATION_FAILED**
   The number of samples available to estimate the RMS of the residual is too low. 

**VELOCITY_FIT_RANGE**
   The lineMeas velocity fit range does not include the input velocity
   (comming from either the previous redhiftSolver stage, input lineMeas catalog).
   The range is thus extended to contain this velocity.


### Error codes

**1 : INTERNAL_ERROR**
  Should not happen, report a bug to amazed-support@lam.fr

**2 : EXTERNAL_LIB_ERROR**
**3 : INVALID_SPECTRUM_WAVELENGTH**
**4 : INVALID_SPECTRUM_FLUX**
**5 : INVALID_NOISE**
**6 : INVALID_WAVELENGTH_RANGE**
**7 : NEGATIVE_CONTINUUMFIT**
**8 : BAD_CONTINUUMFIT**
**9 : NULL_AMPLITUDES**
**10 : PDF_PEAK_NOT_FOUND**
**11 : MAX_AT_BORDER_PDF**
**12 : MISSING_PARAMETER**
**13 : BAD_PARAMETER_VALUE**
**14 : UNKNOWN_ATTRIBUTE**
**15 : BAD_LINECATALOG**
**16 : BAD_LOGSAMPLEDSPECTRUM**
**17 : BAD_COUNTMATCH**
**18 : BAD_TEMPLATECATALOG**
**19 : INVALID_SPECTRUM**
**20 : OVERLAPFRACTION_NOTACCEPTABLE**
**21 : DZ_NOT_COMPUTABLE**
**22 : INCOHERENT_INPUTPARAMETERS**
**23 : BAD_CALZETTICORR**
**24 : CRANGE_VALUE_OUTSIDERANGE**
**25 : CRANGE_VECTBORDERS_OUTSIDERANGE**
**26 : CRANGE_NO_INTERSECTION**
**27 : INVALID_MERIT_VALUES**
  Looping on all templates, could not find one which resulted in a chi2 < INFINITY

**28 : TPL_NAME_EMPTY**
  The template name of a continuum model solution is empty (redshift line model solver).

**29 : EMPTY_LIST**
**30 : LESS_OBSERVED_SAMPLES_THAN_AMPLITUDES_TO_FIT**
**31 : SPECTRUM_CORRECTION_ERROR**
  When correcting input spectrum values (parameter `autoCorrectInput`` set to true), we try for the missing values, to :
  - use the lowest flux abs value
  - use the highest noise value
  However, we were unable to find a min flux value or a max noise value.

**32 : SCOPESTACK_ERROR**
**33 : FLAT_ZPDF**
**34 : NULL_MODEL**
**35 : INVALID_SPECTRUM_INDEX**
**36 : UNKNOWN_AIR_VACUUM_CONVERSION**
**37 : BAD_LINE_TYPE**
**38 : BAD_LINE_FORCE**
**39 : FFT_WITH_PHOTOMETRY_NOTIMPLEMENTED**
**40 : MULTIOBS_WITH_PHOTOMETRY_NOTIMPLEMENTED**
**41 : MISSING_PHOTOMETRIC_DATA**
**42 : MISSING_PHOTOMETRIC_TRANSMISSION**
**43 : PDF_NORMALIZATION_FAILED**
**44 : INSUFFICIENT_TEMPLATE_COVERAGE**
**45 : INSUFFICIENT_LSF_COVERAGE**
**46 : INVALID_LSF**
**47 : SPECTRUM_NOT_LOADED**
**48 : LSF_NOT_LOADED**
**49 : UNALLOWED_DUPLICATES**
**50 : UNSORTED_ARRAY**
**51 : INVALID_DIRECTORY**
**52 : INVALID_FILEPATH**
**53 : INVALID_PARAMETER**
**54 : MISSING_CONFIG_OPTION**
**55 : BAD_FILEFORMAT**
**56 : INCOHERENT_CONFIG_OPTIONS**
**57 : ATTRIBUTE_NOT_SUPPORTED**
**58 : INCOMPATIBLE_PDF_MODELSHAPES**
**59 : UNKNOWN_RESULT_TYPE**
**60 : RELIABILITY_NEEDS_TENSORFLOW**
**61 : OUTPUT_READER_ERROR**
**62 : PYTHON_API_ERROR**
**63 : INVALID_NAME**
**64 : INVALID_FILTER_INSTRUCTION**
**65 : INVALID_FILTER_KEY**
**66 : NO_CLASSIFICATION**
**67 : INVALID_PARAMETER_FILE**
**68 : DUPLICATED_LINES**
**69 : STAGE_NOT_RUN_BECAUSE_OF_PREVIOUS_FAILURE**
**70 : LINE_RATIO_UNKNOWN_LINE**
