System overview
===============

Background
----------

The drp-1D is in charge of measuring the redshift of galaxies observed with the
PFS spectrograph. In addition to the redshift an estimate of the accuracy and
reliability of the measurements will be provided. To complete the process, a
measure of several spectral feature is performed.

Basic design and context
------------------------

The drp-1D is defined as a second level processing step and stands after the
first level processing step drp-2D from which it gets the spectra to be
processed.

Associated to sources input spectra, drp-1D produces 5 main outputs: the
redshift, the reliability of redshift, the lines and spectral features
measurement, the PDF and the source classification.

Drp-1D is looking for spectral signatures that can be compared to reference
templates or tables; the ratio in wavelength between the observed spectral
features and their corresponding rest-frame counterparts gives the redshift

λobs/λrest = (1+z)

with λobs the observed wavelength of a spectral feature, λrest the
corresponding rest wavelength (e.g. Hα), and z the redshift of interest. The
main algorithm to be executed is a cross-correlation between the observed PSF
spectrum and a set of reference templates/models. In short, the best fit is
expected to provide the best redshift.

The secondary task of drp-1D is to produce observational and rest-frame
measurements of the spectral features for all the PSF spectra for which a
reasonably measured redshift is provided.

Finally, the third task is to provide a spectral classification of the object
processed.

My update.
