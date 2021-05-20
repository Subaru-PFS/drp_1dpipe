import os
import logging

from pylibamazed.redshift import (CSpectrumSpectralAxis,
                                  CSpectrumFluxAxis_withSpectrum,
                                  CTemplate, CTemplateCatalog)
from astropy.io import fits
import numpy as np


class FitsTemplateCatalog(CTemplateCatalog):
    def Load(self, path):
        """Load a template catalog from a fits file"""
        hdul = fits.open(path)
        for spectrum in hdul[1:]:
            name = spectrum.header['EXTNAME']
            category = spectrum.header['CATEGORY']
            print('Loading {} template {}'.format(category, name))

            wavel = spectrum.data.field('WAVE')
            spectralaxis = CSpectrumSpectralAxis(wavel)

            flux = spectrum.data.field('FLUX')
            signal = CSpectrumFluxAxis_withSpectrum(flux)
            template = CTemplate(name, category, spectralaxis, signal)
            self.Add(template)


class DirectoryTemplateCatalog(CTemplateCatalog):
    """Template catalog retrieved from directory

    """
    def Load(self, path):
        """Load a template catalog from a directory

        Parameters
        ----------
        path : str
            Path to template directory

        Notes
        -----
        The template catalog must contain category direcrtories (at least
        one category). Template files are located in category directory
        and format as two columns text file.

        """
        logger = logging.getLogger("process_spectra_worker")

        # Template directory contains category directories
        categories = os.listdir(path)
        if not categories:
            raise ValueError("Not template category directory found in {}".format(path))

        for category in categories:
            category_path = os.path.join(path, category)
            # Category directory contains template files
            template_file_list = os.listdir(category_path)

            if not template_file_list:
                raise ValueError("Not template file found in {}".format(category_path))

            for template_filename in template_file_list:
                file_path = os.path.join(category_path, template_filename)
                # Template file is a two columns text file
                try:
                    data = np.loadtxt(file_path, unpack=True)
                except Exception as e:
                    logger.error("Unable to read template file {}".format(file_path))
                    raise e
                wavelength = data[0]
                flux = data[1]

                spectralaxis = CSpectrumSpectralAxis(wavelength)
                signal = CSpectrumFluxAxis_withSpectrum(flux)
                template = CTemplate(template_filename, category,
                                     spectralaxis, signal)
                self.Add(template)
