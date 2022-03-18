import os
import numpy as np
from pylibamazed.redshift import get_version
from drp_1dpipe import VERSION
from astropy.io import fits
import json
import pandas as pd


class RedshiftCandidates:

    def __init__(self, drp1d_output, spectrum_reader, logger, user_param, calibration_library):
        self.drp1d_output = drp1d_output
        self.spectrum_reader = spectrum_reader
        self.logger = logger
        self.user_param = user_param
        self.calibration_library = calibration_library

    def write_fits(self, output_dir):
        path = "pfsZcandidates-%05d-%05d-%s-%016x-%03d-0x%016x.fits" % (
            self.spectrum_reader.pfs_object_id["catId"],
            self.spectrum_reader.pfs_object_id["tract"],
            self.spectrum_reader.pfs_object_id["patch"],
            self.spectrum_reader.pfs_object_id["objId"],
            self.spectrum_reader.pfs_object_id["nVisit"] % 1000,
            self.spectrum_reader.pfs_object_id["pfsVisitHash"])
        hdul = []
        self.header_to_fits(hdul)
        self.classification_to_fits(hdul)
        self.galaxy_candidates_to_fits(hdul)
        self.object_pdf_to_fits("galaxy", hdul)
        self.object_lines_to_fits("galaxy", hdul)
        self.qso_candidates_to_fits(hdul)
        self.object_pdf_to_fits("qso", hdul)
        self.qso_lines_to_fits("qso", hdul)
        self.star_candidates_to_fits(hdul)
        self.object_pdf_to_fits("star", hdul)

        fits.HDUList(hdul).writeto(os.path.join(output_dir, path),
                                   overwrite=True)

    def header_to_fits(self, hdulist):
        quality_flag = 2 # no linemeas active
        header = [fits.Card('tract', self.spectrum_reader.pfs_object_id["tract"], 'Area of the sky'),
                  fits.Card('patch', self.spectrum_reader.pfs_object_id["patch"], 'Region within tract'),
                  fits.Card('catId', self.spectrum_reader.pfs_object_id["catId"], 'Source of the objId'),
                  fits.Card('objId', self.spectrum_reader.pfs_object_id["objId"], 'Unique ID for object'),
                  fits.Card('nvisit', self.spectrum_reader.pfs_object_id["nVisit"], 'Number of visit'),
                  fits.Card('vHash', self.spectrum_reader.pfs_object_id["pfsVisitHash"], '63-bit SHA-1 list of visits'),
                  fits.Card('CRPIX1',self.spectrum_reader.wl_infos["CRPIX1"],'Pixel coordinate of reference point'),
                  fits.Card('CRVAL1',self.spectrum_reader.wl_infos["CRVAL1"],'[m] Coordinate value at reference point'),
                  fits.Card('CDELT1',self.spectrum_reader.wl_infos["CDELT1"],'[m] Coordinate increment at reference point'),
                  fits.Card('D1D_VER', get_version()[0:7], 'Version of the DRP_1D library'),
                  fits.Card('D1DP_VER', VERSION, 'Version of the DRP_1DPIPE pipeline'),
                  fits.Card('DAMD_VER', self.spectrum_reader.damd_version, 'Version of the data model'),
                  fits.Card('U_PARAM', json.dumps(self.user_param), "User Parameters content, json"),
                  fits.Card('ZWARNING', quality_flag, 'Quality flag')]

        hdr = fits.Header(header)
        primary = fits.PrimaryHDU(header=hdr)
        hdulist.append(primary)

    def get_classification_type(self):
        return self.drp1d_output.get_classification()["Type"].capitalize()

    def classification_to_fits(self, hdulist):
        classification = [fits.Card('CLASS', self.get_classification_type(),
                                    "Spectro classification: GALAXY, QSO, STAR"),
                          fits.Card('P_GALAXY',self.drp1d_output.get_classification()["galaxyProba"],
                                    "Probability to be a galaxy"),
                          fits.Card('P_QSO',self.drp1d_output.get_classification()["qsoProba"],
                                    "Probability to be a QSO"),
                          fits.Card('P_STAR',self.drp1d_output.get_classification()["starProba"],
                                    "Probability to be a star")]
        hdr = fits.Header(classification)
        hdu = fits.BinTableHDU(header=hdr, name="CLASSIFICATION")
        hdulist.append(hdu)

    def galaxy_candidates_to_fits(self, hdulist):
        nb_candidates = self.drp1d_output.get_nb_candidates("galaxy")
        npix = len(self.spectrum_reader.pfs_object.wavelength)
        zcandidates = np.ndarray((nb_candidates,),
                                 dtype=[('CRANK', 'i4'),
                                        ('Z', 'f4'),
                                        ('Z_ERR', 'f4'),
                                        ('Z_PROBA', 'f4'),
                                        ('SUBCLASS', 'S15'),
                                        ('CFILE','S50'),
                                        ('LFILE','S50'),
                                        ('MODELFLUX', 'f4', (npix,))
                                        ])

        for rank in range(nb_candidates):
            zcandidates[rank]['Z'] = self.drp1d_output.get_candidate_data("galaxy", rank, "Redshift")
            zcandidates[rank]['Z_ERR'] = self.drp1d_output.get_candidate_data("galaxy", rank, "RedshiftUncertainty")
            zcandidates[rank]['CRANK'] = rank
            zcandidates[rank]['Z_PROBA'] = self.drp1d_output.get_candidate_data("galaxy", rank, "RedshiftProba")
            zcandidates[rank]['SUBCLASS'] = ''
            zcandidates[rank]['CFILE'] = self.drp1d_output.get_candidate_data("galaxy", rank, "ContinuumName")
            zcandidates[rank]['LFILE'] = self.drp1d_output.get_candidate_data("galaxy", rank, "LinesRatioName")
            zcandidates[rank]['MODELFLUX'] = self._get_model_on_lambda_range("galaxy", rank)

        hdulist.append(fits.BinTableHDU(name='GALAXY_CANDIDATES', data=zcandidates))

    def qso_candidates_to_fits(self, hdulist):
        if "qso" in self.drp1d_output.object_results:
            nb_candidates = self.drp1d_output.get_nb_candidates("qso")
        else:
            nb_candidates = 0
        npix = len(self.spectrum_reader.pfs_object.wavelength)
        zcandidates = np.ndarray((nb_candidates,),
                                 dtype=[('CRANK', 'i4'),
                                        ('Z', 'f4'),
                                        ('Z_ERR', 'f4'),
                                        ('Z_PROBA', 'f4'),
                                        ('SUBCLASS', 'S15'),
                                        ('MODELFLUX', 'f4', (npix,))
                                        ])

        for rank in range(nb_candidates):
            zcandidates[rank]['Z'] = self.drp1d_output.get_candidate_data("qso", rank, "Redshift")
            zcandidates[rank]['Z_ERR'] = self.drp1d_output.get_candidate_data("qso", rank, "RedshiftUncertainty")
            zcandidates[rank]['CRANK'] = rank
            zcandidates[rank]['Z_PROBA'] = self.drp1d_output.get_candidate_data("qso", rank, "RedshiftProba")
            zcandidates[rank]['SUBCLASS'] = ''
            zcandidates[rank]['MODELFLUX'] = self._get_model_on_lambda_range("qso", rank)

        hdulist.append(fits.BinTableHDU(name='QSO_CANDIDATES', data=zcandidates))
        
    def star_candidates_to_fits(self,hdulist):
        if "star" in self.drp1d_output.object_results:
            nb_candidates = self.drp1d_output.get_nb_candidates("star")
        else:
            nb_candidates = 0
        npix = len(self.spectrum_reader.pfs_object.wavelength)
        zcandidates = np.ndarray((nb_candidates,),
                                 dtype=[('CRANK', 'i4'),
                                        ('V', 'f4'),
                                        ('V_ERR', 'f4'),
                                        ('T_PROBA', 'f4'),
                                        ('SUBCLASS', 'S15'),
                                        ('TFILE','S50'),
                                        ('MODELFLUX', 'f4', (npix,))
                                        ])

        for rank in range(nb_candidates):
            zcandidates[rank]['V'] = self.drp1d_output.get_candidate_data("star", rank, "Redshift")
            zcandidates[rank]['V_ERR'] = self.drp1d_output.get_candidate_data("star", rank, "RedshiftUncertainty")
            zcandidates[rank]['CRANK'] = rank
            zcandidates[rank]['T_PROBA'] = self.drp1d_output.get_candidate_data("star", rank, "RedshiftProba")
            zcandidates[rank]['SUBCLASS'] = ''
            zcandidates[rank]['TFILE'] = self.drp1d_output.get_candidate_data("star", rank, "ContinuumName")
            zcandidates[rank]['MODELFLUX'] = self._get_model_on_lambda_range("star", rank)

        hdulist.append(fits.BinTableHDU(name='STAR_CANDIDATES', data=zcandidates))

    def object_pdf_to_fits(self, object_type, hdulist):
        if object_type in self.drp1d_output.object_results:
            ln_pdf = np.float32(self.drp1d_output.object_dataframes[object_type]["pdf"]["PDFProbaLog"])
            pdf_grid = np.float32(self.drp1d_output.object_dataframes[object_type]["pdf"]["PDFZGrid"])
            grid_size = self.drp1d_output.object_dataframes[object_type]["pdf"].index.size
            grid_name = 'REDSHIFT'
            if object_type == "star":
                grid_name = 'VELOCITY'
            zpdf_hdu = np.ndarray(grid_size, 
                                  dtype=[('ln PDF', 'f4'), (grid_name, 'f4')])
            zpdf_hdu['ln PDF']=ln_pdf
            zpdf_hdu[grid_name]=pdf_grid
        else:
            zpdf_hdu = None

        hdulist.append(fits.BinTableHDU(name=object_type.upper()+'_PDF', data=zpdf_hdu))

    def object_lines_to_fits(self, object_type, hdulist):
        fr = self.drp1d_output.object_dataframes[object_type]["linemeas"]
        fr = fr[fr["LinemeasLineLambda"] > 0]
        fr = fr.set_index("LinemeasLineID")
        line_catalog = self.calibration_library.line_catalogs_df[object_type]
        fr = pd.merge(fr, line_catalog[["PfsName", "WaveLength"]], left_index=True, right_index=True)

        nr = fr.index.size
        zlines = np.ndarray((fr.index.size,),
                            dtype=[('LINENAME', 'S15'),
                                   ('LINEWAVE', 'f4'),
                                   ('LINEZ', 'f4'),
                                   ('LINEZ_ERR', 'f4'),
                                   ('LINESIGMA', 'f4'),
                                   ('LINESIGMA_ERR', 'f4'),
                                   ('LINEVEL', 'f4'),
                                   ('LINEVEL_ERR', 'f4'),
                                   ('LINEFLUX', 'f4'),
                                   ('LINEFLUX_ERR', 'f4'),
                                   ('LINEEW', 'f4'),
                                   ('LINEEW_ERR', 'f4'),
                                   ('LINECONTLEVEL', 'f4'),
                                   ('LINECONTLEVEL_ERR', 'f4')])
        zi = 0
        for i in list(fr.index):
            zlines[zi]['LINENAME'] = fr.at[i, "PfsName"]
            zlines[zi]['LINEWAVE'] = fr.at[i, "LinemeasLineLambda"]*0.1
            zlines[zi]['LINEZ'] = self.drp1d_output.get_candidate_data("galaxy", 0, "Redshift" )
            zlines[zi]['LINEZ_ERR'] = self.drp1d_output.get_candidate_data("galaxy", 0, "RedshiftUncertainty")
            zlines[zi]['LINESIGMA'] = -1
            zlines[zi]['LINESIGMA_ERR'] = -1
            zlines[zi]['LINEVEL'] = -1
            zlines[zi]['LINEVEL_ERR'] = -1
            # erg/cm2/s -> 10^-35 W/m2 : erg/cm2/s=10^-7W/cm2=10^-3W/m2 -> *10^-3
            zlines[zi]['LINEFLUX'] = fr.at[i, "LinemeasLineFlux"]*10**-3
            zlines[zi]['LINEFLUX_ERR'] = fr.at[i, "LinemeasLineFluxError"]*10**-3
            zlines[zi]['LINEEW'] = -1
            zlines[zi]['LINEEW_ERR'] = -1
            zlines[zi]['LINECONTLEVEL'] = -1
            zlines[zi]['LINECONTLEVEL_ERR'] = -1
            zi = zi+1

        hdulist.append(fits.BinTableHDU(name=object_type.upper()+"_LINES", data=zlines))

    def qso_lines_to_fits(self, object_type, hdulist):
        zlines = np.ndarray((0,),
                            dtype=[('LINENAME', 'S15'),
                                   ('LINEWAVE', 'f4'),
                                   ('LINEZ', 'f4'),
                                   ('LINEZ_ERR', 'f4'),
                                   ('LINESIGMA', 'f4'),
                                   ('LINESIGMA_ERR', 'f4'),
                                   ('LINEVEL', 'f4'),
                                   ('LINEVEL_ERR', 'f4'),
                                   ('LINEFLUX', 'f4'),
                                   ('LINEFLUX_ERR', 'f4'),
                                   ('LINEEW', 'f4'),
                                   ('LINEEW_ERR', 'f4'),
                                   ('LINECONTLEVEL', 'f4'),
                                   ('LINECONTLEVEL_ERR', 'f4')])
        hdulist.append(fits.BinTableHDU(name=object_type.upper() + "_LINES", data=zlines))

    def _get_model_on_lambda_range(self, object_type, rank):
        model = np.array(self.spectrum_reader.pfs_object.wavelength, dtype=np.float64, copy=True)
        model.fill(np.nan)
        np.place(model, self.spectrum_reader.pfs_object.mask == 0, self.drp1d_output.object_results[object_type]["model"][rank]["ModelFlux"])
        model = np.multiply(np.array(self.spectrum_reader.pfs_object.wavelength) ** 2, np.array(model)) * (1 / 2.99792458) * 10 ** 14
        return np.float32(model)




                  
