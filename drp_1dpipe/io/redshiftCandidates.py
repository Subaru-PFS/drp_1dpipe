import os
import numpy as np
from pylibamazed.redshift import get_version
from pylibamazed.PdfHandler import buildPdfHandler
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
        if not self.drp1d_output.has_error(None,"classification"):
            self.classification_to_fits(hdul)
        if not self.drp1d_output.has_error("galaxy","redshift_solver"):
            self.galaxy_candidates_to_fits(hdul)
            self.object_pdf_to_fits("galaxy", hdul)
        if not self.drp1d_output.has_error("galaxy","linemeas_solver"):
            try:
                self.object_lines_to_fits("galaxy", hdul)
            except Exception as e:
                raise Exception(f"Could not write line meas, available datasets are {self.drp1d_output.object_results['galaxy'].keys()} : {e}")
        if not self.drp1d_output.has_error("qso","redshift_solver"):
            self.qso_candidates_to_fits(hdul)
            self.object_pdf_to_fits("qso", hdul)
        if not self.drp1d_output.has_error("qso","linemeas_solver"):
            self.qso_lines_to_fits("qso", hdul)
        if not self.drp1d_output.has_error("star","redshift_solver"):    
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
                  fits.Card('U_PARAM', json.dumps(self.user_param), "User Parameters content, json")
                  ]
        params = self.calibration_library.parameters
        for ot in ["galaxy","qso","star"]:
            meth = params[ot]["method"]
            try:
                header.append(fits.Card(f'hierarch {ot.upper()}_ZWARNING',
                                        self.drp1d_output.get_attribute(ot,
                                                                        "warningFlag",
                                                                        meth+"WarningFlags"),
                                        f'Quality flag for {ot} redshift solver'))
            except Exception as e:
                raise Exception(f"Could not write quality flag for {ot} and {meth} : {e}") 
            try:
                if params[ot]["linemeas_method"]:
                    if not self.drp1d_output.has_error(ot,"linemeas_solver"):
                        w = self.drp1d_output.get_attribute(ot,
                                                            "warningFlag",
                                                            "LineMeasSolveWarningFlags")
                    else:
                        w = 0
                    header.append(fits.Card(f'hierarch {ot.upper()}_LWARNING',w,
                                            f'Quality flag for {ot} linemeas solver'))
            except Exception as e:
                raise Exception(f"Could not write linemeas quality flag for {ot} : {e}")
        header.append(fits.Card(f'hierarch CLASSIFICATION_WARNING',
                                0,
                                # self.drp1d_output.get_attribute(None,
                                #                                 "warningFlag",
                                #                                 "classificationWarningFlags"),
                                f'Quality flag for classification solver'))
        for ot in ["galaxy","qso","star"]:
            if self.drp1d_output.has_error(ot,"redshift_solver"):
                header.append(fits.Card(f'hierarch {ot.upper()}_ZERROR',
                                        self.drp1d_output.get_error(ot,"redshift_solver")["code"])
                              )
            else:
                header.append(fits.Card(f'hierarch {ot.upper()}_ZERROR',""))
            if params[ot]["linemeas_method"]:
                if self.drp1d_output.has_error(ot,"linemeas_solver"):
                    header.append(fits.Card(f'hierarch {ot.upper()}_LERROR',
                                            self.drp1d_output.get_error(ot,"linemeas_solver")["code"])
                                  )
                else:
                    header.append(fits.Card(f'hierarch {ot.upper()}_LERROR',""))
        if self.drp1d_output.has_error(None,"classification"):
            header.append(fits.Card(f'hierarch CLASSIFICATION_ERROR',
                                    self.drp1d_output.get_error(None,"classification")["code"])
                                  )
        else:
            header.append(fits.Card(f'hierarch CLASSIFICATION_ERROR',
                                    "")
                          )
                
        hdr = fits.Header(header)
        primary = fits.PrimaryHDU(header=hdr)
        hdulist.append(primary)

    def get_classification_type(self):
        try:
            return self.drp1d_output.get_attribute(None,"classification","Type").upper()
        except Exception as e:
            return ""
        
    def classification_to_fits(self, hdulist):
        
        o_proba = dict()
        classification = ""
        for o in ["galaxy","star","qso"]:
            try:
                o_proba[o] = self.drp1d_output.get_attribute(None,"classification",f"{o}Proba")
            except Exception as e:
                o_proba[o]= 0
        
        classification = [fits.Card('CLASS', self.get_classification_type(),
                                    "Spectro classification: GALAXY, QSO, STAR"),
                          fits.Card('P_GALAXY',o_proba["galaxy"],
                                    "Probability to be a galaxy"),
                          fits.Card('P_QSO',o_proba["qso"],
                                    "Probability to be a QSO"),
                          fits.Card('P_STAR',o_proba["star"],
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
            try:
                ln_pdf = np.float32(self.drp1d_output.get_attribute(object_type,"pdf","PDFProbaLog"))
            except Exception as e:
                raise Exception(f"Failed to get {object_type} pdf : {e}")
            pdfHandler = buildPdfHandler(self.drp1d_output, object_type, True)
            
            pdf_grid = np.float32(pdfHandler.redshifts)
            grid_size = len(pdf_grid)
            grid_name = 'REDSHIFT'
            if object_type == "star":
                grid_name = 'VELOCITY'
            zpdf_hdu = np.ndarray(grid_size, 
                                  dtype=[('ln PDF', 'f4'), (grid_name, 'f4')])
            zpdf_hdu['ln PDF']=pdfHandler.valProbaLog
            zpdf_hdu[grid_name]=pdf_grid
        else:
            zpdf_hdu = None

        hdulist.append(fits.BinTableHDU(name=object_type.upper()+'_PDF', data=zpdf_hdu))

    def object_lines_to_fits(self, object_type, hdulist):
        fr = pd.DataFrame(self.drp1d_output.get_dataset(object_type, "linemeas"))
        fr = fr[fr["LinemeasLineLambda"] > 0]
        fr = fr.set_index("LinemeasLineID")
        line_catalog = self.calibration_library.line_catalogs_df[object_type]["LineMeasSolve"]
        fr = pd.merge(fr, line_catalog[["PfsName", "WaveLength"]], left_index=True, right_index=True)
        fr = fr[fr["PfsName"] != "None"]

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
            zlines[zi]['LINEZ'] = self.drp1d_output.get_candidate_data(object_type, 0, "Redshift" )
            zlines[zi]['LINEZ_ERR'] = self.drp1d_output.get_candidate_data(object_type, 0, "RedshiftUncertainty")
            zlines[zi]['LINESIGMA'] = fr.at[i,"LinemeasLineWidth"]/10.
            zlines[zi]['LINESIGMA_ERR'] = -1
            zlines[zi]['LINEVEL'] = -1
            zlines[zi]['LINEVEL_ERR'] = -1
            # erg/cm2/s -> 10^-35 W/m2 : erg/cm2/s=10^-7W/cm2=10^-3W/m2 -> *10^-3
            zlines[zi]['LINEFLUX'] = fr.at[i, "LinemeasLineFlux"]*10**-3
            zlines[zi]['LINEFLUX_ERR'] = fr.at[i, "LinemeasLineFluxError"]*10**-3
            zlines[zi]['LINEEW'] = -1
            zlines[zi]['LINEEW_ERR'] = -1
            zlines[zi]['LINECONTLEVEL'] = fr.at[i,"LinemeasLineCenterContinuumFlux"]
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




                  
