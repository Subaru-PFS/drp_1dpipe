import os
import numpy as np
from pylibamazed.redshift import get_version
from pylibamazed.PdfHandler import BuilderPdfHandler
from drp_1dpipe import VERSION
from astropy.io import fits
import json
import pandas as pd
from scipy.constants import speed_of_light


class RedshiftCandidates:

    def __init__(self, drp1d_output, spectrum_storage, logger, user_param, calibration_library):
        self.drp1d_output = drp1d_output
        self.spectrum_storage = spectrum_storage
        self.logger = logger
        self.user_param = user_param
        self.calibration_library = calibration_library

    def write_fits(self, output_dir):
        path = "pfsZcandidates-%05d-%05d-%s-%016x-%03d-0x%016x.fits" % (
            self.spectrum_storage.pfs_object_id["catId"],
            self.spectrum_storage.pfs_object_id["tract"],
            self.spectrum_storage.pfs_object_id["patch"],
            self.spectrum_storage.pfs_object_id["objId"],
            self.spectrum_storage.pfs_object_id["nVisit"] % 1000,
            self.spectrum_storage.pfs_object_id["pfsVisitHash"])
        hdul = []
        params = self.calibration_library.parameters
        object_types = params.get_spectrum_models()
        try:
            self.header_to_fits(hdul)
        except Exception as e:
            raise Exception(f'failed to write header : {e}')
        try:
            if not self.drp1d_output.has_error(None,"classification"):
                self.classification_to_fits(hdul)
            else:
                hdul.append(fits.BinTableHDU(name="CLASSIFICATION"))
        except Exception as e:
            raise Exception(f'failed to write classification : {e}')
        try:            
            has_galaxy= "galaxy" in object_types and params.stage_enabled("galaxy","redshiftSolver")
            if has_galaxy and not self.drp1d_output.has_error("galaxy","redshiftSolver"):
                self.galaxy_candidates_to_fits(hdul)
                self.object_pdf_to_fits("galaxy", hdul)
            else:
                hdul.append(fits.BinTableHDU(name='GALAXY_CANDIDATES'))
                hdul.append(fits.BinTableHDU(name='GALAXY_PDF'))
        except Exception as e:
            raise Exception(f'failed to write galaxy : {e}')
        try:
            has_galaxy_lines=  "galaxy" in object_types and params.stage_enabled("galaxy","lineMeasSolver")
            if has_galaxy_lines and not self.drp1d_output.has_error("galaxy","lineMeasSolver"):
                self.object_lines_to_fits("galaxy", hdul)
            else:
                hdul.append(fits.BinTableHDU(name="GALAXY_LINES"))
        except Exception as e:
            raise Exception(f'failed to write galaxy lines : {e}')
        try:
            has_qso= "qso" in object_types and params.stage_enabled("qso","redshiftSolver")
            if has_qso and not self.drp1d_output.has_error("qso","redshiftSolver"):
                self.qso_candidates_to_fits(hdul)
                self.object_pdf_to_fits("qso", hdul)
            else:
                hdul.append(fits.BinTableHDU(name='QSO_CANDIDATES'))
                hdul.append(fits.BinTableHDU(name='QSO_PDF'))
        except Exception as e:
            raise Exception(f'failed to write qso : {e}')
        try:
            has_qso_lines= "qso" in object_types and params.stage_enabled("qso","lineMeasSolver")
            if has_qso_lines and not self.drp1d_output.has_error("qso","lineMeasSolver"):
                self.object_lines_to_fits("qso", hdul)
            else:
                hdul.append(fits.BinTableHDU(name="QSO_LINES"))
        except Exception as e:
            raise Exception(f'failed to write qso lines : {e}')
        try:
            has_star= "star" in object_types and params.stage_enabled("star","redshiftSolver")            
            if has_star and not self.drp1d_output.has_error("star","redshiftSolver"):
                self.star_candidates_to_fits(hdul)
                self.object_pdf_to_fits("star", hdul)
            else:
                hdul.append(fits.BinTableHDU(name='STAR_CANDIDATES'))
                hdul.append(fits.BinTableHDU(name='STAR_PDF'))
        except Exception as e:
            raise Exception(f'failed to write star : {e}')
        
        fits.HDUList(hdul).writeto(os.path.join(output_dir, path),
                                   overwrite=True)

    def header_to_fits(self, hdulist):
        quality_flag = 2 # no linemeas active
        header = [fits.Card('tract', self.spectrum_storage.pfs_object_id["tract"], 'Area of the sky'),
                  fits.Card('patch', self.spectrum_storage.pfs_object_id["patch"], 'Region within tract'),
                  fits.Card('catId', self.spectrum_storage.pfs_object_id["catId"], 'Source of the objId'),
                  fits.Card('objId', self.spectrum_storage.pfs_object_id["objId"], 'Unique ID for object'),
                  fits.Card('nvisit', self.spectrum_storage.pfs_object_id["nVisit"], 'Number of visit'),
                  fits.Card('vHash', self.spectrum_storage.pfs_object_id["pfsVisitHash"], '63-bit SHA-1 list of visits'),
                  fits.Card('CRPIX1',self.spectrum_storage.spectrum_infos["wl_infos"]["CRPIX1"],'Pixel coordinate of reference point'),
                  fits.Card('CRVAL1',self.spectrum_storage.spectrum_infos["wl_infos"]["CRVAL1"],'[m] Coordinate value at reference point'),
                  fits.Card('CDELT1',self.spectrum_storage.spectrum_infos["wl_infos"]["CDELT1"],'[m] Coordinate increment at reference point'),
                  fits.Card('D1D_VER', get_version()[0:7], 'Version of the DRP_1D library'),
                  fits.Card('D1DP_VER', VERSION, 'Version of the DRP_1DPIPE pipeline'),
                  fits.Card('DAMD_VER', self.spectrum_storage.global_infos["damd_version"], 'Version of the data model'),
                  fits.Card('U_PARAM',
                            json.dumps(self.user_param),
                            "User Parameters content, json")
                  ]
        params = self.calibration_library.parameters
        redshift_methods = params.get_objects_solve_methods()
        header.append(fits.Card(f'hierarch INIT_WARNING',
                                        self.drp1d_output.get_attribute(None,
                                                                        "init_warningFlag",
                                                                        "InitWarningFlags"),
                                        f'Quality flag for spectrum initialization'))
        if self.drp1d_output.has_error(None,"init"):
            message = self.get_error(None,"init","message")
            code = self.get_error(None,"init","code")
        else:
            code = ""
            message = ""
        header.append(fits.Card(f'INIT_ERR',
                                message,
                                f"Error message for spectrum initialization" )
                             )
        header.append(fits.Card(f'hierarch INIT_ERROR',
                                code,
                                f"Error code spectrum initialization" )
                      )
        for ot,meth in redshift_methods.items():
            try:
                header.append(fits.Card(f'hierarch {ot.upper()}_ZWARNING',
                                        self.drp1d_output.get_attribute(ot,
                                                                        "warningFlag",
                                                                        meth+"WarningFlags"),
                                        f'Quality flag for {ot} redshift solver'))
            except Exception as e:
                header.append(fits.Card(f'hierarch {ot.upper()}_ZWARNING',
                                        0,
                                        f'Quality flag for {ot} redshift solver'))
                
        linemeas_methods = params.get_objects_linemeas_methods()
        for ot in linemeas_methods.keys():
            try:
                meth = linemeas_methods[ot]
                if not self.drp1d_output.has_error(ot,"lineMeasSolver"):
                    w = self.drp1d_output.get_attribute(ot,
                                                        "warningFlag",
                                                        f"{meth}WarningFlags")
                else:
                    w = 0
                header.append(fits.Card(f'hierarch {ot.upper()}_LWARNING',w,
                                        f'Quality flag for {ot} linemeas solver'))
                if self.drp1d_output.has_error(ot,"lineMeasSolver"):
                    message = self.drp1d_output.get_error(ot,"lineMeasSolver")["message"]
                    code = self.drp1d_output.get_error(ot,"lineMeasSolver")["code"] 
                else:
                    message = ""
                    code = ""                    
                header.append(fits.Card(f'{ot.upper()[0]}_LERR',
                                        message,
                                        f"Error message {ot} linemeas solver" )
                             )
                header.append(fits.Card(f'hierarch {ot.upper()}_LERROR',
                                        code,
                                        f"Error code {ot} linemeas solver" )
                                  )
            except Exception as e:
                raise Exception(f"Could not write linemeas quality flag for {ot} : {e}")
        header.append(fits.Card(f'hierarch CLASSIFICATION_WARNING',
                                0,
                                # self.drp1d_output.get_attribute(None,
                                #                                 "warningFlag",
                                #                                 "classificationWarningFlags"),
                                f'Quality flag for classification solver'))
        for ot in ["galaxy","qso","star"]:
            if self.drp1d_output.has_error(ot,"redshiftSolver"):
                message = self.drp1d_output.get_error(ot,"redshiftSolver")["message"]
                code = self.drp1d_output.get_error(ot,"redshiftSolver")["code"]
            else:
                message = ""
                code = ""
            header.append(fits.Card(f'hierarch {ot.upper()}_ZERROR',
                                    code,
                                    f"Error code {ot} redshift solver" )
            )
            header.append(fits.Card(f'{ot.upper()[0]}_ZERR',
                                    message,
                                    f"Error message for {ot} redshift solver")
            )
                              
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
                if self.drp1d_output.has_attribute(None,"classification",f"{o}Proba"):
                    o_proba[o] = self.drp1d_output.get_attribute(None,"classification",f"{o}Proba")
                else:
                    o_proba[o] = 0
            except:
                o_proba[o] = 0
                
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
        npix = len(self.spectrum_storage.full_wavelength)
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
            zcandidates[rank]['SUBCLASS'] = self.drp1d_output.get_candidate_data("galaxy", rank, "SubType")
            zcandidates[rank]['CFILE'] = self.drp1d_output.get_candidate_data("galaxy", rank, "ContinuumName")
            zcandidates[rank]['LFILE'] = self.drp1d_output.get_candidate_data("galaxy", rank, "LinesRatioName")
            zcandidates[rank]['MODELFLUX'] = self._get_model_on_lambda_range("galaxy", rank)

        hdulist.append(fits.BinTableHDU(name='GALAXY_CANDIDATES', data=zcandidates))

    def qso_candidates_to_fits(self, hdulist):
        if "qso" in self.drp1d_output.object_results:
            nb_candidates = self.drp1d_output.get_nb_candidates("qso")
        else:
            nb_candidates = 0
        npix = len(self.spectrum_storage.full_wavelength)
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
            zcandidates[rank]['SUBCLASS'] = self.drp1d_output.get_candidate_data("qso", rank, "SubType")
            zcandidates[rank]['MODELFLUX'] = self._get_model_on_lambda_range("qso", rank)

        hdulist.append(fits.BinTableHDU(name='QSO_CANDIDATES', data=zcandidates))
        
    def star_candidates_to_fits(self,hdulist):
        if "star" in self.drp1d_output.object_results:
            nb_candidates = self.drp1d_output.get_nb_candidates("star")
        else:
            nb_candidates = 0
        npix = len(self.spectrum_storage.full_wavelength)
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
            zcandidates[rank]['V'] = self.drp1d_output.get_candidate_data("star", rank, "Redshift") * speed_of_light
            zcandidates[rank]['V_ERR'] = self.drp1d_output.get_candidate_data("star", rank, "RedshiftUncertainty")
            zcandidates[rank]['CRANK'] = rank
            zcandidates[rank]['T_PROBA'] = self.drp1d_output.get_candidate_data("star", rank, "RedshiftProba")
            zcandidates[rank]['SUBCLASS'] = "" # self.drp1d_output.get_candidate_data("star", rank, "ContinuumName").split("_")[0]
            zcandidates[rank]['TFILE'] = self.drp1d_output.get_candidate_data("star", rank, "ContinuumName")
            zcandidates[rank]['MODELFLUX'] = self._get_model_on_lambda_range("star", rank)

        hdulist.append(fits.BinTableHDU(name='STAR_CANDIDATES', data=zcandidates))

    def object_pdf_to_fits(self, object_type, hdulist):
        if object_type in self.drp1d_output.object_results:
            try:
                ln_pdf = np.float32(self.drp1d_output.get_attribute(object_type,"pdf","LogZPdfNative"))
            except Exception as e:
                raise Exception(f"Failed to get {object_type} pdf : {e}")
            builder = BuilderPdfHandler()
            pdfHandler = builder.add_params(self.drp1d_output, object_type, True).build()
            
            pdf_grid = np.float32(pdfHandler.redshifts)
            grid_size = len(pdf_grid)
            grid_name = 'REDSHIFT'
            if object_type == "star":
                grid_name = 'VELOCITY'
                pdf_grid = pdf_grid * speed_of_light
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
        line_catalog = self.calibration_library.line_catalogs_df[object_type]["lineMeasSolve"]
        fr = pd.merge(fr, line_catalog[["Name", "WaveLength"]], left_index=True, right_index=True)


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
            zlines[zi]['LINENAME'] = fr.at[i, "Name"]
            zlines[zi]['LINEWAVE'] = fr.at[i, "LinemeasLineLambda"]*0.1
            zlines[zi]['LINEZ'] = self.drp1d_output.get_candidate_data(object_type, 0, "Redshift" )
            zlines[zi]['LINEZ_ERR'] = self.drp1d_output.get_candidate_data(object_type, 0, "RedshiftUncertainty")
            zlines[zi]['LINESIGMA'] = fr.at[i,"LinemeasLineWidth"]/10.
            zlines[zi]['LINESIGMA_ERR'] = -1
            zlines[zi]['LINEVEL'] = fr.at[i,"LinemeasLineVelocity"]
            zlines[zi]['LINEVEL_ERR'] = -1
            # erg/cm2/s -> 10^-35 W/m2 : erg/cm2/s=10^-7W/cm2=10^-3W/m2 -> *10^-3
            zlines[zi]['LINEFLUX'] = fr.at[i, "LinemeasLineFlux"]*10**-3
            zlines[zi]['LINEFLUX_ERR'] = fr.at[i, "LinemeasLineFluxUncertainty"]*10**-3
            zlines[zi]['LINEEW'] = -1
            zlines[zi]['LINEEW_ERR'] = -1
            zlines[zi]['LINECONTLEVEL'] = fr.at[i,"LinemeasLineContinuumFlux"]
            zlines[zi]['LINECONTLEVEL_ERR'] = fr.at[i,"LinemeasLineContinuumFluxUncertainty"]
            zi = zi+1                                  

        hdulist.append(fits.BinTableHDU(name=object_type.upper()+"_LINES", data=zlines))

    def _get_model_on_lambda_range(self, object_type, rank):
        model = np.array(self.spectrum_storage.full_wavelength, dtype=np.float64, copy=True)
        model.fill(np.nan)
        np.place(model, self.spectrum_storage.mask == 0, self.drp1d_output.object_results[object_type]["model"][rank]["ModelFlux"])
        model = np.multiply(np.array(self.spectrum_storage.full_wavelength) ** 2, np.array(model)) * (1 / 2.99792458) * 10 ** 16
        return np.float32(model)




                  
