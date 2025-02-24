import os
import numpy as np
from pylibamazed.redshift import get_version
from pylibamazed.PdfHandler import BuilderPdfHandler
from drp_1dpipe import VERSION
from astropy.io import fits
import json
import pandas as pd
from scipy.constants import speed_of_light


class RedshiftCoCandidates:

    def __init__(self, drp1d_output, spectrum_storage, logger, user_param, calibration_library):
        self.drp1d_output = drp1d_output
        self.spectrum_storage = spectrum_storage
        self.logger = logger
        self.user_param = user_param
        self.calibration_library = calibration_library
        

    def init_file(self, output_dir):
        self.path = os.path.join(output_dir,
                                 "pfsCoZcandidates-%05d-%05d-%s.fits" % (
                                     self.spectrum_storage.pfs_object_id["catId"],
                                     self.spectrum_storage.pfs_object_id["tract"],
                                     self.spectrum_storage.pfs_object_id["patch"]))
        hdul = []
        try:
            self.header_to_fits(hdul)
        except Exception as e:
            raise Exception(f'failed to write header : {e}')

        target_cols = fits.ColDefs([
            fits.Column(name="targetId", format="I", array=np.array([], dtype=np.int16)),  # Target identifier
            fits.Column(name="catId", format="J", array=np.array([], dtype=np.int32)),  # Catalog identifier
            fits.Column(name="tract", format="J", array=np.array([], dtype=np.int32)),  # Tract identifier
            fits.Column(name="patch", format="A3"),  # Patch identifier (String)
            fits.Column(name="objId", format="K", array=np.array([], dtype=np.int64)),  # Object identifier
            fits.Column(name="nVisit", format="K", array=np.array([], dtype=np.int64)),  # Number of visits
            fits.Column(name="visitHash", format="K", array=np.array([], dtype=np.int64)),  # 63-bit SHA-1 list of visits
            fits.Column(name="ra", format="D", array=np.array([], dtype=np.float64)),  # Right Ascension (ICRS)
            fits.Column(name="dec", format="D", array=np.array([], dtype=np.float64)),  # Declination (ICRS)
            fits.Column(name="targetType", format="I", array=np.array([], dtype=np.int16))  # Target type
        ])
        hdul.append(fits.BinTableHDU.from_columns(target_cols, name="TARGET"))
        # Define WARNINGS binary table columns
        warnings_cols = fits.ColDefs([
            fits.Column(name="targetId", format="I", array=np.array([], dtype=np.int16)), 
            fits.Column(name="initWarning", format="K", array=np.array([], dtype=np.int64)), 
            fits.Column(name="galaxyZWarning", format="K", array=np.array([], dtype=np.int64)), 
            fits.Column(name="QSOZWarning", format="K", array=np.array([], dtype=np.int64)), 
            fits.Column(name="starZWarning", format="K", array=np.array([], dtype=np.int64)), 
            fits.Column(name="galaxyLWarning", format="K", array=np.array([], dtype=np.int64)), 
            fits.Column(name="QSOLWarning", format="K", array=np.array([], dtype=np.int64)), 
            fits.Column(name="classificationWarning", format="K", array=np.array([], dtype=np.int64))
        ])
        hdul.append(fits.BinTableHDU.from_columns(warning_cols, name="WARNINGS"))

        # Define ERRORS binary table columns
        errors_cols = fits.ColDefs([
            fits.Column(name="targetId", format="I", array=np.array([], dtype=np.int16)), 
            fits.Column(name="GalaxyZError", format="K", array=np.array([], dtype=np.int64)), 
            fits.Column(name="QSOZError", format="K", array=np.array([], dtype=np.int64)), 
            fits.Column(name="StarZError", format="K", array=np.array([], dtype=np.int64)), 
            fits.Column(name="GalaxyLError", format="K", array=np.array([], dtype=np.int64)), 
            fits.Column(name="QSOLError", format="K", array=np.array([], dtype=np.int64)), 
            fits.Column(name="InitError", format="K", array=np.array([], dtype=np.int64)), 
            fits.Column(name="ClassificationError", format="K", array=np.array([], dtype=np.int64)), 
            fits.Column(name="GalaxyZErrorMessage", format="100A", array=np.array([], dtype="S100")),
            fits.Column(name="QSOZErrorMessage", format="100A", array=np.array([], dtype="S100")),
            fits.Column(name="StarZErrorMessage", format="100A", array=np.array([], dtype="S100")),
            fits.Column(name="GalaxyLErrorMessage", format="100A", array=np.array([], dtype="S100")),
            fits.Column(name="QSOLErrorMessage", format="100A", array=np.array([], dtype="S100")),
            fits.Column(name="InitErrorMessage", format="100A", array=np.array([], dtype="S100"))
        ])
        hdul.append(fits.BinTableHDU.from_columns(warning_cols, name="ERRORS"))

        # Define CLASSIFICATION binary table columns
        classification_cols = fits.ColDefs([
            fits.Column(name="targetId", format="I", array=np.array([], dtype=np.int16)),  
            fits.Column(name="class", format="10A", array=np.array([], dtype="S10")),  
            fits.Column(name="probaGalaxy", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="probaQSO", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="probaStar", format="E", array=np.array([], dtype=np.float32))
        ])
        hdul.append(fits.BinTableHDU.from_columns(warning_cols, name="CLASSIFICATION"))

        # Define GALAXY_CANDIDATES binary table columns
        galaxy_candidates_cols = fits.ColDefs([
            fits.Column(name="targetId", format="I", array=np.array([], dtype=np.int16)), 
            fits.Column(name="cRank", format="J", array=np.array([], dtype=np.int32)), 
            fits.Column(name="Z", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="ZErr", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="ZProba", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="SubClass", format="20A", array=np.array([], dtype="S20")),  
            fits.Column(name="ContinuumFile", format="50A", array=np.array([], dtype="S50")),  
            fits.Column(name="LineCatalogRatioFile", format="50A", array=np.array([], dtype="S50"))
        ])
        hdul.append(fits.BinTableHDU.from_columns(warning_cols, name="GALAXY_CANDIDATES"))

        # Define GALAXY_LINES binary table columns
        galaxy_lines_cols = fits.ColDefs([
            fits.Column(name="targetId", format="I", array=np.array([], dtype=np.int16)),  
            fits.Column(name="LineName", format="20A", array=np.array([], dtype="S20")),  
            fits.Column(name="LineWave", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="LineZ", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="LineZError", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="LineSigma", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="LineSigmaError", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="LineVelocity", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="LineVelocityError", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="LineFlux", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="LineFluxError", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="LineEW", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="LineEWError", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="LineContinuumLevel", format="E", array=np.array([], dtype=np.float32)),  
            fits.Column(name="LineContinuumLevelError", format="E", array=np.array([], dtype=np.float32))
        ])
        
        hdul.append(fits.BinTableHDU.from_columns(galaxy_lines_cols, name="GALAXY_LINES" ))

        fits.HDUList(hdul).writeto(self.path)

        
        
    def write_fits(self, output_dir):
        path = "pfsCoZcandidates-%05d-%05d-%s.fits" % (
            self.spectrum_storage.pfs_object_id["catId"],
            self.spectrum_storage.pfs_object_id["tract"],
            self.spectrum_storage.pfs_object_id["patch"])
        hdul = []
        params = self.calibration_library.parameters
        object_types = params.get_spectrum_models()
        try:
            self.header_to_fits(hdul)
        except Exception as e:
            raise Exception(f'failed to write header : {e}')
        try:
            self.add_target(hdul)
        try:
            if not self.drp1d_output.has_error(None,"classification"):
                self.add_classification(hdul)
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
        header = [fits.Card('D1D_VER', get_version()[0:7], 'Version of the DRP_1D library'),
                  fits.Card('D1DP_VER', VERSION, 'Version of the DRP_1DPIPE pipeline'),
                  fits.Card('DAMD_VER', self.spectrum_storage.global_infos["damd_version"], 'Version of the data model'),
                  fits.Card('U_PARAM',
                            json.dumps(self.user_param),
                            "User Parameters content, json")
                  ]
        params = self.calibration_library.parameters

    def add_target(self):
        with fits.open(self.path,"update") as hdulist:
            targets = hdulist["TARGET"].data
            targetId = len(targets)
            pfsObjectId = self.spectrum_storage.pfs_object_id
            new_targets = np.append(np.array([(targetId,
                                               pfsObjectId["catId"],
                                               pfsObjectId["tract"],
                                               pfsObjectId["patch"],
                                               pfsObjectId["objId"],
                                               pfsObjectId["nVisit"],
                                               pfsObjectId["pfsVisitHash"],
                                               self.spectrum_storage.spectrum_infos["RA"],
                                               self.spectrum_storage.spectrum_infos["DEC"],
                                               self.spectrum_storage.spectrum_infos["targetType"])],
                                        dtype=targets.dtype))
            hdulist["TARGET"].data=new_targets
            hdulist.flush()
                
    def add_warnings(self, hdulist):
        redshift_methods = params.get_objects_solve_methods()
        header.append(fits.Card(f'hierarch INIT_WARNING',
                                        self.drp1d_output.get_attribute(None,
                                                                        "init_warningFlag",
                                                                        "InitWarningFlags"),
                                        f'Quality flag for spectrum initialization'))
        if self.drp1d_output.has_error(None,"init"):
            message = self.drp1d_output.get_error(None,"init","message")
            code = self.drp1d_output.get_error(None,"init","code")
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
                
        classification = [(self.get_classification_type(),
                           o_proba["galaxy"],
                           o_proba["qso"],
                           o_proba["star"])]
        hdu = hdulist["CLASSIFICATION"]
        current_table = hdu.data
        new_table = np.append(current_table,classification)            
        hdu = fits.BinTableHDU(header=hdu.header,
                               data=new_table)
        hdulist["CLASSIFICATION"]=hdu
        hdulist.flush()

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
            zcandidates[rank]['V_ERR'] = self.drp1d_output.get_candidate_data("star", rank, "RedshiftUncertainty") * speed_of_light
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
        z = self.drp1d_output.get_attribute(object_type, "linemeas_parameters", "LinemeasRedshift" )
        for i in list(fr.index):
            zlines[zi]['LINENAME'] = fr.at[i, "Name"]
            zlines[zi]['LINEWAVE'] = fr.at[i, "LinemeasLineLambda"]*0.1
            offset = fr.at[i, "LinemeasLineOffset"]
            zlines[zi]['LINEZ'] = z + offset/speed_of_light + z*offset/speed_of_light
            zlines[zi]['LINEZ_ERR'] = fr.at[i, "LinemeasLineOffsetUncertainty"]/speed_of_light
            zlines[zi]['LINESIGMA'] = fr.at[i,"LinemeasLineWidth"]/10.
            zlines[zi]['LINESIGMA_ERR'] = -1
            zlines[zi]['LINEVEL'] = fr.at[i,"LinemeasLineVelocity"]
            zlines[zi]['LINEVEL_ERR'] = fr.at[i,"LinemeasLineVelocityUncertainty"]
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




                  
