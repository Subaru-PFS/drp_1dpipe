import os
import numpy as np
from pylibamazed.redshift import get_version, ErrorCode
from pylibamazed.PdfHandler import BuilderPdfHandler,get_final_regular_z_grid
from drp_1dpipe import version as drp_1dpipe_version

from astropy.io import fits
import json
import pandas as pd
from scipy.constants import speed_of_light
import logging


def init_output_file(output_dir, catId, user_param, damd_version,stella_version, obs_pfs_version, parameters, wl_size):
    path = os.path.join(output_dir,
                        "pfsCoZcandidates-%05d.fits" % (catId))
    hdul = []
    try:
        header = [fits.Card('D1D_VER', get_version(), 'Version of the DRP_1D library'),
                  fits.Card('D1DP_VER', drp_1dpipe_version, 'Version of the DRP_1DPIPE pipeline'),
                  fits.Card('DAMD_VER', damd_version, 'Version of the data model'),
                  fits.Card('D2D_VER', stella_version, 'Version of stella'),
                  fits.Card('OBS_VER', obs_pfs_version, 'Version of obs pfs'),
                  fits.Card('U_PARAM',
                            json.dumps(user_param),
                            "User Parameters content, json")
                  ]
        hdr = fits.Header(header)
        primary = fits.PrimaryHDU(header=hdr)
        hdul.append(primary)

    except Exception as e:
        raise Exception(f'failed to init output file {path} : {e}')

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
    hdul.append(fits.BinTableHDU.from_columns(warnings_cols, name="WARNINGS"))

    # Define ERRORS binary table columns
    errors_cols = fits.ColDefs([
        fits.Column(name="targetId", format="I", array=np.array([], dtype=np.int16)), 
        fits.Column(name="initError", format="K", array=np.array([], dtype=np.int64)),
        fits.Column(name="galaxyZError", format="K", array=np.array([], dtype=np.int64)), 
        fits.Column(name="QSOZError", format="K", array=np.array([], dtype=np.int64)), 
        fits.Column(name="starZError", format="K", array=np.array([], dtype=np.int64)), 
        fits.Column(name="galaxyLError", format="K", array=np.array([], dtype=np.int64)), 
        fits.Column(name="QSOLError", format="K", array=np.array([], dtype=np.int64)), 
        fits.Column(name="classificationError", format="K", array=np.array([], dtype=np.int64)),
        
        fits.Column(name="initErrorMessage", format="100A", array=np.array([], dtype="S100")),
        fits.Column(name="galaxyZErrorMessage", format="100A", array=np.array([], dtype="S100")),
        fits.Column(name="QSOZErrorMessage", format="100A", array=np.array([], dtype="S100")),
        fits.Column(name="starZErrorMessage", format="100A", array=np.array([], dtype="S100")),
        fits.Column(name="galaxyLErrorMessage", format="100A", array=np.array([], dtype="S100")),
        fits.Column(name="QSOLErrorMessage", format="100A", array=np.array([], dtype="S100"))
    ])
    hdul.append(fits.BinTableHDU.from_columns(errors_cols, name="ERRORS"))

    # Define CLASSIFICATION binary table columns
    classification_cols = fits.ColDefs([
        fits.Column(name="targetId", format="I", array=np.array([], dtype=np.int16)),  
        fits.Column(name="class", format="10A", array=np.array([], dtype="S10")),  
        fits.Column(name="probaGalaxy", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="probaQSO", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="probaStar", format="E", array=np.array([], dtype=np.float32))
    ])
    hdul.append(fits.BinTableHDU.from_columns(classification_cols, name="CLASSIFICATION"))

    # Define GALAXY_CANDIDATES binary table columns
    galaxy_candidates_cols = fits.ColDefs([
        fits.Column(name="targetId", format="I", array=np.array([], dtype=np.int16)), 
        fits.Column(name="cRank", format="J", array=np.array([], dtype=np.int32)), 
        fits.Column(name="redshift", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="redshiftError", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="redshiftProba", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="subClass", format="20A", array=np.array([], dtype="S20")),  
        fits.Column(name="continuumFile", format="50A", array=np.array([], dtype="S50")),  
        fits.Column(name="lineCatalogRatioFile", format="50A", array=np.array([], dtype="S50")),
        fits.Column(name="modelId", format="I", array=np.array([], dtype=np.int16))
    ])
    hdul.append(fits.BinTableHDU.from_columns(galaxy_candidates_cols, name="GALAXY_CANDIDATES"))

    
    empty_models = np.empty((0,wl_size),dtype=np.float32)
    hdul.append(fits.ImageHDU(name="GALAXY_MODELS",data=empty_models))

    zgrid = get_final_regular_z_grid("galaxy", parameters)
    hdul.append(fits.BinTableHDU.from_columns([fits.Column(name="redshift",
                                                          format="E",
                                                           array=zgrid)
                                               ],
                                              name="GALAXY_REDSHIFT_GRID" ))
    empty_pdf = np.empty((0,len(zgrid)),dtype=np.float32)
    hdul.append(fits.ImageHDU(name="GALAXY_LN_PDF",data=empty_pdf))

    # Define GALAXY_LINES binary table columns
    galaxy_lines_cols = fits.ColDefs([
        fits.Column(name="targetId", format="I", array=np.array([], dtype=np.int16)),  
        fits.Column(name="lineName", format="20A", array=np.array([], dtype="S20")),  
        fits.Column(name="lineWave", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="lineZ", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="lineZError", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="lineSigma", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="lineSigmaError", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="lineVelocity", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="lineVelocityError", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="lineFlux", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="lineFluxError", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="lineEW", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="lineEWError", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="lineContinuumLevel", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="lineContinuumLevelError", format="E", array=np.array([], dtype=np.float32))
    ])
    
    hdul.append(fits.BinTableHDU.from_columns(galaxy_lines_cols, name="GALAXY_LINES" ))

    hdul.append(fits.BinTableHDU.from_columns(galaxy_candidates_cols, name="QSO_CANDIDATES"))
   
    hdul.append(fits.ImageHDU(name="QSO_MODELS",data=empty_models))
    zgrid = get_final_regular_z_grid("qso", parameters)
    hdul.append(fits.BinTableHDU.from_columns([fits.Column(name="redshift",
                                                          format="E",
                                                           array=zgrid)
                                               ],
                                              name="QSO_REDSHIFT_GRID" ))
    empty_pdf = np.empty((0,len(zgrid)),dtype=np.float32)
    hdul.append(fits.ImageHDU(name="QSO_LN_PDF",data=empty_pdf))
    hdul.append(fits.BinTableHDU.from_columns(galaxy_lines_cols, name="QSO_LINES" ))

    star_candidates_cols = fits.ColDefs([
        fits.Column(name="targetId", format="I", array=np.array([], dtype=np.int16)), 
        fits.Column(name="cRank", format="J", array=np.array([], dtype=np.int32)), 
        fits.Column(name="velocity", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="velocityError", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="templateProba", format="E", array=np.array([], dtype=np.float32)),  
        fits.Column(name="subClass", format="20A", array=np.array([], dtype="S20")),  
        fits.Column(name="templateFile", format="50A", array=np.array([], dtype="S50")),  
        fits.Column(name="modelId", format="I", array=np.array([], dtype=np.int16))
    ])
    hdul.append(fits.BinTableHDU.from_columns(star_candidates_cols, name="STAR_CANDIDATES"))
    
    hdul.append(fits.ImageHDU(name="STAR_MODELS",data=empty_models))

    zgrid = np.array(get_final_regular_z_grid("star", parameters)) * speed_of_light
    hdul.append(fits.BinTableHDU.from_columns([fits.Column(name="velocity",
                                                          format="E",
                                                           array=zgrid)
                                               ],
                                              name="STAR_VELOCITY_GRID" ))
    empty_pdf = np.empty((0,len(zgrid)),dtype=np.float32)
    hdul.append(fits.ImageHDU(name="STAR_LN_PDF",data=empty_pdf))
    quality_columns = fits.ColDefs([
        fits.Column(name="targetId", format="I", array=np.array([], dtype=np.int16)), 
        fits.Column(name="galaxyContinuumReducedChiSquare",
                    format="E",
                    array=np.array([],dtype=np.float32))
    ])
#    hdul.append(fits.BinTableHDU.from_columns(quality_columns, name="QUALITY"))
    fits.HDUList(hdul).writeto(path) 

class RedshiftCoCandidates:

    def __init__(self, drp1d_output, spectrum_storage, logger, calibration_library):
        self.drp1d_output = drp1d_output
        self.spectrum_storage = spectrum_storage
        self.logger = logger
        self.calibration_library = calibration_library
        self.hdulist = None
        
    def write_fits(self, output_dir):

        params = self.calibration_library.parameters
        object_types = params.get_spectrum_models()
        self.path = os.path.join(output_dir, "pfsCoZcandidates-%05d.fits" % (
            self.spectrum_storage.pfs_object_id["catId"]))
        objId = self.spectrum_storage.pfs_object_id["objId"]
        self.logger.log(logging.INFO,f"add data to {self.path} from {objId}")
        
        self.hdulist = fits.open(self.path, "update")
        try:
            targetId = self.add_target()
        except Exception as e:
            raise Exception(f'failed to add target : {e}')
        try:
            id_ = self.add_warnings()
        except Exception as e:
            raise Exception(f'failed to add warnings : {e}')
        if targetId != id_:
            raise Exception(f'Incoherent warning targetId : {targetId} from target hdu with new id {id_}')
        try:
            id_ = self.add_errors()
        except Exception as e:
            raise Exception(f'failed to add errors : {e}')
        if targetId != id_:
            raise Exception(f'Incoherent error targetId : {targetId} from target hdu with new id {id_}')    
        try:
            id_ = self.add_classification()
        except Exception as e:
            raise Exception(f'failed to write classification : {e}')
        if targetId != id_:
            raise Exception(f'Incoherent classification targetId : {targetId} from target hdu with new id {id_}')
        try:            
            has_galaxy= "galaxy" in object_types and params.stage_enabled("galaxy","redshiftSolver")
            if has_galaxy and not self.drp1d_output.has_error("galaxy","redshiftSolver"):
                self.add_object_candidates("galaxy", targetId)
                self.add_model("galaxy")
            self.add_object_pdf("galaxy")
        except Exception as e:
            raise Exception(f'failed to write galaxy : {e}')
        try:
            has_galaxy_lines=  "galaxy" in object_types and params.stage_enabled("galaxy","lineMeasSolver")
            if has_galaxy_lines and not self.drp1d_output.has_error("galaxy","lineMeasSolver"):
                self.add_object_lines("galaxy", targetId)
        except Exception as e:
            raise Exception(f'failed to write galaxy lines : {e}')
        

        try:
            has_qso= "qso" in object_types and params.stage_enabled("qso","redshiftSolver")
            if has_qso and not self.drp1d_output.has_error("qso","redshiftSolver"):
                self.add_object_candidates("qso", targetId)
                self.add_model("qso")
            self.add_object_pdf("qso")
        except Exception as e:
            raise Exception(f'failed to write qso : {e}')
        try:
            has_qso_lines= "qso" in object_types and params.stage_enabled("qso","lineMeasSolver")
            if has_qso_lines and not self.drp1d_output.has_error("qso","lineMeasSolver"):
                self.add_object_lines("qso",targetId)
        except Exception as e:
            raise Exception(f'failed to write qso lines : {e}')
        try:
            has_star= "star" in object_types and params.stage_enabled("star","redshiftSolver")            
            if has_star and not self.drp1d_output.has_error("star","redshiftSolver"):
                self.add_star_candidates(targetId)
                self.add_model("star")
            self.add_object_pdf("star")
        except Exception as e:
            raise Exception(f'failed to write star : {e}')

        #self.add_quality(self)
        self.hdulist.flush()
        self.hdulist.close()
        
    def add_line_to_hdu(self, hdu_name, line):
        #self.logger.log(logging.INFO,f"add {line} to {hdu_name}") 
        hdu_data = self.hdulist[hdu_name].data
        targetId = len(hdu_data)
        new_data = np.append(hdu_data,
                             np.array([tuple([targetId]+line)],
                                      dtype=hdu_data.dtype)
                             )
        self.hdulist[hdu_name].data = new_data
#        self.hdulist.flush()
        return targetId

    def add_lines_to_hdu(self, hdu_name, lines):
        #self.logger.log(logging.INFO,f"add {lines} to {hdu_name}")
        hdu_data = self.hdulist[hdu_name].data
        new_data = np.append(hdu_data,
                             lines,
                             )
        self.hdulist[hdu_name].data = new_data
#        self.hdulist.flush()    

    def add_array_to_image_hdu(self, hdu_name, array):
        #self.logger.log(logging.INFO,f"add {array} to {hdu_name}")

        hdu_data = self.hdulist[hdu_name].data
        new_data = np.vstack([hdu_data,
                             np.array([array],dtype=np.float32)
                                      ])
        self.hdulist[hdu_name].data = new_data
#        self.hdulist.flush()
        
    def add_target(self):
        pfsObjectId = self.spectrum_storage.pfs_object_id
        new_target = [ pfsObjectId["catId"],
                       pfsObjectId["tract"],
                       pfsObjectId["patch"],
                       pfsObjectId["objId"],
                       pfsObjectId["nVisit"],
                       pfsObjectId["pfsVisitHash"],
                       self.spectrum_storage.spectrum_infos["RA"],
                       self.spectrum_storage.spectrum_infos["DEC"],
                       self.spectrum_storage.spectrum_infos["targetType"]]
        return self.add_line_to_hdu("TARGET",new_target)

    def get_binary_table_size(self, hdu_name):
        with fits.open(self.path) as hdulist:
            hdu_data = hdulist[hdu_name].data
            return len(hdu_data)
        
    def get_error_code(self, ot, stage):
        try:
            err = self.drp1d_output.get_error(ot,stage)
            return ErrorCode[err["code"]].value
        except:
            return 0
        
    def add_errors(self):
        error_codes = []
        error_messages = []
        if self.drp1d_output.has_error(None,"init"):
            message = self.drp1d_output.get_error(None,"init","message")
            code = self.get_error_code(None,"init")
        else:
            code = 0
            message = ""
        error_codes.append(code)
        error_messages.append(message)
        for ot in ["galaxy","qso","star"]:
            if self.drp1d_output.has_error(ot,"redshiftSolver"):
                message = self.drp1d_output.get_error(ot,"redshiftSolver")["message"]
                code = self.get_error_code(ot,"redshiftSolver")
            else:
                message = ""
                code = 0
            error_codes.append(code)
            error_messages.append(message)
        for ot in ["galaxy","qso"]:
            try:
                if self.drp1d_output.has_error(ot,"lineMeasSolver"):
                    message = self.drp1d_output.get_error(ot,"lineMeasSolver")["message"]
                    code = self.get_error_code(ot,"lineMeasSolver")
                else:
                    message = ""
                    code = 0 
                error_codes.append(code)
                error_messages.append(message)
            except:
                error_codes.append(code)
                error_messages.append(message)
        if self.drp1d_output.has_error(None,"classification"):
            error_codes.append(self.get_error_code(None,"classification"))
        else:
            error_codes.append(0)
        return self.add_line_to_hdu("ERRORS",error_codes + error_messages)
        
    def add_warnings(self):
        params = self.calibration_library.parameters

        line = []
        line.append(self.drp1d_output.get_attribute(None,
                                                    "init_warningFlag",
                                                    "InitWarningFlags")
                    )
        for ot in ["galaxy","qso","star"]:
            meth = params.get_redshift_solver_method(ot).value
            if self.drp1d_output.has_error(ot,"redshiftSolver"):
                line.append(0)
            else:
                line.append(self.drp1d_output.get_attribute(ot,
                                                            "warningFlag",
                                                            meth+"WarningFlags")
                            )
                
        for ot in ["galaxy","qso"]:
            meth = params.get_linemeas_method(ot).value
            if self.drp1d_output.has_error(ot,"lineMeasSolver"):
                line.append(0)
            else:
                line.append(self.drp1d_output.get_attribute(ot,
                                                            "warningFlag",
                                                            meth+"WarningFlags")
                            )

        line.append(0)
                                # self.drp1d_output.get_attribute(None,
                                #                                 "warningFlag",
                                #                                 "classificationWarningFlags"),
        return self.add_line_to_hdu("WARNINGS",line)        

    def get_classification_type(self):
        try:
            return self.drp1d_output.get_attribute(None,"classification","Type").upper()
        except Exception as e:
            return ""
        
    def add_classification(self):
        
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
                
        classification = [self.get_classification_type(),
                           o_proba["galaxy"],
                           o_proba["qso"],
                           o_proba["star"]]
        return self.add_line_to_hdu("CLASSIFICATION",classification)
        
    def add_object_candidates(self, object_type, targetId):
        if object_type in self.drp1d_output.object_results:
            nb_candidates = self.drp1d_output.get_nb_candidates(object_type)
        else:
            nb_candidates = 0
        params = self.calibration_library.parameters

        zcandidates = np.ndarray((nb_candidates,),
                                 dtype=[('targetId', 'i4'),
                                        ('cRank', 'i4'),
                                        ('redshift', 'f4'),
                                        ('redshiftError', 'f4'),
                                        ('redshiftProba', 'f4'),
                                        ('subClass', 'S15'),
                                        ('continuumFile','S50'),
                                        ('lineCatalogRatioFile','S50'),
                                        ('modelId', 'i4')
                                        ])
        model_index = self.get_binary_table_size(f"{object_type.upper()}_CANDIDATES")
        for rank in range(nb_candidates):
            zcandidates[rank]['targetId'] = targetId
            zcandidates[rank]['redshift'] = self.drp1d_output.get_candidate_data(object_type, rank, "Redshift")
            zcandidates[rank]['redshiftError'] = self.drp1d_output.get_candidate_data(object_type, rank, "RedshiftUncertainty")
            zcandidates[rank]['cRank'] = rank
            zcandidates[rank]['redshiftProba'] = self.drp1d_output.get_candidate_data(object_type, rank, "RedshiftProba")
            if params.get_redshift_solver_method(object_type).value == "lineModelSolve":
                zcandidates[rank]['subClass'] = self.drp1d_output.get_candidate_data(object_type, rank, "SubType")
                zcandidates[rank]['lineCatalogRatioFile'] = self.drp1d_output.get_candidate_data(object_type, rank, "LinesRatioName")
            else:
                zcandidates[rank]['subClass'] = ""
                zcandidates[rank]['lineCatalogRatioFile'] = ""
            zcandidates[rank]['continuumFile'] = self.drp1d_output.get_candidate_data(object_type, rank, "ContinuumName")
                            
            zcandidates[rank]['modelId'] = model_index
            model_index = model_index + 1
            

        self.add_lines_to_hdu(f"{object_type.upper()}_CANDIDATES",zcandidates)

        
    def add_star_candidates(self,targetId):
        if "star" in self.drp1d_output.object_results:
            nb_candidates = self.drp1d_output.get_nb_candidates("star")
        else:
            nb_candidates = 0

        zcandidates = np.ndarray((nb_candidates,),
                                 dtype=[('targetId', 'i4'),
                                        ('cRank', 'i4'),
                                        ('velocity', 'f4'),
                                        ('velocityError', 'f4'),
                                        ('templateProba', 'f4'),
                                        ('subClass', 'S15'),
                                        ('templateFile','S50'),
                                        ('modelId','i4')
                                        ])
        model_index = self.get_binary_table_size(f"STAR_CANDIDATES")

        for rank in range(nb_candidates):
            zcandidates[rank]['targetId'] = targetId
            zcandidates[rank]['velocity'] = self.drp1d_output.get_candidate_data("star", rank, "Redshift") * speed_of_light
            zcandidates[rank]['velocityError'] = self.drp1d_output.get_candidate_data("star", rank, "RedshiftUncertainty") * speed_of_light
            zcandidates[rank]['cRank'] = rank
            zcandidates[rank]['templateProba'] = self.drp1d_output.get_candidate_data("star", rank, "RedshiftProba")
            zcandidates[rank]['subClass'] = "" # self.drp1d_output.get_candidate_data("star", rank, "ContinuumName").split("_")[0]
            zcandidates[rank]['templateFile'] = self.drp1d_output.get_candidate_data("star", rank, "ContinuumName")
            zcandidates[rank]['modelId'] = model_index
            model_index = model_index + 1
            
        self.add_lines_to_hdu('STAR_CANDIDATES', zcandidates)

    def add_object_lines(self, object_type, targetId):
        fr = pd.DataFrame(self.drp1d_output.get_dataset(object_type, "linemeas"))
        fr = fr[fr["LinemeasLineLambda"] > 0]
        fr = fr.set_index("LinemeasLineID")
        line_catalog = self.calibration_library.line_catalogs_df[object_type]["lineMeasSolve"]
        fr = pd.merge(fr, line_catalog[["Name", "WaveLength"]], left_index=True, right_index=True)


        zlines = np.ndarray((fr.index.size,),
                            dtype=[('targetId', 'i4'),
                                   ('lineName', 'S15'),
                                   ('lineWave', 'f4'),
                                   ('lineZ', 'f4'),
                                   ('lineZError', 'f4'),
                                   ('lineSigma', 'f4'),
                                   ('lineSigmaError', 'f4'),
                                   ('lineVelocity', 'f4'),
                                   ('lineVelocityError', 'f4'),
                                   ('lineFlux', 'f4'),
                                   ('lineFluxError', 'f4'),
                                   ('lineEW', 'f4'),
                                   ('lineEWError', 'f4'),
                                   ('lineContinuumLevel', 'f4'),
                                   ('lineContinuumLevelError', 'f4')])
        zi = 0
        z = self.drp1d_output.get_attribute(object_type, "linemeas_parameters", "LinemeasRedshift" )
        for i in list(fr.index):
            zlines[zi]['targetId']=targetId           
            zlines[zi]['lineName'] = fr.at[i, "Name"]
            zlines[zi]['lineWave'] = fr.at[i, "LinemeasLineLambda"]*0.1
            offset = fr.at[i, "LinemeasLineOffset"]
            zlines[zi]['lineZ'] = z + offset/speed_of_light + z*offset/speed_of_light
            zlines[zi]['lineZError'] = fr.at[i, "LinemeasLineOffsetUncertainty"]/speed_of_light
            zlines[zi]['lineSigma'] = fr.at[i,"LinemeasLineWidth"]/10.
            zlines[zi]['lineSigmaError'] = -1
            zlines[zi]['lineVelocity'] = fr.at[i,"LinemeasLineVelocity"]
            zlines[zi]['lineVelocityError'] = fr.at[i,"LinemeasLineVelocityUncertainty"]
            # erg/cm2/s -> 10^-35 W/m2 : erg/cm2/s=10^-7W/cm2=10^-3W/m2 -> *10^-3
            zlines[zi]['lineFlux'] = fr.at[i, "LinemeasLineFlux"]*10**-3
            zlines[zi]['lineFluxError'] = fr.at[i, "LinemeasLineFluxUncertainty"]*10**-3
            zlines[zi]['lineEW'] = -1
            zlines[zi]['lineEWError'] = -1
            zlines[zi]['lineContinuumLevel'] = fr.at[i,"LinemeasLineContinuumFlux"]
            zlines[zi]['lineContinuumLevelError'] = fr.at[i,"LinemeasLineContinuumFluxUncertainty"]
            zi = zi+1
        self.add_lines_to_hdu(f'{object_type.upper()}_LINES',zlines)

    def add_model(self, object_type):
        if object_type in self.drp1d_output.object_results:
            nb_candidates = self.drp1d_output.get_nb_candidates(object_type)
        else:
            nb_candidates = 0
        for rank in range(nb_candidates):
            model = self._get_model_on_lambda_range(object_type, rank)
            self.add_array_to_image_hdu(f'{object_type.upper()}_MODELS',model)
        
    def add_object_pdf(self, object_type):
        if not self.drp1d_output.has_error(object_type,"redshiftSolver"):
            try:
                ln_pdf = np.float32(self.drp1d_output.get_attribute(object_type,"pdf","LogZPdfNative"))
            except Exception as e:
                raise Exception(f"Failed to get {object_type} pdf : {e}")
            builder = BuilderPdfHandler()
            pdfHandler = builder.add_params(self.drp1d_output, object_type, True).build()
            pdfHandler.convertToRegular()
            pdf = pdfHandler.valProbaLog
        else:
            zgrid = get_final_regular_z_grid(object_type, self.calibration_library.parameters)
            pdf = np.zeros(len(zgrid)) 
        self.add_array_to_image_hdu(f'{object_type.upper()}_LN_PDF',
                                    pdf)

    def add_quality(self, target_id):
        if not self.drp1d_output.has_error("galaxy","redshiftSolver"):
            try:
                continuumReducedChi2 = self.drp1d_output.get_attribute("galaxy","continuum_quality","MinContinuumReducedChi2")
            except:
                continuumReducedChi2 = -1
        else:
            continuumReducedChi2 = -1
        self.add_line_to_hdu("QUALITY",[continuumReducedChi2])
        
    def _get_model_on_lambda_range(self, object_type, rank):
        model = np.array(self.spectrum_storage.full_wavelength, dtype=np.float64, copy=True)
        model.fill(np.nan)
        np.place(model, self.spectrum_storage.mask == 0, self.drp1d_output.get_dataset(object_type,"model",rank)["ModelFlux"])
        model = np.multiply(np.array(self.spectrum_storage.full_wavelength) ** 2, np.array(model)) * (1 / 2.99792458) * 10 ** 16
        return np.float32(model)

    def _get_pdf_grid(self, object_type):
        builder = BuilderPdfHandler()
        pdfHandler = builder.add_params(self.drp1d_output, object_type, True).build()
        pdf_grid = np.float32(pdfHandler.redshifts)


                  
