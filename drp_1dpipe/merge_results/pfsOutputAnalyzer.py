from .abstractOutputAnalyzer import AbstractOutputAnalyzer
from .abstractInputManager import AbstractInputManager
from pylibamazed.Parameters import Parameters
from pylibamazed.redshift import ErrorCode
import pandas as pd
import numpy as np
from astropy.table import Table
from astropy.io import fits
import os
import json
import glob
import logging

class PfsOutputAnalyzer(AbstractOutputAnalyzer):

    def __init__(
        self,
        output_directory: str,
        input_manager: AbstractInputManager,
        parameters: Parameters,
        subdirs: list = None,
        ref_remapping: dict = {},
    ):
        AbstractOutputAnalyzer.__init__(self, output_directory, input_manager, parameters, ref_remapping)
        self.output_subdirs = subdirs
        self.load_datamodel_conversion()
        self.check_fits_integrity()
            

    def load_datamodel_conversion(self) -> None:
        module_root_dir = os.path.split(__file__)[0]
        DATAMODEL_CONVERSION_FILE = "../auxdir/pfs_to_amazed_dm.json"
        if not os.path.exists(os.path.join(module_root_dir, DATAMODEL_CONVERSION_FILE)):
            raise FileNotFoundError("Missing file for conversion between Pfs and Amazed datamodels")
        with open(os.path.join(module_root_dir, DATAMODEL_CONVERSION_FILE)) as f:
            self.datamodel_conversion = json.load(f)

    def get_redshifts(self) -> pd.DataFrame:
        pfsCoZPath = glob.glob(os.path.join(self.output_directory, "data", "pfsCoZcandid*.fits"))[0]
        return self._get_redshifts_from_path(pfsCoZPath)

    def check_fits_integrity(self):
        path = glob.glob(os.path.join(self.output_directory, "data", "pfsCoZcandid*.fits"))[0]
        f = fits.open(path)
        nb_targets = len(f[1].data)
        for hdu in ["WARNINGS","ERRORS","GALAXY_LN_PDF","QSO_LN_PDF","STAR_LN_PDF"]:
            if len(f[hdu].data) != nb_targets:
                raise Exception(f'hdu {hdu} of size {len(f[hdu].data)} , should be {nb_targets}')
        
    def get_global_lines_infos(self,snr_threshold):
        path = glob.glob(os.path.join(self.output_directory, "data", "pfsCoZcandid*.fits"))[0]
        lines = dict()
        lines["qso"] = Table.read(path, hdu=14, format="fits").to_pandas()
        lines["galaxy"] = Table.read(path, hdu=9, format="fits").to_pandas()
        ret = dict()
        ret["count"] = dict()
        ret["meanCount"] = dict()

        ret["posFlux"] = dict()
        ret["correct"] = dict()
        for o in ["qso", "galaxy"]:
            ret["count"][o] = len(lines[o])
            lines[o]["snr"] = lines[o].lineFlux / lines[o].lineFluxError
            ret["posFlux"][o] = len(lines[o][lines[o].lineFlux > 0])
            ret["correct"][o] = len(lines[o][lines[o].snr > snr_threshold])
        return ret

    def get_correct_lines(self, snr_threshold, object_type):
        path = glob.glob(os.path.join(self.output_directory, "data", "pfsCoZcandid*.fits"))[0]
        lines = Table.read(path, hdu=f'{object_type.upper()}_LINES', format="fits").to_pandas()
        target = Table.read(path,hdu=1, format="fits").to_pandas()
        lines = lines[lines.lineFluxError.notnull()]
        lines = pd.merge(lines,target[["targetId","objId"]],left_on="targetId",right_on="targetId")
        lines["snr"]=abs(lines.lineFlux/lines.lineFluxError)
        # lines = lines[lines.snr >= snr_threshold]
        lines = lines[lines.lineWave > self.parameters.get_lambda_range_min()*0.1]
        lines = lines[lines.lineWave < self.parameters.get_lambda_range_max()*0.1]
        lines["lineName"]=lines["lineName"].str.decode('UTF-8')
        return lines

    # def get_redshifts(self):
    #     path = glob.glob(os.path.join(self.output_directory, "data", "pfsCoZcandid*.fits"))[0]
    #     target = Table.read(path,hdu=1, format="fits").to_pandas()
    #     rs = []
    #     for object_type in ["galaxy","qso"]:
    #         gr = Table.read(path, hdu=f'{object_type.upper()}_CANDIDATES', format="fits").to_pandas()
    #         gr = gr[gr.cRank==0]
    #         gr = pd.merge(gr,target[["targetId","objId"]],left_on="targetId",right_on="targetId")
    #         gr = gr.rename(columns={"redshift":f"{object_type}.Redshift"})
    #         rs.append(gr[["objId",f"{object_type}.Redshift"]])
    #     return pd.merge(rs[0],rs[1])
    
    def diff_redshifts(self,ref, threshold =1e-4):
        self.load_results_summary()
        ref.load_results_summary()
        rs = self.get_redshifts()
        ors = ref.get_redshifts()
        for object_type in ["galaxy","qso"]:
            ors = ors.rename(columns={f'{object_type}.Redshift':
                                      f'ref.{object_type}.Redshift'})
        rs = pd.merge(rs,ors,left_on="objId",right_on="objId")
        res = dict()
        cols = []
        for object_type in ["galaxy","qso"]:
            col = f'{object_type}.Redshift'
            refcol = f'ref.{object_type}.Redshift'
            ercol = f'{object_type}.zerr'
            rs[ercol] = abs(rs[col] - rs[refcol])
            rs[ercol] = rs[ercol]/rs[refcol]
            res[object_type]=rs[rs[ercol]>threshold][[col,refcol,ercol]]
        return res

    def diff_lines(self,ref, snr_threshold, rdiff_threshold):
        for object_type in ["galaxy","qso"]:
            # Get lines in lambda range
            cl = self.get_correct_lines(snr_threshold,object_type).set_index(["objId","lineName"])
            ocl = ref.get_correct_lines(snr_threshold,object_type).set_index(["objId","lineName"])
            # Create merged table with output columns :
            #   objId : Object ID
            #   lineName : Line name
            #   objId0x : Object ID in hexa
            #   lineWave_y : Central wavelength of the line in reference file
            #   lineSigma_y : Width of the line in reference file
            #   snr_x : SNR of the line in output file
            #   snr_y : SNR of the line in reference file
            #   dsnr : Relative difference of th SNR of the line
            #   lineFlux_x : Flux of the line in output file
            #   lineFlux_y : Flux of the line in reference file
            #   dflux : Relative difference of the flux of the line
            #   lineFluxError_x : Flux error of the line in output file
            #   lineFluxError_y : Flux error of the line in reference file
            #   derr : Relative difference of the flux error of the line
            df = pd.merge(cl, ocl, left_index=True, right_index=True, how="outer")
            df['objId0x'] = [f"{i[0]:016x}" for i in df.index]
            df['dsnr']=(df['snr_x']-df['snr_y'])/df['snr_x']
            df['dflux']=(df['lineFlux_x']-df['lineFlux_y'])/df['lineFlux_x']
            df['derr']=(df['lineFluxError_x']-df['lineFluxError_y'])/df['lineFluxError_x']
            sel = df[['objId0x','lineWave_y','lineSigma_y','snr_x','snr_y','dsnr','lineFlux_x','lineFlux_y','dflux','lineFluxError_x','lineFluxError_y','derr']].sort_values(by="snr_x")
            # Error raised for relative difference with unexpected NaN value
            dfnn = sel[sel["dflux"].isnull() & sel["lineFlux_x"]!=0.]
            # Error raised for relative difference greater than defined threshold
            dfnnf = sel[sel["dflux"]>rdiff_threshold]
            if len(dfnn):
                print(f'\033[91m✖\033[00m Differences on {object_type} lines:')
                print(dfnn.to_string())
            elif len(dfnnf):
                print(f'\033[91m✖\033[00m Differences on {object_type} lines:')
                print(dfnnf.to_string())
            else:
                print(f"\033[92m✔\033[00m No difference on {object_type} lines.")

    def _get_redshifts_from_path(self, path):
        # get processingID first

        targets = Table.read(path, hdu=1, format="fits").to_pandas()
        targets["ProcessingID"] = targets.catId.map(str) + "-" + targets.objId.map(hex)

        redshifts = targets
        for dataset, infos in self.datamodel_conversion.items():
            hdu_index = infos["hdu_index"]
            cols_mapping = infos["map"]
            hdu_table = Table.read(path, format="fits", hdu=hdu_index)
            hdu_df = hdu_table.to_pandas()
            if "rankCol" in infos.keys():
                rCol = infos["rankCol"]
                hdu_df = hdu_df[hdu_df[rCol] == 0]
            for col, series in hdu_df.items():
                if series.dtype == "object":
                    decoded_series = series.str.decode(encoding="UTF-8")
                    hdu_df[col] = decoded_series
                    # Also convert short int to long int
                elif series.dtype == "int16":
                    hdu_df[col] = series.astype(np.int64)
            if "_lines" in dataset:
                cols = [col.split(".")[-1] for col in cols_mapping.keys()]
                cols = list(set(cols))
                lines = [col.split(".")[0] for col in cols_mapping.keys()]
                lines = list(set(lines))
                for line in lines:
                    renaming = dict()
                    for col in cols:
                        renaming[col] = cols_mapping[f"{line}.{col}"]
                    line_df = hdu_df[hdu_df["lineName"] == line]
                    redshifts = pd.merge(
                        redshifts,
                        line_df[cols + ["targetId"]],
                        left_on="targetId",
                        right_on="targetId",
                        how="outer",
                    )
                    redshifts.rename(columns=renaming, inplace=True)
            else:
                cols = list(cols_mapping.keys())
                hdu_df = hdu_df[cols + ["targetId"]]
                hdu_df.rename(columns=cols_mapping, inplace=True)
                redshifts = pd.merge(redshifts, hdu_df, left_on="targetId", right_on="targetId", how="outer")

        redshifts["classification.Type"] = redshifts["classification.Type"].str.lower()
        for col in redshifts.columns:
            if col.startswith("error") and col.endswith("code"):
                try:
                    redshifts[col] = [ErrorCode(i).name if i != 0 else None for i in redshifts[col]]
                except ValueError as e:
                    logging.getLogger("session_logger").error(
                        f"Wrong Amazed error code in pfs product at col {col} : {e}"
                    )
                    exit(-1)

        return redshifts
