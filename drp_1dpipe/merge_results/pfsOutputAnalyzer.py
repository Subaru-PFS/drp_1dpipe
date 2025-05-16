from .abstractOutputAnalyzer import AbstractOutputAnalyzer
from .abstractInputManager import AbstractInputManager
from pylibamazed.Parameters import Parameters
import pandas as pd
import numpy as np
from astropy.table import Table
import os
import json
import glob


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

    def get_global_lines_infos(self):
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
            ret["posFlux"][o] = len(lines[o][lines[o].lineFlux > 0])
            ret["correct"][o] = len(lines[o][lines[o].lineSigma > 2])
        return ret

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
                redshifts = pd.merge(redshifts, hdu_df, left_on="targetId", right_on="targetId")
        redshifts["classification.Type"] = redshifts["classification.Type"].str.lower()

        return redshifts
