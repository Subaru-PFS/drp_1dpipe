import glob
import os
import pandas as pd
from astropy.io import fits
from pylibamazed.CalibrationLibrary import CalibrationLibrary
from pylibamazed.Parameters import Parameters
from pylibamazed.Spectrum import Spectrum
from pylibamazed.Exception import AmazedError


import json
import numpy as np


class AbstractInputManager:
    def __init__(
        self,
        amazed_config,
        amazed_parameters: Parameters,
        title: str = "",
        calibs: str = "all",
    ):
        self.user_config = init_user_config()

        self.config = amazed_config
        self.title = title
        self.amazed_parameters = amazed_parameters
        self.check_spectra_ids_and_calib(amazed_parameters, amazed_config)
        self.load_calibration(calibs)

    def check_spectra_ids_and_calib(self, amazed_parameters, amazed_config):
        if not all(key in self.config for key in ["spectrum_dir", "calibration_dir"]):
            raise Exception("Given configuration is incomplete")

        if not os.access(os.path.expanduser(self.config["spectrum_dir"]), os.R_OK):
            raise Exception("Spectrum directory " + self.config["spectrum_dir"] + " does not exist")

        if not os.access(os.path.expanduser(self.config["calibration_dir"]), os.R_OK):
            raise Exception("Calibration directory " + self.config["calibration_dir"] + " does not exist")
        self.load_spectra_ids()

        self.calibration_library = CalibrationLibrary(amazed_parameters, amazed_config["calibration_dir"])

        self.spectra.set_index("ProcessingID", inplace=True)
        self.spectra["ProcessingID"] = self.spectra.index
        self.catalog_data = None

    def load_spectra_ids(self):
        raise NotImplementedError()

    def read_spectrum(self):
        raise NotImplementedError()

    def load_calibration(self, calibs):
        self.calibration_library.load_all(calibs)

    def get_template_dir(self, object_type: str):
        return os.path.expanduser(
            os.path.join(
                self.config["calibration_dir"],
                self.amazed_parameters.get_template_dir(object_type),
            )
        )

    def get_template_ratio_path(self, tplratio_file):
        return os.path.join(self.config["calibration_dir"], self.tpl_ratio_catalog, tplratio_file)

    def get_template_path(self, object_type, tpl_file):
        return os.path.join(self.get_template_dir(object_type), object_type, tpl_file)

    # ************************* Data retrieval functions ************************* #

    def restrict_to_lambda_range(self, data, keys, obs_id):
        res = dict()
        df = pd.DataFrame(data)
        lambda_range = self.amazed_parameters.get_lambda_range(obs_id)

        df = df[df["lambda"] >= lambda_range[0]]
        df = df[df["lambda"] <= lambda_range[1]]
        res["lambda"] = df["lambda"].to_list()
        for key in keys:
            res[key] = df[key].to_list()
        return res

    def extract_other_columns(
        self, spectrum: Spectrum, wave: list, obs_id: str = "", filtered_only: bool = True
    ) -> dict:
        """
        Extract additional columns from the reader and returns them as a dictionary.

        Parameters
        ----------
        spectrum : Spectrum
            Spectrum read via the reader to get additional columns from.

        Returns
        -------
        other_cols : dict
            Additional columns under dictionary format.
        """
        other_cols = {}
        df_others = spectrum.get_others(obs_id, filtered_only=filtered_only)
        df_others["other_wave"] = spectrum.get_wave(obs_id, filtered_only=filtered_only)
        df_others = df_others[df_others["other_wave"] >= wave[0]]
        df_others = df_others[df_others["other_wave"] <= wave[-1]]
        for col in df_others.columns:
            other_cols[col] = df_others[col].to_list()
        return other_cols

    def _has_mask_col(self, res: dict) -> bool:
        MASK_COL_NAME = "MASK"
        return MASK_COL_NAME in res.keys()

    def _decode_mask(self, spectrum_data: dict) -> None:
        """
        Decode the number value of the mask into its individual flags.

        Parameters
        ----------
        spectrum_data : dict
            Various lists of data about the spectrum.
        """
        return

    def get_spectrum_data(self, spectrum_id, obs_id: str = ""):
        spectrum = self.read_spectrum(spectrum_id)
        spectrum.init()
        res = dict()
        try:
            res["lambda"] = list(spectrum.get_wave(obs_id))
            res["flux"] = list(spectrum.get_flux(obs_id))
            res["variance"] = list(spectrum.get_error(obs_id))
        except AmazedError as ae:
            print(f"AmazedError occurred on spectrum {spectrum_id}, likely from empty data")
            raise ae

        RESTRICT_KEYS = ["flux", "variance"]

        # Gather additional columns (if any) and add them to the data
        other_cols = self.extract_other_columns(spectrum, res["lambda"], obs_id, filtered_only=True)

        res = self.restrict_to_lambda_range(res, RESTRICT_KEYS, obs_id)
        res = res | other_cols
        res["other_cols"] = list(other_cols.keys())

        if hasattr(self, "spectrum_storage"):
            if hasattr(self.spectrum_storage, "fiberId"):
                res["fiberId"] = str(self.spectrum_storage.fiberId)
                res["nVisit"] = self.spectrum_storage.nVisit

        try:
            res["lsf"] = spectrum.get_lsf()
        except Exception:
            res["lsf"] = None

        res["ranges"] = self.define_value_ranges(res, other_cols)
        if self._has_mask_col(res):
            self._decode_mask(res)
        return res

    def define_value_ranges(self, spectrum, other_cols):
        if len(spectrum["lambda"]) > 0:
            lambda_range = [spectrum["lambda"][0], spectrum["lambda"][-1]]
            flux_range = [min(spectrum["flux"]), max(spectrum["flux"])]
            noise_max = max(spectrum["variance"])
        else:
            lambda_range = []
            flux_range = []
            noise_max = np.nan
        ranges = {
            "lambda": lambda_range,
            "flux": flux_range,
            "noise": noise_max,
        }
        if spectrum["lsf"]:
            if spectrum["lsf"]["lsfType"] == "gaussianVariableWidth":
                ranges["lsf"] = [spectrum["lsf"]["width"].min(), spectrum["lsf"]["width"].max()]
        if other_cols != {}:
            for key, value in other_cols.items():
                other_range = [0, max(value)]
                ranges[key] = other_range
        return ranges

    def get_template_ratio_data(self, catalog_path, filters=[]):
        catalog_data = pd.read_csv(
            self.get_template_ratio_path(catalog_path),
            sep="\t",
            header=0,
            skiprows=1,
            float_precision="round_trip",
            index_col=None,
        )
        for criterion in filters:
            name = criterion["field"]
            value = criterion["value"]
            c_type = criterion["type"]
            if c_type == "=" and value != "All":
                catalog_data = catalog_data[catalog_data[name] == value]

        return catalog_data

    def get_default_linecatalog(self, filters, redshift, object_type):
        return self.get_full_catalog_df(object_type, filters, redshift=redshift)

    def get_linemodel_linecatalog(self, object_type, filters, method, redshift=None):
        return self.get_full_catalog_df(object_type, filters, method, redshift=redshift)

    def get_full_catalog_df(self, object_type, filters, method=None, redshift=None):
        has_catalog = object_type and object_type in self.calibration_library.line_catalogs_df
        if has_catalog:
            has_catalog = method and method in self.calibration_library.line_catalogs_df[object_type]
        if has_catalog:
            catalog_data = self.calibration_library.line_catalogs_df[object_type][method]
        else:
            catalog_data = pd.read_csv(
                os.path.join(
                    self.config["calibration_dir"],
                    "linecatalogs",
                    self.user_config["linecatalog"],
                ),
                sep="\t",
            )
        for criterion in filters:
            name = criterion["field"]
            value = criterion["value"]
            c_type = criterion["type"]
            if c_type == "=" and value != "All":
                catalog_data = catalog_data[catalog_data[name] == value]

        if redshift is not None:
            catalog_data["FittedLineLambda"] = (1 + redshift) * catalog_data["WaveLength"]
            full_lambda_range = self.build_full_lambda_range()

            catalog_data = catalog_data[catalog_data["FittedLineLambda"] >= full_lambda_range[0]]
            catalog_data = catalog_data[catalog_data["FittedLineLambda"] <= full_lambda_range[1]]

        return catalog_data

    def build_full_lambda_range(self):
        """Build the full lambda range of a spectrum."""
        obs_ids = list(self.amazed_parameters.get_observation_ids())
        min_range = self.amazed_parameters.get_lambda_range(obs_ids[0])[0]
        max_range = self.amazed_parameters.get_lambda_range(obs_ids[-1])[1]
        full_lambda_range = [min_range, max_range]
        return full_lambda_range

    def export_input_spectrum_list(self, filtered_redshifts):
        merged = self.spectra.merge(filtered_redshifts, left_index=True, right_index=True)
        if self.amazed_parameters.get_multiobs_method():
            cols = ["ProcessingID", "ObservationChunk"]
        else:
            cols = ["ProcessingID"]
        merged["ProcessingID"] = merged.index
        if "Path" in self.spectra.columns:
            cols.append("Path")

        merged.to_csv(
            "~/input_" + self.title + ".spectrumlist",
            columns=cols,
            sep="\t",
            header=False,
            index=False,
        )

        return "~/input_" + self.title + ".spectrumlist"
