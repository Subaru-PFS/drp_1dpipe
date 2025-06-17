import os
import pandas as pd
import math
import numpy as np
from scipy.stats import sigmaclip
import json
import logging
import warnings


from pylibamazed.Parameters import Parameters
from pylibamazed.AbstractOutput import spectrum_model_stages

module_root_dir = os.path.split(__file__)[0]


class AbstractOutputAnalyzer:
    """A wrapper class for accessing and analyzing an amazed output directory

    Attributes:
        single_outputs (dict[str, SingleOutputManager]): when an individual result is analyzed, it is added to this
            map do not use this to access a single output but rather get_single_output
        redshifts (DataFrame): results summary containing the best redshift for each individual results,
            based on redshifts.csv, it is merged with the reference file if given
            use Dataframe.columns to get all columns names
        filtered_redshifts (DataFrame): view Dataframe relative to `redshifts` containing the filtered version
            obtained with `filter_results` method

    """

    def __init__(self, output_directory, input_manager, parameters: Parameters, ref_remapping: dict = {}):
        self.input_manager = input_manager
        self.output_directory = output_directory

        self.reference_path = None

        # Dataframes containing results, filtered and ordered are only views
        self.redshifts = None
        self.filtered_redshifts = None
        self.puco = []

        self.error_threshold = 5e-4
        self.redshift_error_mean = None
        self.redshift_error_sigma = None
        self.relzerr_to_mismatch_name = dict()
        self.nb_loaded_results = 0
        self.not_referenced_results = pd.Index(pd.Series())
        self.empty_ref_results = pd.Index(pd.Series())
        self.parameters = parameters
        self.attributes_path = None
        self.redshift_error_computed_by_sigma_clipping = True
        self.sigma_clipping_redshift_error = None
        self.mismatch_catalog_path = os.path.join(module_root_dir, "resources", "mismatches.json")
        self.sigma_clipping_around = 0.01
        self.ref_remapping = ref_remapping
        logger = logging.getLogger("session_logger")
        logger.info("output data manager instanciated")

    def get_redshifts(self):
        raise NotImplementedError()

    def add_reference_file(self, reference_path: str):
        """Add reference path

        Args:
            reference_path (str):

        Returns:

        """
        self.reference_path = reference_path

    def add_attributes_file(self, attributes_path: str):
        """Add reference path

        Args:
            attributes_path (str):

        Returns:

        """
        self.attributes_path = attributes_path
        attrs = pd.read_csv(self.attributes_path, sep="\t", header=0, dtype={"SpectrumID": object})
        attrs.set_index("SpectrumID", inplace=True)

        self.redshifts = pd.merge(self.redshifts, attrs, left_index=True, right_index=True)
        self.filtered_redshifts = pd.merge(self.filtered_redshifts, attrs, left_index=True, right_index=True)

    def get_execution_dir(self):
        return self.output_directory

    def _remap_reference_values(self, ref: pd.DataFrame) -> None:
        if len(self.ref_remapping) != 0:
            for remap_key, remap_values in self.ref_remapping.items():
                initial_values = ref[remap_key].unique()
                # check for potential invalid remapping keys
                if set(initial_values).issubset(remap_values.keys()):
                    ref[remap_key] = ref[remap_key].map(remap_values)
                else:
                    warn_message = (
                        f"\033[93mValues given in remapping file for key '{remap_key}' do not match "
                        f"the values found in the reference. Remapping : {list(remap_values.keys())}, "
                        f"reference : {initial_values}. Skipping remapping for this key. \033[0m"
                    )
                    warnings.warn_explicit(
                        warn_message, UserWarning, "AmazedOutputAnalyzer/abstractSession", 100
                    )

    def _check_reference_type_values(self, ref: pd.DataFrame, spectrum_models: list) -> None:
        """
        Check that no invalid value is left in the type column of the reference.

        :param ref: reference object to check
        :type ref: pd.DataFrame
        :param spectrum_models: list of the expected values for the type
        :type spectrum_models: list (of str)
        """
        ref_types = ref["Type"].unique()
        if not set(ref_types).issubset(spectrum_models):
            error_message = (
                f"\033[91mThe following type names were found in the reference (after remapping) : {ref_types}, "
                f"at least one of them is wrong and should not exist, based on the expected types : {spectrum_models}. "
                "You must remove the problematic values such as NaN or None, or use a valid remapping file to "
                "change the name of the types to match the expected types (use the option -rrm). \033[0m"
            )
            ref = ref[ref.Type.notna()]
            ref_types = ref["Type"].unique()
            if not set(ref_types).issubset(spectrum_models):
                raise ValueError(error_message)
        return ref

    def _filter_empty_ref_redshift(self, results):
        """
        Find empty elements in results and filter missing ref.Redshift.

        Parameters
        ----------
        results: pandas.Dataframe
            The results to filter

        Return
        ------
        res_filt: pandas.Dataframe
            The filtered results Dataframe
        """
        logger = logging.getLogger("session_logger")
        res_filt = results[results["ref.Redshift"] > -1]
        self.empty_ref_results = results.index.difference(res_filt.index)
        if self.empty_ref_results.size > 0:
            logger.warning(
                f"Ref is missing Redshift on {self.empty_ref_results.size}"
                f"spectra, filtered those entries, {res_filt.index.size} left"
            )
        else:
            logger.info(f"No Redshift missing in ref, returning {res_filt.index.size} spectra")
        return res_filt

    def load_results_summary(self):
        """Load redshift.csv, and reference catalog if given, into self.redshifts.
        This method is called by Session.__init__ , it should not be called elsewhere

        Returns:

        """
        if self.redshifts is None:
            logger = logging.getLogger("session_logger")
            rs = self.get_redshifts()
            rs = rs.set_index("ProcessingID")
            self.attr_renaming = dict()

            rs["SpectrumID"] = rs.index

            for o in self.parameters.get_spectrum_models():
                for stage in spectrum_model_stages:
                    err_col = f"error.{o}.{stage}.code"
                    if err_col in rs.columns and rs[err_col].dtype != np.dtype("int64"):
                        rs.loc[rs[err_col].isnull(), err_col] = "SUCCESS"

            self.nb_loaded_results = rs.index.size
            logger.info(str(self.nb_loaded_results) + " results loaded")
            rs["Redshift"] = -1.0
            rs["MinContinuumReducedChi2"] = -1.0
            rs["MaxFitAmplitudeSigma"] = -1.0
            rs["SwitchedToFromSpectrum"] = False
            rs["RedshiftProba"] = -1.0
            rs["ReducedLeastSquare"] = -1.0
            for o in self.parameters.get_spectrum_models():
                if not self.parameters.get_redshift_solver_method(o) or f"{o}.Redshift" not in rs.columns:
                    continue
                mask = rs["classification.Type"] == o
                for c in [
                    "Redshift",
                    "MinContinuumReducedChi2",
                    "MaxFitAmplitudeSigma",
                    "SwitchedToFromSpectrum",
                    "RedshiftProba",
                    "ReducedLeastSquare"
                ]:
                    if f"{o}.{c}" in rs.columns:
                        rs.loc[mask, c] = rs[f"{o}.{c}"]

            if self.reference_path is not None:
                ref = pd.read_csv(
                    self.reference_path,
                    sep="\t",
                    header=0,
                    dtype={"ProcessingID": object},
                    low_memory=False,
                )
                index_col = "ProcessingID"
                if index_col not in ref.columns:
                    index_col = "AstronomicalSourceID"

                if len(ref[index_col].unique()) != len(ref):
                    raise ValueError(f"{index_col} must be unique in reference file")
                self._remap_reference_values(ref)
                spectrum_models = self.parameters.get_spectrum_models()
                if "classification.Type" not in ref.columns and "Type" not in ref.columns:
                    raise ValueError(
                        "\033[91mReference file is missing the mandatory 'classification.Type' column, make "
                        "sure it is present with that exact name ('type' or 'classification.type' "
                        "are invalid for example, and should be renamed).\033[0m"
                    )
                if "Redshift" not in ref.columns:
                    if "galaxy.Redshift" not in ref.columns:
                        raise Exception("Redshift or galaxy.Redshift should be in reference")
                    for o in spectrum_models:
                        if (
                            not self.parameters.get_redshift_solver_method(o)
                            or f"{o}.Redshift" not in ref.columns
                        ):
                            continue
                        mask = ref["classification.Type"] == o
                        ref.loc[mask, "Redshift"] = ref[o + ".Redshift"]
                    ref.rename(columns={"classification.Type": "Type"}, inplace=True)
                # Ensuring the classification in the reference has only valid values
                ref = self._check_reference_type_values(ref, spectrum_models)
                renaming = dict()
                for col in ref.columns:
                    if col != index_col:
                        renaming[col] = "ref." + col
                ref.rename(columns=renaming, inplace=True)
                # Filling empty spaces with -99 ; could change if necessary
                #              ref = ref.fillna(-99)
                for c in ref.columns:
                    if ref[c].dtype == np.dtype("O"):
                        ref[c] = ref[c].replace(np.nan, "")
                    else:
                        ref[c] = ref[c].replace(np.nan, 99)
                # It is important to make sure both the index is based on a string column
                ref = ref.astype({index_col: "str"})
                ref.reset_index()
                ref.set_index(index_col, inplace=True)
                m_res = pd.merge(rs, ref, left_on=index_col, right_index=True)

                if index_col != "ProcessingID":
                    self.not_referenced_results = rs.set_index(index_col).index.difference(ref.index)
                else:
                    self.not_referenced_results = rs.index.difference(ref.index)
                if self.not_referenced_results.size > 0:
                    logger.info(
                        "Reference file not complete, "
                        + str(self.not_referenced_results.size)
                        + " unreferenced results"
                    )
                    if m_res.index.size < 2:
                        raise Exception(
                            "Reference file and output directory must match on more than one spectrum"
                        )

                m_res["DeltaZ"] = -1.0
                for o in self.parameters.get_spectrum_models():
                    if (
                        not self.parameters.get_redshift_solver_method(o)
                        or f"{o}.Redshift" not in m_res.columns
                    ):
                        continue
                    m_res[o + ".z_err"] = (m_res[o + ".Redshift"] - m_res["ref.Redshift"]) / (
                        1 + m_res["ref.Redshift"]
                    )
                    m_res[o + ".z_err"] = m_res[o + ".z_err"].replace(0, 1e-17)
                    m_res[o + ".abs_z_err"] = abs(m_res[o + ".z_err"])
                    mask = m_res["classification.Type"] == o
                    m_res.loc[mask, "DeltaZ"] = m_res[o + ".z_err"]

                m_res["DeltaZ"] = m_res["DeltaZ"].replace(0, 1e-17)
                m_res["abs_DeltaZ"] = abs(m_res["DeltaZ"])

                m_res = self._filter_empty_ref_redshift(m_res)

                if self.nb_loaded_results > 50:
                    self.compute_sigma_clipping(m_res)
                else:
                    self.redshift_error_computed_by_sigma_clipping = False
                    self.redshift_error_sigma = 1e-3
                    self.redshift_error_mean = 0

                self.error_threshold = 3 * self.redshift_error_sigma

                if "classification.Type" in m_res.columns:
                    m_res["Correct_Classification"] = [
                        "Correct" if x == y else "Wrong"
                        for (x, y) in zip(m_res["classification.Type"], m_res["ref.Type"])
                    ]

            else:
                m_res = rs

            if m_res.index.size == 0:
                print("no match between reference and redshift.csv")
                exit(-1)
            self.redshifts = m_res
            self.filtered_redshifts = self.redshifts
            # merge mismatches info here
            if self.reference_path:
                self.define_mismatch_spurious_success_column()

    def compute_sigma_clipping(self, m_res):
        traf = m_res[m_res["abs_DeltaZ"] < self.sigma_clipping_around]
        sg_clip = sigmaclip(traf["DeltaZ"], 3, 3)
        self.redshift_error_sigma = sg_clip.clipped.std()
        self.redshift_error_mean = sg_clip.clipped.mean()
        if self.redshift_error_sigma == 0 or np.isnan(self.redshift_error_sigma):
            self.redshift_error_computed_by_sigma_clipping = False
            self.redshift_error_sigma = 1e-3
            self.redshift_error_mean = 0
        else:
            self.sigma_clipping_redshift_error = 3 * self.redshift_error_sigma

    def define_mismatch_column_v2(self):
        # define mismatches table
        with open(self.mismatch_catalog_path) as f:
            mismatches_list = json.load(f)
        mismatches = []
        for lambda_calc in mismatches_list["mismatches_from_calc"].keys():
            for lambda_true in mismatches_list["mismatches_from_calc"][lambda_calc]:
                mismatches.append(
                    {
                        "MismatchWaveLength": float(lambda_calc),
                        "TrueWaveLength": lambda_true,
                    }
                )
        for lambda_true in mismatches_list["mismatches_from_true"].keys():
            for lambda_calc in mismatches_list["mismatches_from_true"][lambda_true]:
                mismatches.append(
                    {
                        "MismatchWaveLength": lambda_calc,
                        "TrueWaveLength": float(lambda_true),
                    }
                )
        mismatch_catalog = pd.DataFrame(mismatches)

        mismatch_catalog["ratio"] = mismatch_catalog.TrueWaveLength / mismatch_catalog.MismatchWaveLength
        linecatalog = self.input_manager.get_linemodel_linecatalog("galaxy", [], method="lineModelSolve")
        linecatalog = linecatalog[linecatalog.Type == "E"].set_index("WaveLength")
        lambda_range_min = self.parameters.get_lambda_range_min()
        lambda_range_max = 0
        for obs_id in self.parameters.get_observation_ids():
            cur = self.parameters.get_lambda_range(obs_id)[1]
            if cur > lambda_range_max:
                lambda_range_max = cur

        for index, row in mismatch_catalog.iterrows():
            try:
                mismatch_name = linecatalog.at[row.MismatchWaveLength, "Name"]
            except Exception as e:
                print(
                    "Bad mismatch file, cannot find {}  in linecatalog : {}".format(
                        row.MismatchWaveLength, type(e)
                    )
                )
                exit(-1)
            try:
                true_name = linecatalog.at[row.TrueWaveLength, "Name"]
            except Exception as e:
                print(
                    "Bad mismatch file, cannot find {} in linecatalog : {}".format(
                        row.TrueWaveLength, type(e)
                    )
                )
                exit(-1)
            msm_name = true_name + "_" + mismatch_name
            self.filtered_redshifts["z_mis"] = row.ratio * (1 + self.filtered_redshifts["ref.Redshift"]) - 1
            self.filtered_redshifts[msm_name] = abs(
                self.filtered_redshifts.Redshift - self.filtered_redshifts.z_mis
            )
            self.filtered_redshifts[msm_name] = self.filtered_redshifts[msm_name] / (
                1 + self.filtered_redshifts.z_mis
            )
            self.filtered_redshifts["delta_zmis"] = (
                self.filtered_redshifts.z_mis - self.filtered_redshifts["ref.Redshift"]
            ) / (1 + self.filtered_redshifts["ref.Redshift"])
            self.filtered_redshifts["lambda_" + msm_name] = (
                self.filtered_redshifts["ref.Redshift"] + 1
            ) * row.TrueWaveLength

            mask = (
                (self.filtered_redshifts[msm_name] < self.error_threshold)
                & (self.filtered_redshifts["lambda_" + msm_name] > lambda_range_min)
                & (self.filtered_redshifts["lambda_" + msm_name] < lambda_range_max)
            )
            delta_zmis = self.filtered_redshifts.iloc[0].delta_zmis
            self.filtered_redshifts.loc[mask, "mismatches"] = delta_zmis
            self.relzerr_to_mismatch_name[delta_zmis] = msm_name
            self.filtered_redshifts.loc[mask, "mismatches_spurious_success"] = msm_name
            self.filtered_redshifts.loc[mask, "mismatch_class"] = "mismatch"
            self.filtered_redshifts.loc[mask, "deltaz_from_lambda"] = 1 - (
                row.MismatchWaveLength / row.TrueWaveLength
            )
            logger = logging.getLogger("session_logger")
            logger.info(
                "testing " + msm_name + " deltaz = " + str(1 - (row.MismatchWaveLength / row.TrueWaveLength))
            )
            self.filtered_redshifts.drop([msm_name, "lambda_" + msm_name], inplace=True, axis=1)

        self.filtered_redshifts.drop(["z_mis", "delta_zmis"], axis=1, inplace=True)

    def define_mismatch_spurious_success_column(self):
        # merge mismatches info here
        self.filtered_redshifts["mismatches"] = -100.0
        self.define_mismatch_column_v2()
        mask = self.filtered_redshifts["abs_DeltaZ"] < self.error_threshold
        self.filtered_redshifts.loc[mask, "mismatches"] = 0.0
        self.filtered_redshifts.loc[mask, "mismatch_class"] = "success"
        self.filtered_redshifts.loc[mask, "mismatches_spurious_success"] = "success"
        mask = self.filtered_redshifts["mismatches"] == -100.0
        self.filtered_redshifts.loc[mask, "mismatch_class"] = "spurious"
        self.filtered_redshifts.loc[mask, "mismatches_spurious_success"] = "spurious"
        self.filtered_redshifts.drop("mismatches", inplace=True, axis=1)
        self.filtered_redshifts["DeltaZAlt"] = (
            self.filtered_redshifts.Redshift - self.filtered_redshifts["ref.Redshift"]
        ) / (1 + self.filtered_redshifts.Redshift)
        self.filtered_redshifts["DeltaZAlt"] = self.filtered_redshifts["DeltaZAlt"].replace(-np.inf, -10)

    def get_results_summary(self, features=None):
        """

        Args:
            features (list[str]) : columns to extract from self.redshifts

        Returns:
            dict[str] : map with entries defined by features
        """
        # TODO define "category" dtypes before importing csv
        self.load_results_summary()
        ret = dict()
        results = self.redshifts

        for col in results.columns:
            if features is not None:
                if col in features:
                    ret[col] = results[col].to_list()
            else:
                ret[col] = results[col].to_list()
        # replace original #Spectrum columns (which is the root file name of the spectrum) by processing ID
        if features is None or "SpectrumID" in features:
            ret["SpectrumID"] = results.index.to_list()

        return ret

    def get_results_sum_part(self, page, size, sorters, filters):
        """Get a page of results

        Args:
            page (int): page number
            size (int): page size
            sorters (list[dict[str]]: list of sorters, defined by "field" (column name)
                                      and "dir", sort direction (= "asc" or "desc")
            filters (list[dict[str]]: list of filters, defined by "field"" (column name),
                                      "type" (operator, takes value in ["<",">","=","in"]

        Returns:
            dict[str] one entry "records" containing a dict with all columns
        """
        results = self.redshifts
        if filters is not None:
            results = self.filter_results(filters)
        if sorters is not None and len(sorters) > 0:
            results = results.sort_values(by=sorters[0]["field"], ascending=sorters[0]["dir"] == "asc")
        results = results.iloc[(page - 1) * size : page * size]
        res = results.to_dict(orient="records")

        for r in res:
            for k, v in r.items():
                if type(v) is float:
                    if math.isnan(v) or math.isinf(v):
                        r[k] = None
        return res

    def get_results_size(self):
        """

        Returns:
            Size of the results after being merged with the reference (if any)
        """
        return self.redshifts.index.size

    def get_filtered_results_size(self):
        """

        Returns:
            Size of filtered results, equal to number of results if there is no filter
        """
        return self.filtered_redshifts.index.size

    def get_filtered_results(self, filters, features):
        """Get filtered results

        Args:
            filters(list[dict[str]]: list of filters, defined by "field"" (column name),
                                     "type" (operator, takes value in ["<",">","=","in"]
            features(list[str]): list of columns to retrieve

        Returns:
            dict[string] with entries defined by features
        """

        results = results.replace([np.inf, -np.inf], np.nan)
        ret = dict()
        for col in results.columns:
            if features is not None:
                if col in features:
                    results = results[results[col].notnull()]
        for col in results.columns:
            if features is not None:
                if col in features:
                    ret[col] = results[col].to_list()
            else:
                ret[col] = results[col].to_list()
        # replace original #Spectrum columns (which is the root file name of the spectrum) by processing ID
        if features is None or "SpectrumID" in features:
            ret["SpectrumID"] = results.index.to_list()

        return ret

    def get_results_groups(self, columns, on_filtered_data=True, bin_size=20):
        """Get histo and detailed stats about redshihft errors for every bin

        Args:
            columns (list[str]): list of columns on which histograms are computed
            on_filtered_data(bool): do we want to process on filtered data or not
            bin_size: number of histogram bins to be used

        Returns:
            dict[str,dict[str,object]] for each column, we have a dictionnary with following entries:
            min,max, step (width of a bin), data (list of <column_type>, value of the data for each bin),
            count (list of integers corresponding to the size of each bin)
            if the column type is not numeric, min, max and step are set to None. If the column is numeric,
            data corresponds to the lower bound of the bin.
        """
        ret = dict()
        if on_filtered_data:
            results = self.filtered_redshifts
        else:
            results = self.redshifts
        ret["total_count"] = int(results.index.size)
        ret["filtered_count"] = int(results.index.size)
        availableTypes = ["float", "double", "int", "float64", "int64", "float32"]
        for column in columns:
            ret[column] = dict()
            ret[column]["count"] = []
            enumLimit = 20
            if column in self.get_enumerable_columns(enumLimit):
                coltype = "enumerable"
            else:
                coltype = results[column].dtype

            if coltype in availableTypes:
                if results.index.size == 0:
                    ret[column]["min"] = None
                    ret[column]["max"] = None
                    ret[column]["step"] = None
                    ret[column]["data"] = []
                else:
                    r = results[column]
                    ret[column]["nan_count"] = int(r.isna().sum())
                    ret[column]["inf_count"] = int(r.isin([np.inf, -np.inf]).values.sum())
                    r = r.replace([np.inf, -np.inf], np.nan)
                    r = r[~np.isnan(r)]
                    if not r.empty:
                        histo = np.histogram(r, bins=bin_size)
                        ret[column]["data"] = histo[1].tolist()
                        c_min = float(r.min())
                        c_max = float(r.max())
                        ret[column]["min"] = c_min
                        ret[column]["max"] = c_max
                        ret[column]["step"] = (c_max - c_min) / (bin_size - 1)
                        ret[column]["mean"] = r.mean()
                        ret[column]["median"] = r.median()
                        ret[column]["sigma"] = r.std()
                        ret[column]["count"] = histo[0].tolist()
                        if not ret[column]["data"]:
                            ret[column]["min"] = None
                            ret[column]["max"] = None
                            ret[column]["step"] = None
                            ret[column]["mean"] = None
                            ret[column]["median"] = None
                            ret[column]["sigma"] = None
                    else:
                        ret[column]["data"] = None
                        ret[column]["min"] = None
                        ret[column]["max"] = None
                        ret[column]["step"] = None
                        ret[column]["mean"] = None
                        ret[column]["median"] = None
                        ret[column]["sigma"] = None
                        ret[column]["count"] = None
            # TODO drop "criterion + _bin" column
            # gr[0] is the value of criterion_bin, it can go from 1 to bin_size
            # in data we put the lower bound of the bin, hence gr[0]-1
            else:
                grs = results.groupby(column)
                ret[column]["data"] = list(grs.groups.keys())
                ret[column]["count"] = [g.size for g in grs.groups.values()]

        return ret

    def get_error_distribution_by_feature(self, column, on_filtered_data=True, nb_sigma=8, bin_size=20):
        """Get histo and detailed stats about redshihft errors for every bin

        Args:
            column (str): list of columns on which histograms are computed
            on_filtered_data(bool): do we want to process on filtered data or not
            bin_size: number of histogram bins to be used

        Returns:
            dict[str,dict[str,object]] for each column, we have a dictionnary with following entries:
            min,max, step (width of a bin), data (list of <column_type>, value of the data for each bin),
            count (list of integers corresponding to the size of each bin)
            if the column type is not numeric, min, max and step are set to None. If the column is numeric,
            data corresponds to the lower bound of the bin.
        """
        ret = dict()
        results = self.filtered_redshifts

        ret[column] = dict()
        enumLimit = 20
        if column in self.get_enumerable_columns(enumLimit):
            colType = "enumerable"
        else:
            colType = results[column].dtype
        availableTypes = [
            np.dtype("float"),
            np.dtype("float32"),
            np.dtype("float64"),
            np.dtype("double"),
            np.dtype("int"),
            np.dtype("int64"),
        ]
        if colType in availableTypes:
            results = results[results[column].notnull()]
            if results.index.size == 0:
                ret[column]["min"] = None
                ret[column]["max"] = None
                ret[column]["step"] = None
                ret[column]["data"] = []
            else:
                c_min = float(results[column].min())
                c_max = float(results[column].max())
                if c_max != c_min:
                    a = (bin_size - 1) / (c_max - c_min)
                    b = 1 - a * c_min
                    results[column + "_bin"] = (a * results[column] + b).apply(math.floor)
                else:
                    results[column + "_bin"] = c_min
                ret[column]["min"] = c_min
                ret[column]["max"] = c_max
                ret[column]["step"] = (c_max - c_min) / (bin_size - 1)
                ret[column]["data"] = []

        catastrophic = results[results["abs_DeltaZ"] > nb_sigma * self.redshift_error_sigma]
        ret["outliers_rate"] = catastrophic.index.size * 100.0 / results.index.size

        # if not self.filtered_redshifts.empty and on_filtered_data:
        #     ret["filtered_count"] = int(results.index.size)
        #     ret["total_count"] = int(self.redshifts.index.size)
        # else:
        #
        #     ret["filtered_count"] = int(results.index.size)

        # TODO drop "criterion + _bin" column
        # gr[0] is the value of criterion_bin, it can go from 1 to bin_size
        # in data we put the lower bound of the bin, hence gr[0]-1

        ret["total_count"] = int(results.index.size)
        clean_results = results[results["abs_DeltaZ"] <= nb_sigma * self.redshift_error_sigma]
        ret["filtered_count"] = int(clean_results.index.size)
        if clean_results.empty:
            raise Exception(
                f"No results with |deltaZ|< {nb_sigma}*{self.redshift_error_sigma}={nb_sigma * self.redshift_error_sigma}"
            )
        if colType in availableTypes:
            clean_grs = clean_results.groupby(column + "_bin")
            for gr in clean_grs:
                ret[column]["data"].append(c_min + (gr[0] - 1) * (c_max - c_min) / (bin_size - 1))
        else:
            clean_grs = clean_results.groupby(column)

        ret[column]["count"] = []

        ret[column]["relative_error"] = dict()

        if (colType in availableTypes) and results.index.size > 0:
            grs = results.groupby(column + "_bin")
        else:
            grs = results.groupby(column)
            ret[column]["data"] = list(grs.groups.keys())

        for s in ["median", "min", "max", "q1", "q3", "d1", "d10"]:
            ret[column]["relative_error"][s] = []
        ret[column]["inliers_rate"] = []
        for g in grs:
            full_ig = g[1].index
            clean_ig = full_ig.intersection(clean_results.index)
            inliers_rate = 100 - (full_ig.size - clean_ig.size) * 100 / full_ig.size
            ret[column]["inliers_rate"].append(inliers_rate)
            ret[column]["count"].append(clean_ig.size)
            if not clean_ig.empty:
                ret[column]["relative_error"]["median"].append(clean_results.loc[clean_ig]["DeltaZ"].median())
                ret[column]["relative_error"]["min"].append(clean_results.loc[clean_ig]["DeltaZ"].min())
                ret[column]["relative_error"]["max"].append(clean_results.loc[clean_ig]["DeltaZ"].max())
                ret[column]["relative_error"]["q1"].append(
                    clean_results.loc[clean_ig]["DeltaZ"].quantile(0.25)
                )
                ret[column]["relative_error"]["q3"].append(
                    clean_results.loc[clean_ig]["DeltaZ"].quantile(0.75)
                )
                ret[column]["relative_error"]["d1"].append(
                    clean_results.loc[clean_ig]["DeltaZ"].quantile(0.1)
                )
                ret[column]["relative_error"]["d10"].append(
                    clean_results.loc[clean_ig]["DeltaZ"].quantile(0.9)
                )
            else:
                for s in ["median", "min", "max", "q1", "q3", "d1", "d10"]:
                    ret[column]["relative_error"][s].append(None)

        return ret

    def get_performance_matrix(self, line, error_threshold):
        results = self.redshifts
        results["redshift_bin"] = (2 * results["galaxy.Redshift"]).apply(math.floor)
        results["trueLf" + line + "_bin"] = (2 * results["ref.lf" + line]).apply(math.floor)
        results["mag_bin"] = (2 * results["mag"]).apply(math.floor)
        redshift_grs = results.groupby(["redshift_bin"])
        min_max_grs = redshift_grs.agg(
            {"trueLf" + line + "_bin": [np.min, np.max], "mag_bin": [np.min, np.max]}
        )
        dres = dict()
        for index, row in min_max_grs.iterrows():
            dres[index / 2] = {
                "Lf"
                + line
                + "_range": [
                    row["trueLf" + line + "_bin"][0] / 2,
                    row["trueLf" + line + "_bin"][1] / 2,
                ],
                "mag_range": [row["mag_bin"][0] / 2, row["mag_bin"][1] / 2],
                "good_rate": [],
                "total_size": [],
            }

        grs = results.groupby(["redshift_bin", "mag_bin", "trueLf" + line + "_bin"]).groups
        good_results = results[results["DeltaZ"] < error_threshold]
        good_grs = good_results.groupby(["redshift_bin", "mag_bin", "trueLf" + line + "_bin"]).groups

        for r in results["redshift_bin"].unique():
            for mag in range(
                int(dres[r / 2]["mag_range"][0] * 2),
                int(dres[r / 2]["mag_range"][1] * 2),
            ):
                gr = []
                ts = []
                for lf in range(
                    int(dres[r / 2]["Lf" + line + "_range"][0] * 2),
                    int(dres[r / 2]["Lf" + line + "_range"][1] * 2),
                ):
                    key = (r, mag, lf)
                    if key in grs.keys():
                        ts.append(grs[key].size)
                        if key in good_grs.keys():
                            gr.append(good_grs[key].size * 100.0 / grs[key].size)
                        else:
                            gr.append(0)
                    else:
                        ts.append(0)
                        gr.append(0)
                dres[r / 2]["good_rate"].append(gr)
                dres[r / 2]["total_size"].append(ts)

        res = []
        for r in results["redshift_bin"].unique():
            v = dres[r / 2]
            v["redshift_range"] = [r / 2, r / 2 + 1 / 2]
            res.append(v)
        return res

    def get_relzerr_groups(self):
        if not self.filtered_redshifts.empty:
            results = self.filtered_redshifts
        else:
            results = self.redshifts
        ret = dict()

        grs = results.groupby("mismatches")

        ret["success"] = dict()
        ret["mismatch"] = dict()
        ret["spurious"] = dict()

        ret["success"]["relative_z_error"] = []
        ret["mismatch"]["relative_z_error"] = []
        ret["spurious"]["relative_z_error"] = []

        ret["success"]["count"] = []
        ret["mismatch"]["count"] = []
        ret["spurious"]["count"] = []

        ret["mismatch"]["true_line"] = []
        ret["mismatch"]["calc_line"] = []

        for relzerr, gv in zip(grs.groups.keys(), grs.groups.values()):
            if relzerr in self.relzerr_to_mismatch_name.keys():
                ret["mismatch"]["true_line"].append(
                    #                    mismatch_catalog[mismatch_catalog["relative_z_error"] == relzerr].true_line.iloc[0])
                    self.relzerr_to_mismatch_name[relzerr].split("_")[0]
                )
                ret["mismatch"]["calc_line"].append(
                    #                   mismatch_catalog[mismatch_catalog["relative_z_error"] == relzerr].calc_line.iloc[0])
                    self.relzerr_to_mismatch_name[relzerr].split("_")[1]
                )
                ret["mismatch"]["relative_z_error"].append(relzerr)
                ret["mismatch"]["count"].append(gv.size)
            elif relzerr == 0:
                ret["success"]["relative_z_error"].append(0)
                ret["success"]["count"].append(gv.size)
            else:
                ret["spurious"]["relative_z_error"].append(relzerr)
                ret["spurious"]["count"].append(gv.size)

        return ret

    def get_relzerr_distribution_on_classes(self, relzerr_interval, nbins=100):
        results = self.filtered_redshifts

        ret = dict()

        if not relzerr_interval[0] or not relzerr_interval[1]:
            relzerr_interval = [results.DeltaZAlt.min(), results.DeltaZAlt.max()]

        classes = ["success", "mismatch", "spurious"]
        for class_ in classes:
            ret[class_] = dict()
            class_res = results[results.mismatch_class == class_]
            histo = np.histogram(class_res.DeltaZAlt, bins=nbins, range=relzerr_interval)
            ret[class_]["relative_z_error"] = []
            relzerrs = histo[1].tolist()
            for i in range(len(relzerrs) - 1):
                ret[class_]["relative_z_error"].append((relzerrs[i + 1] + relzerrs[i]) / 2)
            ret[class_]["count"] = histo[0].tolist()

        return ret

    def get_mismatches(self):
        if not self.filtered_redshifts.empty:
            results = self.filtered_redshifts
        else:
            results = self.redshifts
        ret = dict()
        ret["mismatch_name"] = []
        ret["relative_z_error"] = []
        ret["count"] = []
        ret["relative_z_error_interval"] = [
            results.DeltaZAlt.min(),
            results.DeltaZAlt.max(),
        ]
        results = results[results.mismatches_spurious_success != "success"]
        results = results[results.mismatches_spurious_success != "spurious"]

        grs = results.groupby("mismatches_spurious_success")
        grs_deltaz_mean = grs.deltaz_from_lambda.mean()
        for mismatch, gv in zip(grs.groups.keys(), grs.groups.values()):
            ret["mismatch_name"].append(mismatch)
            ret["relative_z_error"].append(grs_deltaz_mean[mismatch])
            ret["count"].append(gv.size)

        return ret

    def set_error_threshold(self, error_threshold):
        self.error_threshold = error_threshold

        self.define_mismatch_spurious_success_column()

    def get_sigma_clipping_around(self):
        return self.sigma_clipping_around

    def set_sigma_clipping_around(self, sigma_clipping_around):
        self.sigma_clipping_around = sigma_clipping_around

        self.compute_sigma_clipping(self.filtered_redshifts)
        self.define_mismatch_spurious_success_column()

    def get_success_rate_cumulated(self, on_filtered_data=True, nb_points=2000):

        if on_filtered_data:
            rs = self.filtered_redshifts
        else:
            rs = self.redshifts

        rs = rs.sort_values(by=["abs_DeltaZ"], ascending=True)
        rs["new_idx"] = np.arange(0, rs.index.size)
        relzerr_range = []
        success_rate = []

        for i in range(nb_points):
            number_below = (i + 1) * rs.index.size / nb_points
            index = min(math.floor(number_below), rs.index.size - 1)
            relzerr_range.append(rs[rs["new_idx"] == index].abs_DeltaZ.iloc[0])
            success_rate.append(100 * (number_below / rs.index.size))

        ret = dict()
        ret["relzerr"] = relzerr_range
        ret["rate_below"] = success_rate

        return ret

    def get_reference_redshift(self, spectrum_id):
        return self.redshifts[self.redshifts.index == spectrum_id]["ref.Redshift"].iloc[0]

    def get_first_spectrum_id(self):
        return self.redshifts.index[0]

    def get_enumerable_columns_values(self, enumLimit: int):
        self.load_results_summary()

        enumerable_values_path = os.path.join(self.output_directory, "enumerable_values.json")
        if os.path.exists(enumerable_values_path):
            with open(enumerable_values_path) as f:
                return json.load(f)
        ret = dict()
        for col in self.redshifts.columns:
            dtype = self.redshifts[col].dtype
            nonEnumerableCols = ["ProcessingID", "BunchID", "SpectrumID", "AstronomicalSourceID", "subdir"]
            if col in nonEnumerableCols:
                continue
            elif dtype == object:
                ret[col] = pd.unique(self.redshifts[col].dropna()).tolist()
            elif len(pd.unique(self.redshifts[col].dropna()).tolist()) > enumLimit:
                continue
            elif dtype == np.int64:
                ret[col] = pd.unique(self.redshifts[col].dropna()).tolist()
        # Saving enum values as cache only if a reference was provided
        if self.reference_path:
            try:
                with open(enumerable_values_path, "w") as f:
                    json.dump(ret, f)
            except:
                logger = logging.getLogger("session_logger")
                logger.warning(f"Could not write enumerable values to {self.output_directory}")
        return ret

    def get_enumerable_columns(self, enumLimit: int):
        self.load_results_summary()

        ret = []
        for col in self.redshifts.columns:
            dtype = self.redshifts[col].dtype
            nonEnumerableCols = [
                "ProcessingID",
                "BunchID",
                "SpectrumID",
                "AstronomicalSourceID",
            ]
            if col in nonEnumerableCols or dtype == np.float64:
                continue
            elif dtype == object:
                ret.append(col)
            elif len(pd.unique(self.redshifts[col].dropna()).tolist()) > enumLimit:
                continue
            elif dtype == np.int64:
                ret.append(col)
        return ret

    def get_redshift_error_distribution(self, on_filtered_data=True, nbins=30, nb_sigma=5):
        if on_filtered_data:
            rs = self.filtered_redshifts
        else:
            rs = self.redshifts

        err_min = self.redshift_error_mean - nb_sigma * self.redshift_error_sigma
        err_max = self.redshift_error_mean + nb_sigma * self.redshift_error_sigma

        err_dist = dict()
        histo = np.histogram(rs["DeltaZ"], bins=nbins, range=[err_min, err_max])  # , density=True)

        count_percentage = histo[0] * 100.0 / histo[0].sum()
        err_dist["count"] = count_percentage.tolist()
        err_dist["DeltaZ"] = histo[1].tolist()
        x = np.linspace(err_min, err_max, nbins)
        # gauss = stats.norm.pdf(x, redshift_error_mean, redshift_error_sigma)
        gauss = np.exp(
            -0.5 * np.square((x - self.redshift_error_mean) / self.redshift_error_sigma)
        ) / np.sqrt(2 * np.pi * self.redshift_error_sigma**2)
        gauss *= count_percentage.sum() / gauss.sum()
        err_dist["gauss"] = gauss.tolist()

        return err_dist

    def get_confusion_matrix(self, colA, colB):

        rs = self.filtered_redshifts

        grs = rs.groupby([colA, colB]).count()
        if rs[colA].dtype == np.dtype("int64"):
            xl = list(map(int, rs[colA].unique()))
        else:
            xl = list(rs[colA].unique())
        if rs[colB].dtype == np.dtype("int64"):
            yl = list(map(int, rs[colB].unique()))
        else:
            yl = list(rs[colB].unique())

        xl = list(filter(lambda x: x is not np.nan, rs[colA].unique()))
        yl = list(filter(lambda y: y is not np.nan, rs[colB].unique()))
        xl.sort()
        yl.sort()
        none_x = int(rs[colA].isna().sum())
        none_y = int(rs[colB].isna().sum())
        l = []
        percent = []

        for y in yl:
            subl = []
            subp = []
            for x in xl:
                try:
                    n = int(grs.loc[(x, y), "SpectrumID"])
                    subl.append(n)
                    subp.append(n / len(rs))
                except:
                    subl.append(0)
                    subp.append(0)
            l.append(subl)
            percent.append(subp)
        return {colA: xl, colB: yl, "z": l, "x_none": none_x, "y_none": none_y, "percent": percent}
