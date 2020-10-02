from collections import namedtuple
import os.path
from astropy.io import fits
import logging
import numpy as np
import pandas as pd

from drp_1dpipe.io.writer import write_candidates, write_dummy
from drp_1dpipe.core.utils import TemporaryFilesSet

from pfs.datamodel.drp import PfsObject

RedshiftResult = namedtuple('RedshiftResult',
                            ['spectrum', 'processingid', 'redshift', 'merit',
                             'template', 'method', 'deltaz', 'reliability',
                             'snrha', 'lfha', 'snroII', 'lfoII', 'type_'])

RedshiftCandidate = namedtuple('RedshiftCandidate',
                               ['rank', 'ids', 'redshift', 'intgProba',
                               'rank_pdf', 'deltaz', 'gaussAmp', 'gaussAmpErr',
                               'gaussSigma', 'gaussSigmaErr'])

Classification = namedtuple('Classification',
                            ['type', 'evidenceG', 'evidenceS', 'evidenceQ'])
#                            ['type', 'merit', 'evidenceG', 'evidenceS', 'svidenceQ']) # TODO: To review after fix #5639

LineMeasurement = namedtuple('LineMeasurement',
                             ['type', 'force', 'name', 'elt_id',
                              'lambda_rest_beforeOffset', 'lambda_obs', 'amp',
                              'err', 'err_fit', 'fit_group', 'velocity',
                              'offset', 'sigma', 'flux', 'flux_err', 'flux_di',
                              'center_cont_flux', 'cont_err'])

StarCandidate = namedtuple('StarCandidate',
                           ['redshift', 'intgProba', 'evidenceLog', 'template'])

redshift_file_type_map = (str, str, float, float,
                          str, str, float, str,
                          float, float, float, float,
                          str)

classification_file_type_map = (str, float, float, float) # TODO: To review after fix #5639

candidates_file_type_map = (int, str, float, float,
                            int, float, float, float,
                            float, float)

linemeas_file_type_map = (str, str, str, int, float,
                          float, float, float, float,
                          lambda x: None if x == '-1' else x,
                          float, float, float,
                          float, float, float, float,
                          float)

starCandidate_file_type_map = (float, float, float, str)

redshift_header = ["#Spectrum", "ProcessingID", "Redshift", "Merit", "Template", "Method",
    "Deltaz", "Reliability", "snrHa", "lfHa", "snrOII", "lfOII", "Type"]

class SummaryFile:

    def __init__(self, output_dir=None):
        """Constructor for SpectrumResults

        Parameters
        ----------
        output_dir : `str`
            Output directory path

        Raises
        ------
        FileNotFoundError
            A FileNotFoundError exception is raised if 
            * output directory is None
            * output directory not found
        """
        if output_dir is None:
            raise FileNotFoundError("Output directory is None : {}")
        if not os.path.exists(output_dir):
            raise FileNotFoundError("No output directory detected for : {}".format(os.path.basename(output_dir)))
        self.output_dir = output_dir
        self.__summary_file_name__ = 'undefined'

    def read(self):
        """Build redshift_results.

        redshift_results is a dict RedshiftResult, keyed by spectrum
        file names.
        """
        path = os.path.join(self.output_dir, self.__summary_file_name__)
        if not os.path.exists(path):
            raise FileNotFoundError("No redshift summary file detected : {}".format(path))
        with open(path, 'r') as f:
                self.summary = []
                for l in f:
                    if not l.strip() or l.startswith('#'):
                        continue
                    _r = [f(x) for f, x in zip(redshift_file_type_map, l.split())]
                    candidate = RedshiftResult(*_r)
                    self.summary.append(candidate)
    
    def write(self):
        path = os.path.join(self.output_dir, self.__summary_file_name__)
        with open(path, 'w') as ff:
            header = "\t".join(redshift_header)+"\n"
            ff.write(header)
            for elt in self.summary:
                data = "\t".join([str(i) for i in elt._asdict().values()])+'\n'
                ff.write(data)

class RedshiftSummary(SummaryFile):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__summary_file_name__ = 'redshift.csv'

class StellarSummary(SummaryFile):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__summary_file_name__ = 'stellar.csv'

class QsoSummary(SummaryFile):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__summary_file_name__ = 'qso.csv'


class SpectrumResults:
    """A class for mapping spectrum results
    """

    def __init__(self, spectrum_path=None, output_dir=None, output_lines_dir=None, stellar="on", dummy=False):
        """Constructor for SpectrumResults

        Parameters
        ----------
        spectrum_path : `str`
            Path to spectrum file
        output_dir : `str`
            Output directory path
        output_lines_dir : `str`, optional
            Output directory path for lines measurement, by default None

        Raises
        ------
        FileNotFoundError
            A FileNotFoundError exception is raised if 
            * output directory is None
            * output directory not found
            * Spectrum file not found
            * output directory line not found
        """
        self.dummy = dummy

        if spectrum_path is not None:
            if not os.path.exists(spectrum_path):
                raise FileNotFoundError("No spectrum file detected for : {}".format(os.path.basename(self.output_dir)))
        self.spectrum_path = spectrum_path

        if not dummy:
            if output_dir is None:
                raise FileNotFoundError("Output directory is None : {}")
            if not os.path.exists(output_dir):
                raise FileNotFoundError("No output directory detected for : {}".format(os.path.basename(output_dir)))
            self.output_dir = output_dir
            if output_lines_dir is not None:
                if not os.path.exists(output_lines_dir):
                    raise FileNotFoundError("No output lines directory detected for : {}".format(os.path.basename(self.output_dir)))
            self.output_lines_dir = output_lines_dir
            self.stellar = stellar

    def _read_candidates(self):
        """Method used to read candidate file produced by amazed

        Raises
        ------
        FileNotFoundError
            A FileNotFoundError exception is raised if candidates file is not found
        """
        path = os.path.join(self.output_dir, 'candidatesresult.csv')
        if not os.path.exists(path):
            raise FileNotFoundError("No candidates file detected for : {}".format(os.path.basename(self.output_dir)))
        with open(path, 'r') as f:
                self.candidates = []
                for l in f:
                    if not l.strip() or l.startswith('#'):
                        continue
                    # rank	IDs	redshift	intgProba	Rank_PDF	Deltaz	gaussAmp_unused	gaussAmpErr_unused	gaussSigma_unused	gaussSigmaErr_unused
                    _r = [f(x) for f, x in zip(candidates_file_type_map, l.split())]
                    candidate = RedshiftCandidate(*_r)
                    self.candidates.append(candidate)

    def _read_models(self):
        """Method used to read models files produced by amazed

        Raises
        ------
        AttributeError
            An AttributeError exception is raised if no candidates is detected
        FileNotFoundError
            A FileNotFoundError exception is raised if models files are not found
        """
        models = []
        if not hasattr(self, 'candidates'):
            raise AttributeError("Unable to get model. No candidates detected for : {}".format(self.output_dir))
        for i, candidate in enumerate(self.candidates):
            path = os.path.join(self.output_dir, 'linemodelsolve.linemodel_spc_extrema_{}.csv'.format(i))
            if not os.path.exists(path):
                raise FileNotFoundError("No model file detected for : {} candidate number : {}".format(os.path.basename(self.output_dir), i))
            with open(path, 'r') as f:
                model = []
                for l in f:
                    if not l.strip() or l.startswith('#'):
                        continue
                    model.append(l.split()[1])
            models.append(model)
        self.models = np.array(models, dtype=np.float64)

    def _read_lambda_ranges(self):
        """Method used to read lambda vector from spectrum
        """
        obj = PfsObject.readFits(self.spectrum_path)
        self.lambda_ranges = obj.wavelength
        self.mask = obj.mask

    def _read_classification(self):
        """Method used to read classification file produced by amazed

        Raises
        ------
        FileNotFoundError
            A FileNotFoundError exception is raised if classification file is not found
        """
        path = os.path.join(self.output_dir, 'classificationresult.csv')
        if not os.path.exists(path):
            raise FileNotFoundError("No classification file detected for : {}".format(os.path.basename(self.output_dir)))
        with open(path, 'r') as f:
                self.classification = []
                for l in f:
                    if not l.strip() or l.startswith('#'):
                        continue
                    _r = [f(x) for f, x in zip(classification_file_type_map, l.split())]
                    self.classification = Classification(*_r)

    def _read_zpdf(self):
        """Method used to read pdf file produced by amazed

        Raises
        ------
        FileNotFoundError
            A FileNotFoundError exception is raised if pdf file is not found
        """
        path = os.path.join(self.output_dir, 'zPDF',
                            'logposterior.logMargP_Z_data.csv')
        if not os.path.exists(path):
            raise FileNotFoundError("No zPDF file detected for : {}".format(os.path.basename(self.output_dir)))
        with open(path, 'r') as f:
            pdf = []
            for l in f:
                if not l.strip() or l.startswith('#'):
                    continue
                pdf.append(l.split())
            self.zpdf = np.array(pdf, dtype=float)

    def _read_lines(self):
        """Method used to read lines file produced by amazed

        Raises
        ------
        FileNotFoundError
            A FileNotFoundError exception is raised if :
            * Output lines directory is None
            * Output lines files are not found
        """
        if self.output_lines_dir is None:
            raise FileNotFoundError("Output lines directory is None for : {}".format(os.path.basename(self.output_dir)))
        path = os.path.join(self.output_lines_dir,
                            'linemodelsolve.linemodel_fit_extrema_0.csv')
        if not os.path.exists(path):
            raise FileNotFoundError("No lines file detected for : {}".format(os.path.basename(self.output_dir)))
        with open(path, 'r') as f:
            lm = []
            for l in f:
                if not l.strip() or l.startswith('#'):
                    continue
                try:
                    _r = [f(x) for f, x in zip(linemeas_file_type_map,
                                                l.split())]
                    lm.append(LineMeasurement(*_r))
                except Exception as e:
                    logging.log(logging.CRITICAL,
                                "Can't parse line measurement : "
                                "{}: {}".format(e, l))
                    continue
            self.linemeas = lm

    def _read_star(self):
        """Read star result for each spectrum"""
        path = os.path.join(self.output_dir, 'stellarsolve.stellarresult.csv')
        if not os.path.exists(path):
            raise FileNotFoundError("No stellar candidates file detected for : {}".format(os.path.basename(self.output_dir)))
        with open(path, 'r') as f:
            num_line = 0
            for l in f:
                if not l.strip() or l.startswith('#'):
                    continue
                if num_line == 0 :
                    num_line = 1
                    continue
                else:
                    _r = [f(x) for f, x in zip( starCandidate_file_type_map, l.split())]
                    self.star_candidate = [StarCandidate(*_r)]
                    break

    def load(self):
        """Method used to load all results produced by amazed for one spectrum
        """
        # read classification
        self._read_classification()

        # read lambda ranges from spectrum files
        self._read_lambda_ranges()

        # read candidates from output dirs
        self._read_candidates()

        # read zPDF from output dirs
        self._read_zpdf()

        # read models from output dirs
        self._read_models()

        # read line measurement from output-lf dirs
        if self.output_lines_dir is not None:
            self._read_lines()

        if self.stellar.strip().lower() == 'only':
            self._read_star()
        else:
            try:
                self._read_star()
            except FileNotFoundError:
                pass
            
    def write(self, path):
        """Method used to write PFS product

        Parameters
        ----------
        path : `str`
            Output directory

        Returns
        -------
        `str`
            Name of product file
        """
        if not self.dummy:
            self.load()
            if self.classification.type == 'G' and self.stellar.strip().lower() != 'only':
                object_class = 'GALAXY'
                lambda_scale = self.lambda_ranges
                mask = self.mask
                candidates = self.candidates
                models = self.models
                zpdf = self.zpdf
                linemeas = (self.linemeas if self.output_lines_dir else None)
            elif self.classification.type == 'S' or self.stellar.strip().lower() == 'only':
                object_class = 'STAR'
                lambda_scale = None
                mask = None
                candidates = self.star_candidate
                models = None
                zpdf = None
                linemeas = None
            catId, tract, patch, objId, nvisit, pfsVisitHash = self._parse_pfsObject_name(os.path.basename(self.spectrum_path))
            filename = write_candidates(path,
                                        catId, tract, patch, objId, nvisit, pfsVisitHash,
                                        lambda_scale,
                                        mask,
                                        candidates,
                                        models,
                                        zpdf,
                                        linemeas,
                                        object_class)
            return filename
        else:
            catId, tract, patch, objId, nvisit, pfsVisitHash = self._parse_pfsObject_name(
                os.path.basename(self.spectrum_path))
            filename = write_dummy(path, catId, tract, patch, objId, nvisit, pfsVisitHash)

    @staticmethod
    def _parse_pfsObject_name(name):
        """Parse a pfsObject file name.

        Template is : pfsObject-%05d-%05d-%s-%016x-%03d-0x%016x.fits
        pfsObject-%(catId)05d-%(tract)05d-%(patch)s-%(objId)016x-%(nVisit % 1000)03d-0x%(pfsVisitHash)016x.fits
        """
        basename = os.path.splitext(name)[0]
        head, catId, tract, patch, objId, nvisit, pfsVisitHash = basename.split('-')
        assert head == 'pfsObject'
        return (int(catId), int(tract), patch, int(objId, 16), int(nvisit), int(pfsVisitHash, 16))

# class AmazedResults:
#     """
#     An object representation of an amazed output directory.
#     """

#     def __init__(self, output_dir, spectrum_dir, lineflux=True,
#                  tmpcontext=None):
#         """
#         :param output_dir: Amazed output directory
#         :param spectrum_dir: Base path to spectrums
#         :param lineflux: Whether we have line flux measurements
#         :param tmpcontext: TemporaryFileSet context to use
#         """

#         self.output_dir = output_dir
#         self.spectrum_dir = spectrum_dir
#         self.redshift_results = {}
#         self.lambda_ranges = {}
#         self.candidates = {}
#         self.zpdf = {}
#         self.linemeas = {}
#         self.lineflux = lineflux
#         if tmpcontext is None:
#             self.tmpcontext = TemporaryFilesSet()
#         else:
#             self.tmpcontext = tmpcontext

#     def load(self):
        
#         # Start by reading redshifts.csv
#         self._read_redshifts_csv()

#         # read lambda ranges from spectrum files
#         self._read_lambda_ranges()

#         # read candidates from output dirs
#         self._read_candidates()

#         # read zPDF from output dirs
#         self._read_zPDF()

#         # read model from output dirs
#         # self._read_models()

#         # read line measurement from output-lf dirs
#         if self.lineflux:
#             self._read_linemeas()

#     def write(self):
#         created_files = []
#         self.load()
#         for spectrum, results in self.redshift_results.items():
#             catId, tract, patch, objId, nvisit, pfsVisitHash = \
#                 self._parse_pfsObject_name(spectrum)
#             filename = write_candidates(self.output_dir,
#                                 catId, tract, patch, objId, nvisit, pfsVisitHash,
#                                 self.lambda_ranges[spectrum],
#                                 self.redshift_results[spectrum],
#                                 self.candidates[spectrum],
#                                 self.zpdf[spectrum],
#                                 (self.linemeas[spectrum]
#                                 if self.lineflux
#                                 else None))
#             created_files.append(filename)
#         return created_files

#     def _read_redshifts_csv(self):
#         """Build redshift_results.

#         redshift_results is a dict RedshiftResult, keyed by spectrum
#         file names.
#         """
#         redshift_file = os.path.join(self.output_dir, 'redshift.csv')
#         # self.tmpcontext.add_files(redshift_file)
#         with open(redshift_file, 'r') as f:
#             for l in f:
#                 if not l.strip() or l.startswith('#'):
#                     continue
#                 try:
#                     _r = [f(x) for f, x in zip(redshift_file_type_map,
#                                                l.split())]
#                     result = RedshiftResult(*_r)
#                 except Exception as e:
#                     logging.log(logging.CRITICAL,
#                                 "Can't parse result : {}: {}".format(e, l))
#                     continue
#                 else:
#                     self.redshift_results[result.spectrum] = result
#                     self.tmpcontext.add_dirs(os.path.join(self.output_dir,
#                                                           result.processingid))

#     def _read_lambda_ranges(self):
#         """Read lambda ranges from spectrum fits files.

#         lambda_ranges is a dict of lambda ranges, keyed by spectrum file names.
#         """
#         for spectrum, result in self.redshift_results.items():
#             path = os.path.join(self.spectrum_dir, spectrum)
#             obj = PfsObject.readFits(path)
#             self.lambda_ranges[spectrum] = obj.wavelength

#     def _read_candidates(self):
#         """Read redshift candidates from candidatesresult.csv."""
#         for result in self.redshift_results.values():
#             path = os.path.join(self.output_dir, result.processingid,
#                                 'candidatesresult.csv')
#             with open(path, 'r') as f:
#                 self.candidates[result.spectrum] = []
#                 for l in f:
#                     if not l.strip() or l.startswith('#'):
#                         continue
#                     # rank	IDs	redshift	intgProba	Rank_PDF	Deltaz	gaussAmp_unused	gaussAmpErr_unused	gaussSigma_unused	gaussSigmaErr_unused
#                     _r = [f(x) for f, x in zip(candidates_file_type_map,
#                                                l.split())]
#                     candidate = RedshiftCandidate(*_r)
#                     self.candidates[result.spectrum].append(candidate)

#     def _read_zPDF(self):
#         """Read zPDF for each spectrum."""
#         for result in self.redshift_results.values():
#             path = os.path.join(self.output_dir, result.processingid,
#                                 'zPDF',
#                                 'logposterior.logMargP_Z_data.csv')
#             with open(path, 'r') as f:
#                 pdf = []
#                 for l in f:
#                     if not l.strip() or l.startswith('#'):
#                         continue
#                     pdf.append(l.split())
#                 self.zpdf[result.spectrum] = np.array(pdf, dtype=float)

#     def _read_models(self):
#         """Read zPDF for each spectrum."""
#         for result in self.redshift_results.values():
#             path = os.path.join(self.output_dir, result.processingid,
#                                 'zPDF',
#                                 'logposterior.logMargP_Z_data.csv')
#             with open(path, 'r') as f:
#                 pdf = []
#                 for l in f:
#                     if not l.strip() or l.startswith('#'):
#                         continue
#                     pdf.append(l.split())
#                 self.zpdf[result.spectrum] = np.array(pdf, dtype=float)

#     def _read_linemeas(self):
#         """Read line measurement for each spectrum."""
#         for result in self.redshift_results.values():
#             path = os.path.join('-'.join([self.output_dir, 'lf']),
#                                 result.processingid,
#                                 'linemodelsolve.linemodel_fit_extrema_0.csv')
#             self.tmpcontext.add_dirs(os.path.join('-'.join([self.output_dir,
#                                                             'lf'])))
#             with open(path, 'r') as f:
#                 lm = []
#                 for l in f:
#                     if not l.strip() or l.startswith('#'):
#                         continue
#                     try:
#                         _r = [f(x) for f, x in zip(linemeas_file_type_map,
#                                                    l.split())]
#                         lm.append(LineMeasurement(*_r))
#                     except Exception as e:
#                         logging.log(logging.CRITICAL,
#                                     "Can't parse line measurement : "
#                                     "{}: {}".format(e, l))
#                         continue
#                 self.linemeas[result.spectrum] = lm

#     @staticmethod
#     def _parse_pfsObject_name(name):
#         """Parse a pfsObject file name.

#         Template is : pfsObject-%03d-%05d-%s-%016x-%03d-0x%016x.fits
#         pfsObject-%(catId)03d-%(tract)05d-%(patch)s-%(objId)016x-%(nVisit % 1000)03d-0x%(pfsVisitHash)016x.fits
#         """
#         basename = os.path.splitext(name)[0]
#         head, catId, tract, patch, objId, nvisit, pfsVisitHash = basename.split('-')
#         assert head == 'pfsObject'
#         return (int(catId), int(tract), patch, int(objId, 16), int(nvisit), int(pfsVisitHash, 16))
