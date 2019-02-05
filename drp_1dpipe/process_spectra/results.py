from collections import namedtuple
import os.path
import fitsio
from drp_1dpipe.io.writer import write_candidates
import logging
import numpy as np

RedshiftResult = namedtuple('RedshiftResult',
                            ['spectrum', 'processingid', 'redshift', 'merit',
                             'template', 'method', 'deltaz', 'reliability',
                             'snrha', 'lfha', 'snroII', 'lfoII', 'type_'])

RedshiftCandidate = namedtuple('RedshiftCandidate',
                               ['rank', 'redshift', 'intgProba', 'gaussAmp',
                                'gaussAmpErr', 'gaussSigma', 'gaussSigmaErr'])

LineMeasurement = namedtuple('LineMeasurement',
                             ['type', 'force', 'name', 'elt_id',
                              'lambda_rest_beforeOffset', 'lambda_obs', 'amp',
                              'err', 'err_fit', 'fit_group', 'velocity',
                              'offset', 'sigma', 'flux', 'flux_err', 'flux_di',
                              'center_cont_flux', 'cont_err_'])


class AmazedResults:
    """
    An object representation of an amazed output directory.
    """

    def __init__(self, output_dir, spectrum_dir):
        """
        :param output_dir: Amazed output directory
        :param spectrum_dir: Base path to spectrums
        """

        self.output_dir = output_dir
        self.spectrum_dir = spectrum_dir
        self.redshift_results = {}
        self.lambda_ranges = {}
        self.candidates = {}
        self.zpdf = {}
        self.linemeas = {}

        # Start by reading redshifts.csv
        self._read_redshifts_csv()

        # read lambda ranges from spectrum files
        self._read_lambda_ranges()

        # read candidates from output dirs
        self._read_candidates()

        # read zPDF from output dirs
        self._read_zPDF()

        # read line measurement from output-lf dirs
        self._read_linemeas()

    def write(self):
        for spectrum, results in self.redshift_results.items():
            tract, patch, catId, objId, \
                nVisit, pfsVisitHash = self._parse_pfsObject_name(spectrum)
            write_candidates(self.output_dir,
                             tract, patch, catId, objId, nVisit, pfsVisitHash,
                             self.lambda_ranges[spectrum],
                             self.redshift_results[spectrum],
                             self.candidates[spectrum],
                             self.zpdf[spectrum],
                             self.linemeas[spectrum])

    def _read_redshifts_csv(self):
        """Build redshift_results.

        redshift_results is a dict RedshiftResult, keyed by spectrum
        file names.
        """
        with open(os.path.join(self.output_dir, 'redshift.csv'), 'r') as f:
            for l in f:
                if not l.strip() or l.startswith('#'):
                    continue
                try:
                    _r = [f(x) for f, x in zip((str, str, float, float,
                                                str, str, float, str,
                                                float, float, float, float,
                                                str),
                                               l.split())]
                    result = RedshiftResult(*_r)
                except Exception as e:
                    logging.log(logging.CRITICAL,
                                "Can't parse result : {}: {}".format(e, l))
                    continue
                else:
                    self.redshift_results[result.spectrum] = result

    def _read_lambda_ranges(self):
        """Read lambda ranges from spectrum fits files.

        lambda_ranges is a dict of lambda ranges, keyed by spectrum file names.
        """
        for spectrum, result in self.redshift_results.items():
            path = os.path.join(self.spectrum_dir, spectrum)
            fits = fitsio.FITS(path, 'r')
            self.lambda_ranges[spectrum] = fits['FLUXTBL']['lambda'][:]

    def _read_candidates(self):
        """Read redshift candidates from candidatesresult.csv."""
        for result in self.redshift_results.values():
            path = os.path.join(self.output_dir, result.processingid,
                                'candidatesresult.csv')
            with open(path, 'r') as f:
                self.candidates[result.spectrum] = []
                for l in f:
                    if not l.strip() or l.startswith('#'):
                        continue
                    # rank redshift intgProba gaussAmp gaussAmpErr gaussSigma gaussSigmaErr
                    _r = [f(x) for f, x in zip((int, float, float, float,
                                                float, float, float),
                                               l.split())]
                    candidate = RedshiftCandidate(*_r)
                    self.candidates[result.spectrum].append(candidate)

    def _read_zPDF(self):
        """Read zPDF for each spectrum."""
        for result in self.redshift_results.values():
            path = os.path.join(self.output_dir, result.processingid,
                                'zPDF',
                                'logposterior.logMargP_Z_data.csv')
            with open(path, 'r') as f:
                pdf = []
                for l in f:
                    if not l.strip() or l.startswith('#'):
                        continue
                    pdf.append(l.split())
                self.zpdf[result.spectrum] = np.array(pdf, dtype=float)

    def _read_linemeas(self):
        """Read line measurement for each spectrum."""
        for result in self.redshift_results.values():
            path = os.path.join('-'.join([self.output_dir, 'lf']),
                                result.processingid,
                                'linemodelsolve.linemodel_fit_extrema_0.csv')
            with open(path, 'r') as f:
                lm = []
                for l in f:
                    if not l.strip() or l.startswith('#'):
                        continue
                    try:
                        _r = [f(x) for f, x in zip((str, str, str, int, float,
                                                    float, float, float, float,
                                                    lambda x: None if x == '-1' else x,
                                                    float, float, float,
                                                    float, float, float, float,
                                                    float),
                                                   l.split())]
                        lm.append(LineMeasurement(*_r))
                    except Exception as e:
                        logging.log(logging.CRITICAL,
                                    "Can't parse line measurement : "
                                    "{}: {}".format(e, l))
                        continue
                self.linemeas[result.spectrum] = lm

    @staticmethod
    def _parse_pfsObject_name(name):
        """Parse a pfsObject file name.

        Template is : pfsObject-%05d-%s-%03d-%08x-%02d-0x%08x.fits
        """
        basename = os.path.splitext(name)[0]
        head, tract, patch, catId, objId, nVisit, \
            pfsVisitHash = basename.split('-')
        assert head == 'pfsObject'
        return (int(tract), patch, int(catId), int(objId, 16), int(nVisit),
                int(pfsVisitHash, 16))
