import os
import numpy as np
from pylibamazed.redshift import get_version
from drp_1dpipe import VERSION
from astropy.io import fits
from pfs.datamodel.drp import PfsObject


class RedshiftCandidates:

    def __init__(self, drp1d_output, spectrum_path):
        self.drp1d_output = drp1d_output
        self.spectrum_path = spectrum_path

    def write_fits(self, output_dir):
        catId, tract, patch, objId, nVisit, pfsVisitHash = self._parse_pfsObject_name(
                os.path.basename(self.spectrum_path))
        path = "pfsZcandidates-%05d-%05d-%s-%016x-%03d-0x%016x.fits" % (
        catId, tract, patch, objId, nVisit % 1000, pfsVisitHash)
        hdul = []
        self._read_lambda_ranges()
        self.drp1d_output.load_all()
        self.header_to_fits(hdul)
        self.classification_to_fits(hdul)
        self.galaxy_candidates_to_fits(hdul)
        self.object_pdf_to_fits("galaxy", hdul)
        self.object_lines_to_fits("galaxy", hdul)
        self.qso_candidates_to_fits(hdul)
        self.object_pdf_to_fits("qso", hdul)
        self.object_lines_to_fits("qso", hdul)
        self.star_candidates_to_fits(hdul)
        self.object_pdf_to_fits("star", hdul)

        fits.HDUList(hdul).writeto(os.path.join(output_dir, path),
                                   overwrite=True)

    def header_to_fits(self, hdulist):

        catId, tract, patch, objId, nVisit, pfsVisitHash = self._parse_pfsObject_name(
                os.path.basename(self.spectrum_path))
        quality_flag = 2 # no linemeas active
        header = [fits.Card('tract', tract, 'Area of the sky'),
                  fits.Card('patch', patch, 'Region within tract'),
                  fits.Card('catId', catId, 'Source of the objId'),
                  fits.Card('objId', objId, 'Unique ID for object'),
                  fits.Card('nvisit', nVisit, 'Number of visit'),
                  fits.Card('vHash', pfsVisitHash, '63-bit SHA-1 list of visits'),
                  fits.Card('D1D_VER', get_version(), 'Version of the DRP_1D library'),
                  fits.Card('D1DP_VER', VERSION, 'Version of the DRP_1DPIPE pipeline'),
                  fits.Card('DAMD_VER', "unknown", 'Version of the data model'), #TODO
                  fits.Card('PAR_FILE', "parameters.json", "Parameters file name"), #TODO
                  fits.Card('ZWARNING', quality_flag, 'Quality flag')]

        hdr = fits.Header(header)
        primary = fits.PrimaryHDU(header=hdr)
        hdulist.append(primary)

    def classification_to_fits(self, hdulist):
        classification = [fits.Card('CLASS',self.drp1d_output.classification["Type"],
                                    "Spectro classification: GALAXY, QSO, STAR"),
                          fits.Card('P_GALAXY',self.drp1d_output.classification["GalaxyProba"],
                                    "Probability to be a galaxy"),
                          fits.Card('P_QSO',self.drp1d_output.classification["QSOProba"],
                                    "Probability to be a QSO"),
                          fits.Card('P_STAR',self.drp1d_output.classification["StarProba"],
                                    "Probability to be a star")]
        hdr = fits.Header(classification)
        hdu = fits.BinTableHDU(header=hdr)
        hdulist.append(hdu)

    def galaxy_candidates_to_fits(self, hdulist):
        nb_candidates = self.drp1d_output.nb_candidates["galaxy"]
        npix = len(self.lambda_ranges)
        zcandidates = np.ndarray((nb_candidates,),
                                 dtype=[('CRANK', 'i4'),
                                        ('Z', 'f8'),
                                        ('Z_ERR', 'f8'),
                                        ('Z_PROBA', 'f8'),
                                        ('SUBCLASS', 'S15'),
                                        ('CFILE','S50'),
                                        ('LFILE','S50'),
                                        ('MODELFLUX', 'f8', (npix,))
                                        ])

        for rank in range(nb_candidates):
            zcandidates[rank]['Z'] = self.drp1d_output.get_candidate_data("galaxy", rank, "Redshift")
            zcandidates[rank]['Z_ERR'] = self.drp1d_output.get_candidate_data("galaxy", rank, "RedshiftError")
            zcandidates[rank]['CRANK'] = rank
            zcandidates[rank]['Z_PROBA'] = self.drp1d_output.get_candidate_data("galaxy", rank, "RedshiftProba")
            zcandidates[rank]['SUBCLASS'] = ''
            zcandidates[rank]['CFILE'] = self.drp1d_output.get_candidate_data("galaxy", rank, "TemplateName")
            zcandidates[rank]['LFILE'] = self.drp1d_output.get_candidate_data("galaxy", rank, "LinesRatioName")
            model = np.array(self.drp1d_output.model["galaxy"][rank]["ModelFlux"])
            model = np.multiply(np.array(self.lambda_ranges) ** 2, np.array(model) * (1 / 2.99792458) * 10 ** 14)
#           model = np.multiply(np.array(self.lambda_ranges) ** 2, np.array(model)) * (1/  2.99792458) * 10 ** 14)
            zcandidates[rank]['MODELFLUX'] = model

        hdulist.append(fits.BinTableHDU(name='GALAXY_CANDIDATES', data=zcandidates))

    def qso_candidates_to_fits(self, hdulist):
        if "qso" in self.drp1d_output.nb_candidates:
            nb_candidates = self.drp1d_output.nb_candidates["qso"]
        else:
            nb_candidates = 0
        npix = len(self.lambda_ranges)
        zcandidates = np.ndarray((nb_candidates,),
                                 dtype=[('CRANK', 'i4'),
                                        ('Z', 'f8'),
                                        ('Z_ERR', 'f8'),
                                        ('Z_PROBA', 'f8'),
                                        ('SUBCLASS', 'S15'),
                                        ('MODELFLUX', 'f8', (npix,))
                                        ])


        for rank in range(nb_candidates):
            zcandidates[rank]['Z'] = self.drp1d_output.get_candidate_data("qso", rank, "Redshift")
            zcandidates[rank]['Z_ERR'] = self.drp1d_output.get_candidate_data("qso", rank, "RedshiftError")
            zcandidates[rank]['CRANK'] = rank
            zcandidates[rank]['Z_PROBA'] = self.drp1d_output.get_candidate_data("qso", rank, "RedshiftProba")
            zcandidates[rank]['SUBCLASS'] = ''
            model = np.array(self.drp1d_output.model["qso"][rank]["ModelFlux"])
            model = np.multiply(np.array(self.lambda_ranges)**2, np.array(model)) * (1/2.99792458) * 10**14
            zcandidates[rank]['MODELFLUX'] = model

        hdulist.append(fits.BinTableHDU(name='QSO_CANDIDATES', data=zcandidates))
        
    def star_candidates_to_fits(self,hdulist):
        nb_candidates = self.drp1d_output.nb_candidates["star"]
        npix = len(self.lambda_ranges)
        zcandidates = np.ndarray((nb_candidates,),
                                 dtype=[('CRANK', 'i4'),
                                        ('V', 'f8'),
                                        ('V_ERR', 'f8'),
                                        ('T_PROBA', 'f8'),
                                        ('SUBCLASS', 'S15'),
                                        ('TFILE','S50'),
                                        ('MODELFLUX', 'f8', (npix,))
                                        ])        

        for rank in range(nb_candidates):
            zcandidates[rank]['V'] = self.drp1d_output.get_candidate_data("star", rank, "Redshift")
            zcandidates[rank]['V_ERR'] = self.drp1d_output.get_candidate_data("star", rank, "RedshiftError")
            zcandidates[rank]['CRANK'] = rank
            zcandidates[rank]['T_PROBA'] = self.drp1d_output.get_candidate_data("star", rank, "RedshiftProba")
            zcandidates[rank]['SUBCLASS'] = ''
            zcandidates[rank]['TFILE'] = self.drp1d_output.get_candidate_data("star", rank, "ModelTplName")
            # model = np.array(self.lambda_ranges, dtype=np.float64, copy=True)
            model = np.array(self.drp1d_output.model["star"][rank]["ModelFlux"])
            model = np.multiply(np.array(self.lambda_ranges)**2, np.array(model)) * (1/2.99792458) * 10**14
            zcandidates[rank]['MODELFLUX'] = model

        hdulist.append(fits.BinTableHDU(name='STAR_CANDIDATES', data=zcandidates))

    def object_pdf_to_fits(self, object_type, hdulist):
        if object_type in self.drp1d_output.pdf:
            pdf = self.drp1d_output.pdf[object_type].to_records(index=False)
            grid_size = self.drp1d_output.pdf[object_type].index.size
            zpdf_hdu = np.ndarray(grid_size, buffer=pdf,
                                  dtype=[('ln PDF', 'f8'), ('REDSHIFT', 'f8')])
        else:
            zpdf_hdu = None

        hdulist.append(fits.BinTableHDU(name=object_type.upper()+'_PDF', data=zpdf_hdu))

    def object_lines_to_fits(self, object_type, hdulist):
        zlines = np.ndarray((0,),
                            dtype=[('LINENAME', 'S15'),
                                   ('LINEWAVE', 'f8'),
                                   ('LINEZ', 'f8'),
                                   ('LINEZ_ERR', 'f8'),
                                   ('LINESIGMA', 'f8'),
                                   ('LINESIGMA_ERR', 'f8'),
                                   ('LINEVEL', 'f8'),
                                   ('LINEVEL_ERR', 'f8'),
                                   ('LINEFLUX', 'f8'),
                                   ('LINEFLUX_ERR', 'f8'),
                                   ('LINEEW', 'f8'),
                                   ('LINEEW_ERR', 'f8'),
                                   ('LINECONTLEVEL', 'f8'),
                                   ('LINECONTLEVEL_ERR', 'f8')])
        hdulist.append(fits.BinTableHDU(name=object_type.upper()+"_LINES", data=zlines))
    
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

    def _read_lambda_ranges(self):
        """Method used to read lambda vector from spectrum
        """
        obj = PfsObject.readFits(self.spectrum_path)
        valid = np.where(obj.mask == 0, True, False)
        self.lambda_ranges = obj.wavelength[valid]

#        return {"catId":int(catId),
#                "tract":int(tract),
#                "patch":patch,
#                "objId":int(objId, 16),
#                "nvisit":int(nvisit),
#                "vHash":int(pfsVisitHash, 16)}
