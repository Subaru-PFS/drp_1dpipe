import fitsio
import os.path
from pyamazed.redshift import *
import numpy as np


def write_candidates(output_dir,
                     tract, patch, catId, objId, nVisit, pfsVisitHash,
                     lambda_ranges, redshift, candidates):
    """Create a pfsZcandidates FITS file from an amazed output directory."""

    path = "pfsZcandidates-%05d-%s-%03d-%08x-%02d-0x%08x.fits" % (
        tract, patch, catId, objId, nVisit % 100, pfsVisitHash)

    print("Saving {} redshifts to {}".format(len(candidates), os.path.join(output_dir, path)))
    print("redshifts is", redshift)
    fits = fitsio.FITS(os.path.join(output_dir, path), 'rw', clobber=True)

    npix = len(lambda_ranges)
    npdf = 20

    header = [{'name': 'tract', 'value': tract, 'comment': 'Area of the sky'},
              {'name': 'patch', 'value': patch, 'comment': 'Region within tract'},
              {'name': 'catId', 'value': catId, 'comment': 'Source of the objId'},
              {'name': 'objId', 'value': objId, 'comment': 'Unique ID for object'},
              {'name': 'nVisit', 'value': nVisit, 'comment': 'Number of visits'},
              {'name': 'pfsVisitHash', 'value': pfsVisitHash, 'comment': 'SHA-1 hash of the visits'}]
    data = {}

    #data['PDU'] = np.array([])
    data['LAMBDA_SCALE'] = np.array(lambda_ranges, dtype=[('WAVELENGTH', 'f4')])
    data['ZCANDIDATES'] = np.ndarray((len(candidates),),
                                     dtype=[('Z', 'f8'), ('Z_ERR', 'f8'), ('ZRANK', 'i4'),
                                            ('RELIABILITY', 'f8'), ('CLASS', 'S15'),
                                            ('SUBCLASS', 'S15'), ('ZFIT', 'f8', (npix,))])

    for i, candidate in enumerate(candidates):
        data['ZCANDIDATES'][i]['Z'] = candidate.redshift
        data['ZCANDIDATES'][i]['Z_ERR'] = -1
        data['ZCANDIDATES'][i]['ZRANK'] = candidate.rank
        data['ZCANDIDATES'][i]['RELIABILITY'] = -1
        data['ZCANDIDATES'][i]['CLASS'] = ''
        data['ZCANDIDATES'][i]['SUBCLASS'] = ''
        data['ZCANDIDATES'][i]['ZFIT'] = np.zeros((npix,))

    data['ZPDF'] = np.ndarray((1,),
                              dtype=[('REDSHIFT', 'f8'), ('DENSITY', 'f8')])
    data['ZLINES'] = np.ndarray((1,),
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
    #fits.write(data['PDU'], extname='PDU', header=header)
    fits.write(data['LAMBDA_SCALE'], extname='LAMBDA_SCALE')
    fits.write(data['ZCANDIDATES'], extname='ZCANDIDATES')
    fits.write(data['ZPDF'], extname='ZPDF')
    fits.write(data['ZLINES'], extname='ZLINES')

    fits.close()
