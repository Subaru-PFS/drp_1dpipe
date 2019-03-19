import fitsio
import os.path
import numpy as np


def write_candidates(output_dir,
                     tract, patch, catId, objId, nVisit, pfsVisitHash,
                     lambda_ranges, redshift, candidates, zpdf, linemeas):
    """Create a pfsZcandidates FITS file from an amazed output directory."""

    path = "pfsZcandidates-%05d-%s-%03d-%08x-%02d-0x%08x.fits" % (
        tract, patch, catId, objId, nVisit % 100, pfsVisitHash)

    print("Saving {} redshifts to {}".format(len(candidates),
                                             os.path.join(output_dir, path)))
    print("redshifts is", redshift)
    fits = fitsio.FITS(os.path.join(output_dir, path), 'rw', clobber=True)

    npix = len(lambda_ranges)

    header = [{'name': 'tract', 'value': tract,
               'comment': 'Area of the sky'},
              {'name': 'patch', 'value': patch,
               'comment': 'Region within tract'},
              {'name': 'catId', 'value': catId,
               'comment': 'Source of the objId'},
              {'name': 'objId', 'value': objId,
               'comment': 'Unique ID for object'},
              {'name': 'nVisit', 'value': nVisit,
               'comment': 'Number of visits'},
              {'name': 'pfsVisitHash', 'value': pfsVisitHash,
               'comment': 'SHA-1 hash of the visits'}]
    data = {}

    #data['PDU'] = np.array([])
    data['LAMBDA_SCALE'] = np.array(lambda_ranges,
                                    dtype=[('WAVELENGTH', 'f4')])
    data['ZCANDIDATES'] = np.ndarray((len(candidates),),
                                     dtype=[('Z', 'f8'), ('Z_ERR', 'f8'),
                                            ('ZRANK', 'i4'),
                                            ('RELIABILITY', 'f8'),
                                            ('CLASS', 'S15'),
                                            ('SUBCLASS', 'S15'),
                                            ('MODELFLUX', 'f8', (npix,))])

    for i, candidate in enumerate(candidates):
        data['ZCANDIDATES'][i]['Z'] = candidate.redshift
        data['ZCANDIDATES'][i]['Z_ERR'] = -1
        data['ZCANDIDATES'][i]['ZRANK'] = candidate.rank
        data['ZCANDIDATES'][i]['RELIABILITY'] = candidate.intgProba
        data['ZCANDIDATES'][i]['CLASS'] = ''
        data['ZCANDIDATES'][i]['SUBCLASS'] = ''
        data['ZCANDIDATES'][i]['MODELFLUX'] = np.zeros((npix,))  # TODO : get from linemodel_spc_extrema_0

    data['ZPDF'] = np.ndarray(len(zpdf), buffer=zpdf,
                              dtype=[('REDSHIFT', 'f8'), ('DENSITY', 'f8')])
    data['ZLINES'] = np.ndarray((len(linemeas),),
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
    for i, lm in enumerate(linemeas):
        data['ZLINES'][i]['LINENAME'] = lm.name
        data['ZLINES'][i]['LINEWAVE'] = lm.lambda_obs  # TODO: or lambda_rest_beforeOffset ?
        data['ZLINES'][i]['LINEZ'] = np.nan  # TODO: what is that ?
        data['ZLINES'][i]['LINEZ_ERR'] = np.nan  # TODO: what is that ?
        data['ZLINES'][i]['LINESIGMA'] = lm.sigma
        data['ZLINES'][i]['LINESIGMA_ERR'] = np.nan  # TODO: what is that ?
        data['ZLINES'][i]['LINEVEL'] = lm.velocity
        data['ZLINES'][i]['LINEVEL_ERR'] = np.nan  # TODO: what is that
        data['ZLINES'][i]['LINEFLUX'] = lm.flux
        data['ZLINES'][i]['LINEFLUX_ERR'] = lm.flux_err
        data['ZLINES'][i]['LINEEW'] = np.nan  # TODO: what is that
        data['ZLINES'][i]['LINEEW_ERR'] = np.nan  # TODO: what is that
        data['ZLINES'][i]['LINECONTLEVEL'] = np.nan  # TODO: what is that
        data['ZLINES'][i]['LINECONTLEVEL_ERR'] = np.nan  # TODO: what is that


    #fits.write(data['PDU'], extname='PDU', header=header)
    fits.write(data['LAMBDA_SCALE'], extname='LAMBDA_SCALE')
    fits.write(data['ZCANDIDATES'], extname='ZCANDIDATES')
    fits.write(data['ZPDF'], extname='ZPDF')
    fits.write(data['ZLINES'], extname='ZLINES')

    fits.close()
