from astropy.io import fits
import os.path
import numpy as np

def write_candidates(output_dir,
                     catId, tract, patch, objId, nVisit, pfsVisitHash,
                     lambda_ranges, mask, candidates, models, zpdf, linemeas, object_class):
    """Create a pfsZcandidates FITS file from an amazed output directory."""

    path = "pfsZcandidates-%05d-%05d-%s-%016x-%03d-0x%016x.fits" % (
        catId, tract, patch, objId, nVisit % 1000, pfsVisitHash)

    print("Saving {} redshifts to {}".format(len(candidates),
                                             os.path.join(output_dir, path)))
    header = [fits.Card('tract', tract, 'Area of the sky'),
              fits.Card('patch', patch, 'Region within tract'),
              fits.Card('catId', catId, 'Source of the objId'),
              fits.Card('objId', objId, 'Unique ID for object'),
              fits.Card('nvisit', nVisit, 'Number of visit'),
              fits.Card('vHash', pfsVisitHash, '63-bit SHA-1 list of visits')]

    hdr = fits.Header(header)
    primary = fits.PrimaryHDU(header=hdr)
    hdul = [primary]

    if object_class == 'GALAXY':
        npix = len(lambda_ranges)

        # data['PDU'] = np.array([])

        # create LAMBDA_SCALE HDU
        lambda_scale = np.array(lambda_ranges, dtype=[('WAVELENGTH', 'f4')])
        hdul.append(fits.BinTableHDU(name='MODELWL', data=lambda_scale))

        # create ZCANDIDATES HDU
        zcandidates = np.ndarray((len(candidates),),
                                    dtype=[('Z', 'f8'), ('Z_ERR', 'f8'),
                                        ('ZRANK', 'i4'),
                                        ('RELIABILITY', 'f8'),
                                        ('CLASS', 'S15'),
                                        ('SUBCLASS', 'S15'),
                                        ('MODELFLUX', 'f8', (npix,))])
        for i, candidate in enumerate(candidates):
            zcandidates[i]['Z'] = candidate.redshift
            zcandidates[i]['Z_ERR'] = candidate.deltaz
            zcandidates[i]['ZRANK'] = candidate.rank
            zcandidates[i]['RELIABILITY'] = candidate.intgProba
            zcandidates[i]['CLASS'] = object_class
            zcandidates[i]['SUBCLASS'] = ''
            model = np.array(lambda_ranges, dtype=np.float64, copy=True)
            model.fill(np.nan)
            np.place(model, mask == 0, models[i])
            model = np.multiply(lambda_scale**2, np.array(model)) * (1/3) * 10**14
            zcandidates[i]['MODELFLUX'] = model
        hdul.append(fits.BinTableHDU(name='ZCANDIDATES', data=zcandidates))


        # create ZPDF HDU
        zpdf_hdu = np.ndarray(len(zpdf), buffer=zpdf,
                            dtype=[('REDSHIFT', 'f8'), ('PDF', 'f8')])
        hdul.append(fits.BinTableHDU(name='ZPDF', data=zpdf_hdu))

        # create ZLINES HDU
        if linemeas is not None :
            zlines = np.ndarray((len(linemeas),),
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
                zlines[i]['LINENAME'] = lm.name
                zlines[i]['LINEWAVE'] = lm.lambda_obs  # TODO: or lambda_rest_beforeOffset ?
                zlines[i]['LINEZ'] = np.nan  # TODO: what is that ?
                zlines[i]['LINEZ_ERR'] = np.nan  # TODO: what is that ?
                zlines[i]['LINESIGMA'] = lm.sigma
                zlines[i]['LINESIGMA_ERR'] = np.nan  # TODO: what is that ?
                zlines[i]['LINEVEL'] = lm.velocity
                zlines[i]['LINEVEL_ERR'] = np.nan  # TODO: what is that
                zlines[i]['LINEFLUX'] = lm.lambda_obs**2*lm.flux * (1/3) * 10**14
                zlines[i]['LINEFLUX_ERR'] = lm.lambda_obs**2*lm.flux_err * (1/3) * 10**14
                zlines[i]['LINEEW'] = np.nan  # TODO: what is that
                zlines[i]['LINEEW_ERR'] = np.nan  # TODO: what is that
                zlines[i]['LINECONTLEVEL'] = np.nan  # TODO: what is that
                zlines[i]['LINECONTLEVEL_ERR'] = np.nan  # TODO: what is that
            hdul.append(fits.BinTableHDU(name='ZLINES', data=zlines))

    elif object_class == 'STAR':

        # create ZCANDIDATES HDU
        zcandidates = np.ndarray((len(candidates),),
                                    dtype=[('Z', 'f8'), ('Z_ERR', 'f8'),
                                        ('ZRANK', 'i4'),
                                        ('RELIABILITY', 'f8'),
                                        ('CLASS', 'S15'),
                                        ('SUBCLASS', 'S15')])
        for i, candidate in enumerate(candidates):
            zcandidates[i]['Z'] = candidate.redshift
            zcandidates[i]['Z_ERR'] = 0.
            zcandidates[i]['ZRANK'] = 0
            zcandidates[i]['RELIABILITY'] = candidate.intgProba
            zcandidates[i]['CLASS'] = object_class
            zcandidates[i]['SUBCLASS'] = candidate.template
        hdul.append(fits.BinTableHDU(name='ZCANDIDATES', data=zcandidates))


    fits.HDUList(hdul).writeto(os.path.join(output_dir, path),
                               overwrite=True)
    
    return path
