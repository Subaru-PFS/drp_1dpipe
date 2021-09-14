import os.path
from astropy.io import fits
from pylibamazed.redshift import (CLSFFactory, CParameterStore, TLSFArguments,
                                  TLSFGaussianVarWidthArgs,TLSFGaussianConstantWidthArgs,
                                  TLSFGaussianConstantResolutionArgs,TLSFGaussianNISPVSSPSF201707Args)

LSFFactory = CLSFFactory.GetInstance()

def readWidthIOFile(path):
    with fits.open(path) as hdul:
        wavelength = hdul[1].data.field(0)
        widthArr = hdul[1].data.field(1)
    return wavelength, widthArr;

TLSFArgumentsCtor = {'GaussianNISPSIM2016':TLSFArguments, 'GaussianConstantWidth':TLSFGaussianConstantWidthArgs, 'GaussianConstantResolution':TLSFGaussianConstantResolutionArgs, 'GaussianNISPVSSPSF201707': TLSFGaussianNISPVSSPSF201707Args, "GaussianVariableWidth":TLSFGaussianVarWidthArgs}

def defaultConstructor(key, paramStore, ref_dir=None):
    return TLSFArgumentsCtor[key](paramStore)

def GaussianVarWidthConstructor(key, paramStore, ref_dir):
    lsfpath = os.path.normpath(paramStore.Get_string("LSF.GaussianVariablewidthFileName"))
    lsfpath = os.path.join(ref_dir, lsfpath)
    lbdaArr, widthArr = readWidthIOFile(lsfpath)
    return TLSFArgumentsCtor[key](lbdaArr, widthArr)

TLSFArgumentsBuilder = {'GaussianVariableWidth':GaussianVarWidthConstructor, 'GaussianNISPSIM2016':defaultConstructor, 'GaussianConstantWidth':defaultConstructor, 'GaussianConstantResolution':defaultConstructor, 'GaussianNISPVSSPSF201707':defaultConstructor}

def CreateLSF(key, paramstore, ref_dir):
    lsfArgs = TLSFArgumentsBuilder[key](key, paramstore, ref_dir)
    return LSFFactory.Create(key, lsfArgs)


