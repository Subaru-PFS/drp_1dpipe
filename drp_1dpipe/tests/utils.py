import numpy as np
from pfs.datamodel.pfsObject import PfsObject

NROW = 11640
NCOARSE = 10

def generate_fake_fits(fileName=None):
    obj = PfsObject(tract=99999, patch="0,0", objId=0, catId=0, visits=[], pfsConfigIds=[],
                    nVisit=None)
    obj.lam = np.array(range(NROW))/NROW*(1260-380)+380
    obj.flux = np.random.rand(NROW, 1)
    obj.fluxTbl.lam = np.array(range(NROW))/NROW*(1260-380)+380
    obj.fluxTbl.flux = np.random.rand(NROW)
    obj.fluxTbl.fluxVariance = np.random.rand(NROW)
    obj.fluxTbl.mask = np.zeros(NROW)
    obj.mask = np.zeros(NROW)
    obj.sky = np.zeros(NROW)
    obj.covar = np.random.rand(NROW, 3)
    obj.covar2 = np.random.rand(NCOARSE, NCOARSE)
    obj.visits = []
    obj.pfsConfigIds = []
    obj.write(fileName=fileName)
