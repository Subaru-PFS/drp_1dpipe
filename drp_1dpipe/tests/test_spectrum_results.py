import pytest
import os
import json
import tempfile
from IPython import embed
import numpy as np

from drp_1dpipe.process_spectra.results import SpectrumResults, RedshiftSummary, StellarSummary, QsoSummary

from pfs.datamodel.drp import PfsObject
from pfs.datamodel.masks import MaskHelper
from pfs.datamodel.target import Target
from pfs.datamodel.observations import Observations

def create_pfsobject(wavelength, directory):
    target = Target(catId=0, tract=1, patch='2,2', objId=3, ra=0.0, dec=0.0)
    observations = Observations(np.array([0]),
        np.array(['n']),
        np.array([0]),
        np.array([0]),
        np.array([0]),
        np.array([(0.0,0.0)]),
        np.array([(0.0,0.0)]))

    obj = PfsObject(target=target,
                observations=observations,
                wavelength=wavelength,
                flux=np.ones(2),
                mask=np.zeros(2),
                sky=np.zeros(2),
                covar=np.array([np.ones(2), np.zeros(2), np.zeros(2)]),
                covar2=np.zeros((2, 2)),
                flags=MaskHelper())
    obj.write(dirName=directory)
    return obj.filenameFormat%obj.getIdentity()

def test_spectrum_results():
    with pytest.raises(FileNotFoundError):
        sr = SpectrumResults('.', None)
    with pytest.raises(FileNotFoundError):
        sr = SpectrumResults('.', '/not/exists/')
    with pytest.raises(FileNotFoundError):
        sr = SpectrumResults('.', '.', output_lines_dir='/not/exists')

def test_candidates():
    sd = tempfile.TemporaryDirectory()
    od = sd
    sr = SpectrumResults(sd.name, od.name)
    with pytest.raises(FileNotFoundError):
        sr._read_candidates()
    rstr = "#com\n0	ID	1.0	2.0	1	3.0	4.0	5.0	6.0	7.0"
    rname = os.path.join(od.name, "candidatesresult.csv")
    with open(rname, "w") as ff:
        ff.write(rstr)
    sr = SpectrumResults(sd.name, od.name)
    sr._read_candidates()
    assert len(sr.candidates) == 1
    assert pytest.approx(sr.candidates[0].redshift, 1.e-12) == 1.0

def test_zpdf():
    sd = tempfile.TemporaryDirectory()
    od = sd
    rstr = "#com\n1.0 2.0"
    dname = os.path.join(od.name, "zPDF")
    os.makedirs(dname)
    rname = os.path.join(dname, "logposterior.logMargP_Z_data.csv")
    with open(rname, "w") as ff:
        ff.write(rstr)
    sr = SpectrumResults(sd.name, od.name)
    sr._read_zpdf()
    assert len(sr.zpdf) == 1
    assert len(sr.zpdf[0]) == 2
    assert pytest.approx(sr.zpdf[0][0], 1.e-12) == 1.0
    assert pytest.approx(sr.zpdf[0][1], 1.e-12) == 2.0
    

def test_lambda_ranges():
    sd = tempfile.TemporaryDirectory()
    od = sd
    sr = SpectrumResults(sd.name, od.name)
    with pytest.raises(IsADirectoryError):
        sr._read_lambda_ranges()
    wl = [1.0, 2.0]
    filename = create_pfsobject(wl, sd.name)
    sr = SpectrumResults(os.path.join(sd.name,filename), od.name)
    sr._read_lambda_ranges()
    assert len(sr.lambda_ranges) == 2
    assert pytest.approx(sr.lambda_ranges[0], 1.e-12) == 1.0
    assert pytest.approx(sr.lambda_ranges[1], 1.e-12) == 2.0

def test_lines():
    sd = tempfile.TemporaryDirectory()
    od = sd
    sr = SpectrumResults(sd.name, od.name)
    with pytest.raises(FileNotFoundError):
        sr._read_lines()
    sr = SpectrumResults(sd.name, od.name, output_lines_dir=sd.name)
    with pytest.raises(FileNotFoundError):
        sr._read_lines()
    rstr = "#com\na b c 0 1.0 2.0 3.0 4.0 5.0 -1 6.0 7.0 8.0 9.0 10.0 11.0 12.0 13.0"
    rname = os.path.join(sd.name, "linemodelsolve.linemodel_fit_extrema_0.csv")
    with open(rname, "w") as ff:
        ff.write(rstr)
    sr = SpectrumResults(sd.name, od.name, output_lines_dir=sd.name)
    sr._read_lines()
    assert len(sr.linemeas) == 1
    assert sr.linemeas[0].type == "a"
    assert sr.linemeas[0].force == "b"
    assert sr.linemeas[0].name == "c"
    assert sr.linemeas[0].elt_id == 0
    assert pytest.approx(sr.linemeas[0].lambda_rest_beforeOffset, 1.e-12) == 1.0
    assert pytest.approx(sr.linemeas[0].lambda_obs, 1.e-12) == 2.0
    assert pytest.approx(sr.linemeas[0].amp, 1.e-12) == 3.0
    assert pytest.approx(sr.linemeas[0].err, 1.e-12) == 4.0
    assert pytest.approx(sr.linemeas[0].err_fit, 1.e-12) == 5.0
    assert sr.linemeas[0].fit_group is None
    assert pytest.approx(sr.linemeas[0].velocity, 1.e-12) == 6.0
    assert pytest.approx(sr.linemeas[0].offset, 1.e-12) == 7.0
    assert pytest.approx(sr.linemeas[0].sigma, 1.e-12) == 8.0
    assert pytest.approx(sr.linemeas[0].flux, 1.e-12) == 9.0
    assert pytest.approx(sr.linemeas[0].flux_err, 1.e-12) == 10.0
    assert pytest.approx(sr.linemeas[0].flux_di, 1.e-12) == 11.0
    assert pytest.approx(sr.linemeas[0].center_cont_flux, 1.e-12) == 12.0
    assert pytest.approx(sr.linemeas[0].cont_err, 1.e-12) == 13.0

def test_classification():
    sd = tempfile.TemporaryDirectory()
    od = sd
    sr = SpectrumResults(sd.name, od.name)
    with pytest.raises(FileNotFoundError):
        sr._read_classification()
    rstr = "#com\nA 1.0 2.0 3.0"
    rname = os.path.join(od.name, "classificationresult.csv")
    with open(rname, "w") as ff:
        ff.write(rstr)
    sr = SpectrumResults(sd.name, od.name)
    sr._read_classification()
    assert sr.classification.type == "A"
    assert pytest.approx(sr.classification.evidenceG, 1.e-12) == 1.0
    assert pytest.approx(sr.classification.evidenceS, 1.e-12) == 2.0
    assert pytest.approx(sr.classification.evidenceQ, 1.e-12) == 3.0

def test_models():
    sd = tempfile.TemporaryDirectory()
    od = sd
    sr = SpectrumResults(sd.name, od.name)
    with pytest.raises(AttributeError):
        sr._read_models()
    rstr = "#com\n1.0 2.0"
    rname = os.path.join(od.name, "linemodelsolve.linemodel_spc_extrema_0.csv")
    with open(rname, "w") as ff:
        ff.write(rstr)
    sr = SpectrumResults(sd.name, od.name)
    sr.candidates = [0]
    sr._read_models()
    assert len(sr.models) == 1
    assert len(sr.models[0]) == 1
    assert pytest.approx(sr.models[0][0], 1.e-12) == 2.0
    sr.candidates = [0, 1]
    with pytest.raises(FileNotFoundError):
        sr._read_models()

def test_redshift():
    od = tempfile.TemporaryDirectory()
    sr = RedshiftSummary(output_dir=od.name)
    with pytest.raises(FileNotFoundError):
        sr.read()
    rstr = "#com\nstr1	str2	1.0	2.0 str3    str4	3.0	str5	4.0	5.0	6.0	7.0	str6"
    rname = os.path.join(od.name, "redshift.csv")
    with open(rname, "w") as ff:
        ff.write(rstr)
    sr = RedshiftSummary(output_dir=od.name)
    sr.read()
    assert len(sr.summary) == 1
    assert pytest.approx(sr.summary[0].redshift, 1.e-12) == 1.0
    assert sr.summary[0].type_ == "str6"
    nod = tempfile.TemporaryDirectory()
    sr.output_dir = nod.name
    sr.write()
    with open(os.path.join(nod.name, 'redshift.csv')) as ff:
        ll = ff.readlines()

def test_parse_pfsObjectName():
    name = 'pfsObject-999-96321-P,P-0000000000001234-745-0x0000000000000045.fits'
    c, t, p, o, v, h = SpectrumResults._parse_pfsObject_name(name)
    assert c == 999
    assert t == 96321
    assert p == "P,P"
    assert o == 4660
    assert v == 745
    assert h == 69