import pytest
import os
import json
import collections
import tempfile
import types
import numpy as np

from drp_1dpipe.core.utils import normpath, config_update
from drp_1dpipe.core.config import Config

from drp_1dpipe.process_spectra.process_spectra import main_method
from drp_1dpipe.pre_process.config import config_defaults


def test_config_update_none():
    cf0 = config_defaults.copy()
    for k in cf0.keys():
        cf0[k] = None
    cf0['loglevel'] = 'DEBUG'
    obj = config_update(cf0)
    with pytest.raises(AttributeError):
        cfg = obj.config


def test_main_method():
    wd = tempfile.TemporaryDirectory()
    config = Config(config_defaults)
    config.workdir = wd.name
    config.logdir = wd.name
    config.spectra_dir = wd.name
    config.output_dir = wd.name
    config.process_method = "dummy"

    result_run = main_method(config)
    assert result_run == 0
    
    assert os.path.exists(os.path.join(config.logdir, "process_spectra.log"))

    ld = tempfile.TemporaryDirectory()
    config.logdir = ld.name
    config.process_method = "amazed"
    with pytest.raises(Exception):
        main_method(config)
    logpath_task = os.path.join(ld.name, "process_spectra.log")
    assert os.path.exists(logpath_task)
    logpath_amazed = os.path.join(ld.name, "amazed.log")
    assert os.path.exists(logpath_amazed)


# @pytest.fixture()
# def context():
#     pass

# def test_redshifts_csv():
#     sd = tempfile.TemporaryDirectory()
#     od = tempfile.TemporaryDirectory()
#     rstr = "#Spectrum	ProcessingID	Redshift	Merit	Template	Method	Deltaz	Reliability	snrHa	lfHa	snrOII	lfOII	Type\n\
#     0000    0001    1.0 2.0 tpl LM  3.0 C   4.0 5.0 6.0 7.0 G"
#     rname = os.path.join(od.name, "redshift.csv")
#     with open(rname, "w") as ff:
#         ff.write(rstr)
#     ar = AmazedResults(od.name, sd.name)
#     ar._read_redshifts_csv()
#     assert ar.redshift_results['0000'].processingid == '0001'
#     assert ar.redshift_results['0000'].redshift == 1.0
#     assert ar.redshift_results['0000'].merit == 2.0
#     assert ar.redshift_results['0000'].template == 'tpl'
#     assert ar.redshift_results['0000'].method == 'LM'
#     assert ar.redshift_results['0000'].deltaz == 3.0
#     assert ar.redshift_results['0000'].reliability == 'C'
#     assert ar.redshift_results['0000'].snrha == 4.0
#     assert ar.redshift_results['0000'].lfha == 5.0
#     assert ar.redshift_results['0000'].snroII == 6.0
#     assert ar.redshift_results['0000'].lfoII == 7.0
#     assert ar.redshift_results['0000'].type_ == 'G'


# def test_cadidates():
#     sd = tempfile.TemporaryDirectory()
#     od = tempfile.TemporaryDirectory()
#     rstr = "#Spectrum	ProcessingID	Redshift	Merit	Template	Method	Deltaz	Reliability	snrHa	lfHa	snrOII	lfOII	Type\n\
#     0000    0001    1.0 2.0 tpl LM  3.0 C   4.0 5.0 6.0 7.0 G"
#     rname = os.path.join(od.name, "redshift.csv")
#     with open(rname, "w") as ff:
#         ff.write(rstr)
#     rstr = "#rank	IDs	redshift	intgProba	Rank_PDF	Deltaz	gaussAmp_unused	gaussAmpErr_unused	gaussSigma_unused	gaussSigmaErr_unused\n\
#     0	ID	1.0	2.0	1	3.0	4.0	5.0	6.0	7.0"
#     os.makedirs(os.path.join(od.name, '0001'))
#     rname = os.path.join(od.name, '0001', "candidatesresult.csv")
#     with open(rname, "w") as ff:
#         ff.write(rstr)
#     ar = AmazedResults(od.name, sd.name)
#     ar._read_redshifts_csv()
#     ar._read_candidates()
#     assert ar.candidates['0000'][0].rank == 0
#     assert ar.candidates['0000'][0].ids == 'ID'
#     assert ar.candidates['0000'][0].redshift == 1.0
#     assert ar.candidates['0000'][0].intgProba == 2.0
#     assert ar.candidates['0000'][0].rank_pdf == 1
#     assert ar.candidates['0000'][0].deltaz == 3.0
#     assert ar.candidates['0000'][0].gaussAmp == 4.0
#     assert ar.candidates['0000'][0].gaussAmpErr == 5.0
#     assert ar.candidates['0000'][0].gaussSigma == 6.0
#     assert ar.candidates['0000'][0].gaussSigmaErr == 7.0


# def test_zpdf():
#     sd = tempfile.TemporaryDirectory()
#     od = tempfile.TemporaryDirectory()
#     rstr = "#Spectrum	ProcessingID	Redshift	Merit	Template	Method	Deltaz	Reliability	snrHa	lfHa	snrOII	lfOII	Type\n\
#     0000    0001    1.0 2.0 tpl LM  3.0 C   4.0 5.0 6.0 7.0 G"
#     rname = os.path.join(od.name, "redshift.csv")
#     with open(rname, "w") as ff:
#         ff.write(rstr)
#     rstr = "#header\n\
#     1.0 10.0\n2.0 20.0"
#     dname = os.path.join(od.name, '0001', 'zPDF')
#     os.makedirs(dname)
#     rname = os.path.join(dname, "logposterior.logMargP_Z_data.csv")
#     with open(rname, "w") as ff:
#         ff.write(rstr)
#     ar = AmazedResults(od.name, sd.name)
#     ar._read_redshifts_csv()
#     ar._read_zPDF()
#     assert pytest.approx(ar.zpdf['0000'][0][0]) == 1.
#     assert pytest.approx(ar.zpdf['0000'][0][1]) == 10.
#     assert pytest.approx(ar.zpdf['0000'][1][0]) == 2.
#     assert pytest.approx(ar.zpdf['0000'][1][1]) == 20.


# def test_linemeas():
#     sd = tempfile.TemporaryDirectory()
#     od = tempfile.TemporaryDirectory()
#     rstr = "#Spectrum	ProcessingID	Redshift	Merit	Template	Method	Deltaz	Reliability	snrHa	lfHa	snrOII	lfOII	Type\n\
#     0000    0001    1.0 2.0 tpl LM  3.0 C   4.0 5.0 6.0 7.0 G"
#     rname = os.path.join(od.name, "redshift.csv")
#     with open(rname, "w") as ff:
#         ff.write(rstr)
#     rstr = "#header\n\
#     A	W	HA               	1   1.0	2.0	3.0	4.0	5.0	N	7.0	8.0	9.0	10.0	11.0	12.0	13.0	14.0"
#     dname = os.path.join(od.name, '-'.join([od.name,'lf']), '0001')
#     os.makedirs(dname)
#     rname = os.path.join(dname, "linemodelsolve.linemodel_fit_extrema_0.csv")
#     with open(rname, "w") as ff:
#         ff.write(rstr)
#     ar = AmazedResults(od.name, sd.name)
#     ar._read_redshifts_csv()
#     ar._read_linemeas()
#     assert len(ar.linemeas['0000']) == 1
#     assert ar.linemeas['0000'][0].type == 'A'
#     assert ar.linemeas['0000'][0].force == 'W'
#     assert ar.linemeas['0000'][0].name == 'HA'
#     assert ar.linemeas['0000'][0].elt_id == 1
#     assert ar.linemeas['0000'][0].lambda_rest_beforeOffset == 1.0
#     assert ar.linemeas['0000'][0].lambda_obs == 2.0
#     assert ar.linemeas['0000'][0].amp == 3.0
#     assert ar.linemeas['0000'][0].err == 4.0
#     assert ar.linemeas['0000'][0].err_fit == 5.0
#     assert ar.linemeas['0000'][0].fit_group == 'N'
#     assert ar.linemeas['0000'][0].velocity == 7.0
#     assert ar.linemeas['0000'][0].offset == 8.0
#     assert ar.linemeas['0000'][0].sigma == 9.0
#     assert ar.linemeas['0000'][0].flux == 10.0
#     assert ar.linemeas['0000'][0].flux_err == 11.0
#     assert ar.linemeas['0000'][0].flux_di == 12.0
#     assert ar.linemeas['0000'][0].center_cont_flux == 13.0
#     assert ar.linemeas['0000'][0].cont_err == 14.0