"""
File: drp_1dpipe/pre_process/pre_process.py

Created on: 24/10/18
Author: PSF DRP1D developers
"""

import os
import json
import logging
import glob
import argparse


from drp_1dpipe import VERSION
from drp_1dpipe.core.logger import init_logger
from drp_1dpipe.core.argparser import define_global_program_options, AbspathAction
from drp_1dpipe.core.utils import normpath, get_conf_path, config_update, config_save
from drp_1dpipe.pre_process.config import config_defaults
from pfs.datamodel.drp import PfsCoadd


logger = logging.getLogger("pre_process")


def define_specific_program_options():
    """Define specific program options.
    
    Return
    ------
    :obj:`ArgumentParser`
        An ArgumentParser object
    """
    parser = argparse.ArgumentParser(
        prog='pre_process',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('--bunch_size', metavar='SIZE',
                        help='Maximum number of spectra per bunch.')
    parser.add_argument('--spectra_dir', metavar='DIR', action=AbspathAction,
                        help='Base path where to find spectra. '
                        'Relative to workdir.')
    parser.add_argument('--bunch_list', metavar='FILE',
                        help='List of files of bunch of astronomical objects.')
    parser.add_argument('--output_dir', '-o', metavar='DIR', action=AbspathAction,
                        help='Output directory.')

    return parser


def bunch_pfsobject_dir(bunch_size, spectra_dir):
    """Split the list of files in bunches of `bunch_size` files 

    Get the list of spectra files located into `spectra_dir` directory.
    Split the liste of files in bunches. The size of bunch is given by
    the "bunch_size" argument.

    Ex: for 85 files and bunch_size=20, the generator gives 4 bunches of 
    20 files and 1 bunch of 5 file.

    Parameters
    ----------
    bunch_size : int
        The number of spectra per bunch
    spectra_dir : str
        Path to spectra directoryt

    Yields
    -------
    :obj:`generator`
        A generator woth the max number of sources
    """    
    _list = []
    for source in glob.glob(os.path.join(spectra_dir,"*","*","*","*.fits")):
        prefix_length = len(spectra_dir) 
        _list.append({"fits": source.split(os.sep)[-1]})
        if len(_list) >= int(bunch_size):
            yield _list
            _list = []
    if _list:
        yield _list

        
def bunch_pfscoadd_file(bunch_size, pfscoadd_file):
    """Split the list of files in bunches of `bunch_size` files 

    Get the list of spectra files located into `spectra_dir` directory.
    Split the liste of files in bunches. The size of bunch is given by
    the "bunch_size" argument.

    Ex: for 85 files and bunch_size=20, the generator gives 4 bunches of 
    20 files and 1 bunch of 5 file.

    Parameters
    ----------
    bunch_size : int
        The number of spectra per bunch
    spectra_dir : str
        Path to spectra directoryt

    Yields
    -------
    :obj:`generator`
        A generator woth the max number of sources
    """    
    _list = []
    coadd = PfsCoadd.readFits(pfscoadd_file)
    for source in coadd:
        object_id = int(source.objId)
        _list.append(object_id)
        if len(_list) >= int(bunch_size):
            yield _list
            _list = []
    if _list:
        yield _list

        
def pre_process(config, bunch_list):
    # initialize logger
    workdir = normpath(config.workdir)
    logdir = normpath(config.logdir)
    log_level = config.log_level
    spectra_dir = normpath(config.spectra_dir)
    output_dir = normpath(config.output_dir)
    bunch_size = config.bunch_size
    logger = init_logger("pre_process", logdir, log_level)
    start_message = "Running pre_process {}".format(VERSION)
    logger.info(start_message)
    
    spectra_dir = normpath(workdir, spectra_dir)
    nb_bunches = 0
    if config.coadd_file:
        coadd_file = normpath(config.coadd_file)
        for i, objid_list in enumerate(bunch_pfscoadd_file(bunch_size,coadd_file)):
            nb_bunches = i + 1
            spectralist_file = os.path.join(output_dir, f'spectralist_B{i}.json')
            with open(spectralist_file, "w") as ff:
                json.dump({'coadd_file':coadd_file,'objIdList':objid_list}, ff)
    else:
        for i, spc_list in enumerate(bunch_pfsobject_dir(bunch_size, spectra_dir)):
            nb_bunches= i + 1
            spectralist_file = os.path.join(output_dir, f'spectralist_B{i}.json')
            with open(spectralist_file, "w") as ff:
                json.dump(spc_list, ff)
    return nb_bunches
    
    
def main_method(config):
    """main_method

    Parameters
    ----------
    config : :obj:`Config`
        Configuration object

    Returns
    -------
    int
        0 on success
    """    
    pre_process(config.workdir,config.logdir, config.log_level,
                config.spectra_dir,config.output_dir,
                config.bunch_size,config.bunch_list)


def main():
    """Pre-Process entry point

    Return
    ------
    int
        Exit code of the main method
    """
    parser = define_specific_program_options()
    define_global_program_options(parser)
    args = parser.parse_args()
    config = config_update(
        config_defaults,
        args=vars(args),
        install_conf_path=get_conf_path("pre_process.json")
        )
    config_save(config, "pre_process_config.json")
    return main_method(config)


if __name__ == '__main__':
    main()
