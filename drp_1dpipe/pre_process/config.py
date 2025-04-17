import logging

from drp_1dpipe.core.utils import get_auxiliary_path, get_conf_path

config_defaults = {
    # Global programm options
    'config': get_conf_path("pre_process.json"),
    'workdir': '.',
    'logdir': 'logdir',
    'loglevel': 'INFO',
    'log_level': 30,
    # Specific programm options
    'bunch_size': 8,
    'spectra_dir': 'spectra',
    'bunch_list': 'spectralist.json',
    'output_dir':'output',
    'object_id':0,
    'parameters_file': ''
    }
