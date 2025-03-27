import logging

from drp_1dpipe.core.utils import get_auxiliary_path, get_conf_path


config_defaults = {
    # Global programm options
    'config': '',
    'workdir': '.',
    'logdir': '@AUTO@',
    'loglevel': 'INFO',
    'log_level': 30,
    # Specific programm options
    'scheduler': 'local',
    'venv': '',
    'concurrency': 1,
    'spectra_dir': 'spectra',
    'coadd_file': '',
    'bunch_size': 8,
    'notification_url': '',
    'output_dir':'@AUTO@',
    'stellar': 'on',
    'parameters_file': '',
    'get_default_parameters': None,
    'debug':False,
    'object_id':0
    }

