import logging

from drp_1dpipe.core.utils import get_auxiliary_path, get_conf_path

config_defaults = {
    # Global programm options
    'config': get_conf_path("process_spectra.json"),
    'workdir': '.',
    'logdir': 'logdir',
    'loglevel': 'INFO',
    'log_level': 30,
    # Specific programm options
    'spectra_dir': 'spectra',
    'calibration_dir': 'calibration',
    'spectra_listfile': '',
    'default_parameters_file': get_auxiliary_path("parameters_stellar_galaxy.json"),
    'zclassifier_dir': '',
    'process_method': 'AMAZED',
    'output_dir': 'output',
    'default_linemeas_parameters_file': get_auxiliary_path("linemeas-parameters.json"),
    'lineflux': 'on',
    'continue_': False,
    'stellar': 'on',
    'parameters_file': '',
    'linemeas_parameters_file': ''
    }
