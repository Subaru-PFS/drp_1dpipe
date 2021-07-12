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
    'process_method': 'AMAZED',
    'output_dir': 'output',
    'continue_': False,
    'parameters_file': '',
    }
