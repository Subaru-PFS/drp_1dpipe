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
    'bunch_size': 8,
    'notification_url': '',
    'lineflux': 'on',
    'parameters_file': get_auxiliary_path("parameters_stellar_galaxy.json"),
    'linemeas_parameters_file': get_auxiliary_path("linemeas-parameters.json"),
    'output_dir':'@AUTO@',
    'stellar': 'on'
    }

