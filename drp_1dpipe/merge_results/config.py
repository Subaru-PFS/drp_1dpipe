import logging

from drp_1dpipe.core.utils import get_auxiliary_path, get_conf_path

config_defaults = {
    # Global programm options
    'config': get_conf_path("merge_results.json"),
    'workdir': '.',
    'logdir': 'logdir',
    'loglevel': 'INFO',
    'log_level': 30,
    # Specific programm options
    'output_dir':'output',
    'report_line_snr_threshold':3,
    'ref_output_dir':"validation"
    }
