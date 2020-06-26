import os
import logging


def init_logger(process_name, logdir, loglevel, console=False):
    """Initializes a logger depending on which module calls it.

    :param process_name: name of the module calling it.

    :Example:

    In define_program_options() of process_spectra.py :

    init_logger("pre_process")
    """

    os.makedirs(logdir, exist_ok=True)

    logger = logging.getLogger(process_name)
    logger.setLevel(loglevel)
    formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')

    # file handler
    file_handler = logging.FileHandler(os.path.join(logdir, process_name + '.log'), 'w')
    file_handler.setLevel(loglevel)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # stream handler
    if console:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(loglevel)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger
