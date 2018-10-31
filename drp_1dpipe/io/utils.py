import os.path


def get_auxiliary_path(dir):
    return os.path.join(os.path.dirname(__file__), 'auxdir', dir)


def get_conf_path(dir):
    return os.path.join(os.path.dirname(__file__), 'conf', dir)
