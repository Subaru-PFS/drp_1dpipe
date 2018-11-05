import os.path


def get_auxiliary_path(dir):
    return os.path.join(os.path.dirname(__file__), 'auxdir', dir)


def get_conf_path(dir):
    return os.path.join(os.path.dirname(__file__), 'conf', dir)


def get_args_from_file(file_name, args):
    with open(file_name, 'r') as ff:
        lines = ff.readlines()
    for line in lines:
        try:
            key, value = line.replace('\n', '').split('#')[0].split("=")
            try:
                if getattr(args, key.strip()) is None:
                    setattr(args, key.strip(), value.strip())
            except AttributeError:
                setattr(args, key.strip(), value.strip())
        except ValueError:
            pass
