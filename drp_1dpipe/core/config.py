import os.path
import json
from collections import namedtuple

__all__ = ['ConfigJson']

    
class ConfigDefault():
    _default = {}

    @classmethod
    def init_default(cls, config):
        cls._default = config.copy()
    
    @classmethod
    def get_default(cls):
        return cls._default


class Config:
    """ Base class for config """

    def __init__(self, config):
        ConfigDefault.init_default(config)
        self._to_object(config)

    def _to_object(self, config_dict):
        for k, v in config_dict.items():
            if k not in ConfigDefault.get_default():
                raise AttributeError('Invalid config file parameter '
                                    '{}'.format(k))
            if v is not None:
                setattr(self, k, v)

    def update(self, config_dict):
        self._to_object(config_dict)

    def load(self, path):
        """ Abstract method to be implemented """
        raise NotImplementedError()

    def save(self, path):
        """ Abstract method to be implemented """
        raise NotImplementedError()
    

class ConfigJson(Config):
    """Class for JSON configuration"""

    def load(self, path):
        """ Parse configuration file
        
        Parameters
        ----------
        path : `str`
            Path of the configuration file to load
        """
        if os.path.exists(path):
            with open(path, 'r') as ff:
                config = json.load(ff)
            self._to_object(config)
        else:
            raise FileNotFoundError("Unkown configuration file {}".format(path))

    def save(self, path, indent=2):
        """ Save configuration file

        Parameters
        ----------
        config : `dict`
            Configration dictionary
        path : `str`
            Path of the configuration file to save
        """
        with open(path, 'w') as ff:
            json.dump(vars(self), ff, indent=indent)
