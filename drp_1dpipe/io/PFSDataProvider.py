from drp_1dpipe.io.PFSReader import PFSReader
from drp_1dpipe.io.PFSExternalStorage import PFSExternalStorage


class PFSDataProvider():
    """Data provider class"""
    def __init__(self, config, parameters, calibration):
        self.config = config
        self.storage = PFSExternalStorage(self.config)
        self.reader = PFSReader(parameters, calibration, None)

    def get_spectrum(self, spectrum_id):
        """
        Return the spectrum values for a given spectrum_id with a data structure compatible with Amazed library

        Parameters
        ----------

        spectrun_id: str
            id of the source
        
        Return
        ------
        spectrum
            Amazed spectrum object
        """
        with self.reader as reader:
            with self.storage(spectrum_id, self.config.coadd_file) as (resource, spectrum_infos):
                reader.source_id = spectrum_id
                reader.load_all(resource, spectrum_infos)
            return reader.get_spectrum()
