import logging

from pylibamazed.redshift import CLogHandler

logger = logging.getLogger('')

__all__ = ["StreamHandler", "SocketHandler"]


class StreamHandler(CLogHandler):
    def __init__(self, c_logger, logger):
        super(StreamHandler, self).__init__(c_logger)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

    def NotifyEntry(self, header, msg):
        logger.info(msg)


class SocketHandler(CLogHandler):
    def __init__(self, c_logger, logger, host, port):
        super(SocketHandler, self).__init__(c_logger)
        handler = logging.handlers.SocketHandler(host, port)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

    def NotifyEntry(self, header, msg):
        logger.info(msg)