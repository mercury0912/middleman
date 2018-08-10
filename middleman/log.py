import logging
import logging.handlers

# Logger objects for internal tornado use
access_log = logging.getLogger("middle.access")
gen_log = logging.getLogger("middle.general")


class Log:
    DEFAULT_FORMAT = ('[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d]'
                      ' %(message)s')
    DEFAULT_DATE_FORMAT = '%y%m%d %H:%M:%S'
    LOGGING_LEVEL = ['debug', 'info', 'warning', 'error']

    def __init__(self, fmt=DEFAULT_FORMAT, datefmt=DEFAULT_DATE_FORMAT):
        self._fmt = fmt
        self._datefmt = datefmt

    def init_logging(self, options=None, logger=None):
        if options is None:
            return
        if logger is None:
            logger = logging.getLogger()
        if options.logging not in self.LOGGING_LEVEL:
            err_msg = Log.get_error_message('logging', options.logging,
                                            self.LOGGING_LEVEL)
            raise ValueError(err_msg)
        logger.setLevel(getattr(logging, options.logging.upper()))
        formatter = logging.Formatter(fmt=self._fmt, datefmt=self._datefmt)
        if options.log_file_prefix:
                ch = logging.handlers.RotatingFileHandler(
                    filename=options.log_file_prefix,
                    maxBytes=options.log_file_max_bytes,
                    backupCount=options.log_file_backup_count)
                ch.setFormatter(formatter)
                logger.addHandler(ch)
        if options.log_both or not logger.hasHandlers():
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            logger.addHandler(ch)

    @staticmethod
    def get_error_message(opt, val, choices):
        error_message = ", ".join(map(lambda choice: "%r" % choice, choices))
        error_message = ("Option %s: invalid choice: %r "
                         "(choose from %s)" % (opt, val, error_message))
        return error_message
