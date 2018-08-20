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
        logger.setLevel(getattr(logging, options.logging.upper()))
        formatter = logging.Formatter(fmt=self._fmt, datefmt=self._datefmt)
        if options.log_file_prefix:
                ch = logging.handlers.RotatingFileHandler(
                    filename=options.log_file_prefix,
                    maxBytes=options.log_file_max_bytes,
                    backupCount=options.log_file_backup_count)
                ch.setFormatter(formatter)
                logger.addHandler(ch)
        if options.log_both:
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            logger.addHandler(ch)


def logger_thread(q):
    while True:
        record = q.get()
        if record is None:
            break
        logger = logging.getLogger(record.name)
        logger.handle(record)
