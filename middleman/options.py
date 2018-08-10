import argparse
import json
import os
import sys

from middleman import __version__
from middleman.log import Log


class Options:
    options = None

    @staticmethod
    def parse_options():
        args = Options._parse_command_line()
        config = Options._parse_config_file(args.config_file)
        if config:
            Options.options = Options._merge_options(args, config)
        else:
            Options.options = args

    @staticmethod
    def _merge_options(command_line_options, config_options):
        for opt, value in config_options.items():
            if opt in command_line_options:
                val = getattr(command_line_options, opt)
                if val is not None and not isinstance(value, type(val)):
                    raise TypeError(
                        "Option %r is required to be a %s (%s given)" %
                        (opt, type(val).__name__, type(value).__name__))
                setattr(command_line_options, opt, value)
            else:
                raise KeyError("Invalid option: %r" % opt)
        return command_line_options

    @staticmethod
    def _parse_command_line():
        parser = argparse.ArgumentParser(
            description="I'm just a middleman.", add_help=False,
            usage="%(prog)s [OPTION]...",
            epilog="Online help: https://github.com/mercury0912/middleman")
        group_proxy = parser.add_argument_group('Proxy options')
        group_proxy.add_argument(
            '-p', action='store', dest='port',
            type=int, default=8255, metavar='port',
            help='specify server port number (default: %(default)s)')

        group_log = parser.add_argument_group('Logging control')
        group_log.add_argument('--logging', default='warning',
                               choices=Log.LOGGING_LEVEL,
                               dest='logging',
                               help='set log level (default: %(default)s)')
        group_log.add_argument('--log_both', action='store_true',
                               help=("send log to stderr and file if "
                                     "log_file_prefix is also set "
                                     "(default: %(default)s)"))
        group_log.add_argument('--log_file_prefix', metavar='PATH',
                               help=('path prefix for log files. '
                                     'Note that if you are running multiple '
                                     'middleman processes, log_file_prefix '
                                     'must be different for each of them (e.g.'
                                     ' include the port number)'))
        group_log.add_argument('--log_file_max_bytes', type=int,
                               default=4 * 1024 * 1024,
                               help=("max bytes of log files before rollover "
                                   "(default: %(default)s)"))
        group_log.add_argument('--log_file_backup_count', type=int, default=10,
                               help=('number of log files to keep '
                                     '(default: %(default)s)'))

        group_misc = parser.add_argument_group('Miscellaneous')
        group_misc.add_argument('-v', '--version', action='version',
                                version='%(prog)s ' + __version__)
        group_misc.add_argument('-h', '--help', action='help',
                                help='show this help text and exit')

        parser.add_argument('-f', dest='config_file', metavar='config-file',
                            type=argparse.FileType('r'),
                            help='use given config-file')
        args = parser.parse_args()
        return args

    @staticmethod
    def _parse_config_file(config_file):
        if config_file is None:
            return None

        try:
            cfg = json.load(config_file)
            if not isinstance(cfg, dict):
                raise TypeError(
                    "Type mismatch: %r required, but %s given" %
                    (dict.__name__, type(cfg).__name__))
        except ValueError as exc:
            if config_file is sys.stdin:
                path = config_file.name
            else:
                path = os.path.abspath(config_file.name)
            raise ValueError("%s: %s" % (path, exc))
        finally:
            config_file.close()
        return cfg

    @staticmethod
    def display_options_info():
        print('=' * 80)
        for attr in dir(Options.options):
            if not attr.startswith('_'):
                val = getattr(Options.options, attr)
                if attr == 'config_file':
                    if val is not None:
                        if val is sys.stdin:
                            val = val.name
                        else:
                            val = os.path.abspath(val.name)
                elif attr == 'log_file_prefix':
                    val = val if val is None else os.path.abspath(val)
                print("%-30s %s" % (attr, val))
        print('=' * 80, flush=True)
