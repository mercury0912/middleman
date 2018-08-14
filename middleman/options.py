__all__ = ['options', 'display_options_info', 'parse_options']


import argparse
import json
import os


from middleman import __version__
from middleman.log import Log


def display_options_info():
    if options is None:
        return
    print('=' * 80)
    for attr in vars(options):
        val = getattr(options, attr)
        if attr == 'config_file' or attr == 'log_file_prefix':
            if val is not None:
                val = os.path.abspath(val)
        print("%-30s %s" % (attr, val))
    print('=' * 80, flush=True)


def parse_options():
    opts = _parse_command_line()
    if opts.config_file is not None:
        config = _parse_config_file(opts.config_file)
        opts = _merge_options(opts, config)
    return opts


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
                           default=os.path.abspath('middlemanlog'),
                           help=('path prefix for log files. '
                                 'Note that if you are running multiple '
                                 'middleman processes, log_file_prefix '
                                 'must be different for each of them (e.g.'
                                 ' include the port number)'
                                 '(default: %(default)s)'))
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
                        help='use given config-file')
    args = parser.parse_args()
    return args


def _parse_config_file(config_file):
    if config_file is None:
        return None

    config_file = os.path.abspath(config_file)
    try:
        with open(config_file) as f:
            cfg = json.load(f)
            if not isinstance(cfg, dict):
                raise TypeError(
                    "Type mismatch: %r required, but %s given" %
                    (dict.__name__, type(cfg).__name__))
    except ValueError as exc:
        raise ValueError("%s: %s" % (config_file, exc))
    return cfg


def _merge_options(cmd_line_options, config_options):
    for opt, value in config_options.items():
        _check_config_file(opt, value, cmd_line_options)
        setattr(cmd_line_options, opt, value)
    return cmd_line_options


def _check_config_file(opt, val, cmd_opts):
    if opt not in cmd_opts:
        raise KeyError("Invalid option: %r" % opt)
    cmd_val = getattr(cmd_opts, opt)

    if cmd_val is not None and not isinstance(val, type(cmd_val)):
        raise TypeError(
            "Option %r is required to be a %s (%s given)" %
            (opt, type(cmd_val).__name__, type(val).__name__))


options = parse_options()
