__all__ = ['options',
           'display_options_info',
           'parse_options',
           'get_error_message',
           ]


import argparse
import json
import os
import sys


from middleman import __version__
from middleman.log import Log
from middleman.util import exit_prog
from middleman.sysplatform.auto import DAEMON_RUNNING

_opt_choices_map = {'logging': Log.LOGGING_LEVEL, 'daemon': DAEMON_RUNNING}
_opt_files = ['config_file', 'log_file_prefix', 'pid_file']
_LOCALHOST = '127.0.0.1'
_LOCALHOST_V6 = '::1'


def display_options_info(opts):
    if opts is None:
        return
    print()
    print('=' * 80)
    isserver = opts.server
    for attr in sorted(vars(opts)):
        val = getattr(opts, attr)
        if attr == 'V':
            continue
        if isserver and attr.startswith("remote"):
            continue
        print("%-30s %s" % (attr, val))
    print('=' * 80)
    print(flush=True)


def parse_options():
    try:
        opts = _parse_command_line()
        if opts.config_file is not None:
            config = _parse_config_file(opts.config_file)
            opts = _merge_options(opts, config)
        set_abspath(opts, _opt_files)
        if hasattr(opts, 'V'):
            display_options_info(opts)
            sys.exit(0)
        else:
            return opts
    except Exception as exc:
        exit_prog(1, "%s" % exc)


def set_abspath(opts, opt_files):
    for name in opt_files:
        if name in opts:
            path = getattr(opts, name)
            if path is not None:
                abspath = os.path.abspath(path)
                setattr(opts, name, abspath)


def _parse_command_line():
    parser = argparse.ArgumentParser(
        description="I'm just a middleman.", add_help=False,
        usage="%(prog)s (-r remote|-s) [OPTION]...",
        epilog="Online help: https://github.com/mercury0912/middleman")
    group_proxy = parser.add_argument_group('service-level options')
    group_proxy.add_argument(
        '-a', dest='local', default=_LOCALHOST, metavar='local',
        help='local IP address or hostname (default: %(default)s)')
    group_proxy.add_argument('-l', dest='local_port', type=int, default=8388,
                             metavar='local_port',
                             help='local listen port (default: %(default)s)')
    # group_proxy.add_argument('-r', dest='remote',
    #                          default='_LOCALHOST', metavar='remote',
    #                          help='server endpoint address')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-s', dest='server', action='store_true',
                             help='run as the server (default: %(default)s)')
    group.add_argument('-r', dest='remote',
                             default=_LOCALHOST, metavar='remote',
                             help='server endpoint address')
    group_proxy.add_argument(
        '-p', dest='remote_port', type=int, default=1080,
        metavar='remote_port',
        help='remote listen port: invalid in server mode '
             '(default: %(default)s)')
    # group_proxy.add_argument('-s', dest='server', action='store_true',
    #                          help='run as the server (default: %(default)s)')
    group_proxy.add_argument('-t', dest='timeout', type=int,
                             default=300, metavar='timeout',
                             help='timeout in seconds for idle connections (default: %(default)ss)')

    group_log = parser.add_argument_group('logging control')
    group_log.add_argument('-L', dest='logging', metavar='logging',
                           default='warning',
                           choices=Log.LOGGING_LEVEL,
                           help='set log level (default: %(default)s)')
    group_log.add_argument('-B', '--log_both', action='store_true',
                           dest="log_both",
                           help=("send log to stderr and file "
                                 "(default: %(default)s)"))
    group_log.add_argument('-P', metavar='log_file_prefix',
                           dest="log_file_prefix",
                           default=os.path.abspath('middlemanlog'),
                           help=('specify log file name prefix. '
                                 '(default: %(default)s)'))
    group_log.add_argument('-M', metavar='log_file_max_bytes',
                           dest='log_file_max_bytes', type=int,
                           default=4 * 1024 * 1024,
                           help=("max bytes of log files before rollover "
                                 "(default: %(default)s)"))
    group_log.add_argument('-C', metavar='log_file_backup_count',
                           dest='log_file_backup_count', type=int, default=10,
                           help=('number of log files to keep '
                                 '(default: %(default)s)'))

    group_misc = parser.add_argument_group('miscellaneous')
    group_misc.add_argument('-v', '--version', action='version',
                            version='%(prog)s ' + __version__)
    group_misc.add_argument('-h', '--help', action='help',
                            help='show this help text and exit')
    group_misc.add_argument('-V', action='store_true', default=argparse.SUPPRESS,
                            help='show configure options and exit')

    group_gen = parser.add_argument_group('general options')
    group_gen.add_argument('-f', metavar='config_file', dest='config_file',
                           help='specify configuration file')
    if os.name != 'nt':
        group_gen.add_argument(
            '-d', default='off', choices=DAEMON_RUNNING,
            dest='daemon', help='daemon mode default: %(default)s)')
        group_gen.add_argument(
            '-u', dest='uid', type=int, default=os.getuid(), metavar='uid',
            help='set the process owner (default: %(default)s)')
        group_gen.add_argument(
            '--pid', metavar='pid_file', dest='pid_file',
            default='/var/run/middleman.pid',
            help='set pid file (default: %(default)s)')
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
    _check_choices(opt, val)


def _check_choices(opt, val):
    if opt in _opt_choices_map:
        if val not in _opt_choices_map[opt]:
            err_msg = get_error_message(opt, val, _opt_choices_map[opt])
            raise ValueError(err_msg)


def get_error_message(opt, val, choices):
    error_message = ", ".join(map(lambda choice: "%r" % choice, choices))
    error_message = ("Option %s: invalid choice: %r "
                     "(choose from %s)" % (opt, val, error_message))
    return error_message


options = parse_options()
