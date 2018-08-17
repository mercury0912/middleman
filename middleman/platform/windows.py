from middleman.util import exit_prog

DAEMON_RUNNING = ['off']


def daemonize(opts):
    if opts.dameon != 'off':
        exit_prog(1, 'daemon mode is only supported on Unix like systems')
