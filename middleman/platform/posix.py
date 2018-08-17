from middleman.util import exit_prog
from middleman.platform import daemon

DAEMON_RUNNING = ['start', 'stop', 'restart', 'off']


def daemonize(opts):
    if opts.daemon == 'off':
        return
    dam = daemon.DaemonContext(pidfile_path=opts.pid_file)
    try:
        handler = getattr(dam, opts.daemon)
        handler()
    except daemon.DaemonAlreadyRunningError as e:
        # gracefully exit, daemon is already running
        exit_prog(0, "A middleman process already exists (%s)" % e)
    except daemon.DaemonOSEnvironmentError as exc:
        exit_prog(1, '%s' % exc)
    except Exception as exc:
        exit_prog(1, '%s' % exc)
    if opts.daemon == 'stop':
        exit_prog(0)
