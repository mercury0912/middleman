import fcntl
import os

from middleman.util import exit_prog
from middleman.sysplatform import daemon, common

DAEMON_RUNNING = ['start', 'stop', 'restart', 'off']


def daemonize(opts):
    if opts.daemon == 'off':
        return
    dam = daemon.DaemonContext(pidfile_path=opts.pid_file, uid=opts.uid)
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


def _set_nonblocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


def set_close_exec(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)


class Waker:
    def __init__(self):
        r, w = os.pipe()
        _set_nonblocking(r)
        _set_nonblocking(w)
        set_close_exec(r)
        set_close_exec(w)
        self.reader = os.fdopen(r, "rb", 0)
        self.writer = os.fdopen(w, "wb", 0)

    def fileno(self):
        return self.reader.fileno()

    def write_fileno(self):
        return self.writer.fileno()

    def wake(self):
        try:
            self.writer.write(b"x")
        except (IOError, ValueError):
            pass

    def consume(self):
        try:
            while True:
                result = self.reader.read()
                if not result:
                    break
        except IOError:
            pass

    def close(self):
        self.reader.close()
        common.try_close(self.writer)
