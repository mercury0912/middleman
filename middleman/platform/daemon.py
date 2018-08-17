import atexit
import errno
import os
import pwd
import resource
import signal
import sys
import time

from middleman.platform import pidlockedfile


class DaemonError(Exception):
    """ Base exception class for errors from this module. """


class DaemonOSEnvironmentError(DaemonError, OSError):
    """ Exception raised when daemon OS environment setup receives error. """


class DaemonProcessDetachError(DaemonError, OSError):
    """ Exception raised when process detach fails. """


class DaemonAlreadyRunningError(DaemonError,
                                pidlockedfile.PidLockedFileLockedError):
    pass


class DaemonContext:
    """ Context for turning the current program into a daemon process.

        A `DaemonContext` instance represents the behaviour settings and
        process context for the program when it becomes a daemon. The
        behaviour and environment is customised by setting options on the
        instance, before calling the `open` method.

        Each option can be passed as a keyword argument to the `DaemonContext`
        constructor, or subsequently altered by assigning to an attribute on
        the instance at any time prior to calling `open`. That is, for
        options named `wibble` and `wubble`, the following invocation::

            foo = daemon.DaemonContext(wibble=bar, wubble=baz)
            foo.open()

        is equivalent to::

            foo = daemon.DaemonContext()
            foo.wibble = bar
            foo.wubble = baz
            foo.open()

        The following options are defined.

        `working_directory`
            :Default: ``'/'``
            Full path of the working directory to which the process should
            change on daemon start.

            Since a filesystem cannot be unmounted if a process has its
            current working directory on that filesystem, this should either
            be left at default or set to a directory that is a sensible “home
            directory” for the daemon while it is running.

        `umask`
            :Default: ``0``

            File access creation mask (“umask”) to set for the process on
            daemon start.

            A daemon should not rely on the parent process's umask value,
            which is beyond its control and may prevent creating a file with
            the required access mode. So when the daemon context opens, the
            umask is set to an explicit known value.

            If the conventional value of 0 is too open, consider setting a
            value such as 0o022, 0o027, 0o077, or another specific value.
            Otherwise, ensure the daemon creates every file with an
            explicit access mode for the purpose.
        """
    def __init__(self,
                 pidfile_path,
                 working_directory="/",
                 umask=0,
                 uid=None,
                 gid=None,
                 initgroups=False,
                 signal_map=None):
        self.working_directory = working_directory
        self.umask = umask
        self.pidfile_path = pidfile_path
        self.pidlockedfile = None

        if uid is None:
            self.uid = os.getuid()
        if gid is None:
            self.gid = os.getuid()
        self.initgroups = initgroups
        if signal_map is None:
            self.signal_map = make_default_signal_map()

        self._is_open = False

    @property
    def is_open(self):
        """ ``True`` if the instance is currently open. """
        return self._is_open

    def open(self):
        """ Become a daemon process.
            :return: ``None``.
            Open the daemon context, turning the current program into a daemon
            process. This performs the following steps:
            * If this instance's `is_open` property is true, return
              immediately. This makes it safe to call `open` multiple times on
              an instance

            * Set the process owner (UID and GID) to the `uid` and `gid`
              attribute values.

              If the `initgroups` attribute is true, also set the process's
              supplementary groups to all the user's groups (i.e. those
              groups whose membership includes the username corresponding
              to `uid`).
              
            * Change current working directory to the path specified by the
              `working_directory` attribute so we won't prevent file systems
              from being unmounted.

            * Detach the current process into its own process group,
              and disassociate from any controlling terminal.

            * If the `pidfile_path` attribute is not ``None``, create
              pid file and lock it with underlying lock implementation

            * Close all open file descriptors.
              
            * Reset the file access creation mask to the value specified by
              the `umask` attribute.

            * Attach file descriptors 0, 1, and 2 to /dev/null.
            """

        change_file_creation_mask(self.umask)
        change_process_owner(self.uid, self.gid, self.initgroups)
        detach_process_context()
        change_working_directory(self.working_directory)
        if self.pidfile_path is not None:
            self.pidlockedfile = already_running(self.pidfile_path)
        fd = self.pidlockedfile.fd if self.pidlockedfile is not None else -1
        signal_handler_map = self._make_signal_handler_map()
        set_signal_handlers(signal_handler_map)
        close_all_open_files_exc_pid(fd)
        redirect_standard_stream()
        self._is_open = True

    def close(self):
        if not self.is_open:
            return
        if self.pidlockedfile is not None:
            self.pidlockedfile.remove_existing_pidfile()
        self._is_open = False

    @staticmethod
    def terminate(signal_number, _):
        """ Signal handler for end-process signals.
            :param signal_number: The OS signal number received.
            :param _: The frame object at the point the
                signal was received.
            :return: ``None``.
            Signal handler for the ``signal.SIGTERM`` signal. Performs the
            following step:
            * Raise a ``SystemExit`` exception explaining the signal.
            """
        exception = SystemExit(
                "Terminating on signal {signal_number!r}".format(
                    signal_number=signal_number))
        raise exception

    def _make_signal_handler(self, target):
        """ Make the signal handler for a specified target object.
            :param target: A specification of the target for the
                handler; see below.
            :return: The value for use by `signal.signal()`.
            If `target` is ``None``, return ``signal.SIG_IGN``. If `target`
            is a text string, return the attribute of this instance named
            by that string. Otherwise, return `target` itself.
            """
        if target is None:
            result = signal.SIG_IGN
        elif isinstance(target, str):
            name = target
            result = getattr(self, name)
        else:
            result = target
        return result

    def _make_signal_handler_map(self):
        """ Make the map from signals to handlers for this instance.
            :return: The constructed signal map for this instance.
            Construct a map from signal numbers to handlers for this
            context instance, suitable for passing to
            `set_signal_handlers`.
            """
        signal_handler_map = dict(
                (signal_number, self._make_signal_handler(target))
                for (signal_number, target) in self.signal_map.items())
        return signal_handler_map

    def start(self):
        self.open()

    def stop(self):
        if self.pidfile_path is not None:
            if not os.path.exists(self.pidfile_path):
                raise DaemonOSEnvironmentError(
                    ("pid file %s does not exists. 'Daemon not running?'" %
                     self.pidfile_path))
            self.pidlockedfile = pidlockedfile.PidLockedFile(self.pidfile_path)
            try:
                if self.pidlockedfile.try_lock():
                    self.pidlockedfile.unlock()
                    raise DaemonOSEnvironmentError(
                        ("pid file % does exists. but Daemon not running?" %
                         self.pidfile_path))
            except pidlockedfile.PidLockedFileLockedError:
                pid = self.pidlockedfile.read_pid_from_pidfile()
                try:
                    # try to kill, maximum 5s to wait
                    for i in range(50):
                        os.kill(pid, signal.SIGTERM)
                        time.sleep(0.1)
                    else:
                        raise DaemonOSEnvironmentError(
                            "'timed out when stopping pid %d' % pid")
                except OSError as exc:
                    if exc.errno != errno.ESRCH:
                        raise DaemonOSEnvironmentError("%s" % exc)
                except Exception as exc:
                    raise DaemonOSEnvironmentError("%s" % exc)
                self.pidlockedfile.remove_existing_pidfile()

    def restart(self):
        self.stop()
        self.start()


def change_file_creation_mask(mask):
    """ Change the file creation mask for this process.

    :param mask: The numeric file creation mask to set.
    :return: ``None``.
    """
    try:
        os.umask(mask)
    except Exception as exc:
        error = DaemonOSEnvironmentError(
            "Unable to change file creation mask ({exc})".format(exc=exc))
        raise error


def change_working_directory(directory):
    """Change the working directory of this process.

    :param directory: The target directory path.
    :return: ``None``.
    """
    try:
        os.chdir(directory)
    except Exception as exc:
        error = DaemonOSEnvironmentError(
                "Unable to change working directory ({exc})".format(exc=exc))
        raise error


def get_username_for_uid(uid):
    """ Get the username for the specified UID. """
    passwd_entry = pwd.getpwuid(uid)
    username = passwd_entry.pw_name
    return username


def change_process_owner(uid, gid, initgroups=False):
    """ Change the owning UID, GID, and groups of this process.
        :param uid: The target UID for the daemon process.
        :param gid: The target GID for the daemon process.
        :param initgroups: If true, initialise the supplementary
            groups of the process.
        :return: ``None``.
        Sets the owning GID and UID of the process (in that order, to
        avoid permission errors) to the specified `gid` and `uid`
        values.
        If `initgroups` is true, the supplementary groups of the
        process are also initialised, with those corresponding to the
        username for the target UID.
        All these operations require appropriate OS privileges. If
        permission is denied, a ``DaemonOSEnvironmentError`` is
        raised.
        """
    username = None
    try:
        username = get_username_for_uid(uid)
    except KeyError:
        # We don't have a username to pass to ‘os.initgroups’.
        initgroups = False

    try:
        if username is not None and initgroups:
            os.initgroups(username, gid)
        else:
            os.setgid(gid)
        os.setuid(uid)
    except Exception as exc:
        error = DaemonOSEnvironmentError(
                "Unable to change process owner ({exc})".format(exc=exc))
        raise error


def already_running(path):
    pid_file = pidlockedfile.PidLockedFile(path)
    try:
        pid_file.write_pid_to_pidfile()
    except pidlockedfile.PidLockedFileLockedError:
        raise DaemonAlreadyRunningError(
            "PID: %s" % pid_file.read_pid_from_pidfile())
    except Exception as exc:
        error = DaemonOSEnvironmentError("{exc}".format(exc=exc))
        raise error
    atexit.register(pid_file.remove_existing_pidfile)
    return pid_file


def detach_process_context():
    """ Detach the process context from parent and session.
        :return: ``None``.
        Detach from the parent process and session group, allowing the
        parent to exit while this process continues running.
        Reference: “Advanced Programming in the Unix Environment”,
        section 13.3, by W. Richard Stevens, published 1993 by
        Addison-Wesley.
        """
    def fork_then_exit_parent(error_message):
        """ Fork a child process, then exit the parent process.
            :param error_message: Message for the exception in case of a
                detach failure.
            :return: ``None``.
            :raise DaemonProcessDetachError: If the fork fails.
            """
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError as exc:
            error = DaemonProcessDetachError(
                    "{message}: [{exc.errno:d}] {exc.strerror}".format(
                        message=error_message, exc=exc))
            raise error

    # Become a session leader to lose controlling TTY.
    fork_then_exit_parent(error_message="Failed first fork")
    os.setsid()

    # Ensure future opens won't allocate controlling TTYs.
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    fork_then_exit_parent(error_message="Failed second fork")


#
# If the process hard resource limit of maximum number of open file
# descriptors is "infinity", a default value of ``MAXFD`` will be used.
#
MAXFD = 2048


def get_maximum_file_descriptors():
    """ Get the maximum number of open file descriptors for this process.
        :return: The number (integer) to use as the maximum number of open
            files for this process.
        The maximum is the process hard resource limit of maximum number of
        open file descriptors. If the limit is “infinity”, a default value
        of ``MAXFD`` is returned.
        """
    (__, hard_limit) = resource.getrlimit(resource.RLIMIT_NOFILE)
    result = hard_limit
    if hard_limit == resource.RLIM_INFINITY:
        result = MAXFD
    return result


def close_all_open_files_exc_pid(fd):
    rv = get_maximum_file_descriptors()
    if fd == -1:
        os.closerange(0, rv)
    else:
        os.closerange(0, fd)
        os.closerange(fd+1, rv)


def redirect_standard_stream():
    fd0 = os.open(os.devnull, os.O_RDWR)
    fd1 = os.dup(0)
    fd2 = os.dup(0)
    if fd0 != 0 or fd1 != 1 or fd2 != 2:
        raise DaemonOSEnvironmentError(
            'unexpected file descriptors %d %d %d' % (fd0, fd1, fd2))


def make_default_signal_map():
    """ Make the default signal map for this system.
        :return: A mapping from signal number to handler object.
        The signals available differ by system. The map will not contain
        any signals not defined on the running system.
        """
    name_map = {
            'SIGTSTP': None,
            'SIGTTIN': None,
            'SIGTTOU': None,
            'SIGHUP': signal.SIG_DFL,
            'SIGTERM': 'terminate',
            }
    signal_map = dict(
            (getattr(signal, name), target)
            for (name, target) in name_map.items()
            if hasattr(signal, name))
    return signal_map


def set_signal_handlers(signal_handler_map):
    """ Set the signal handlers as specified.
        :param signal_handler_map: A map from signal number to handler
            object.
        :return: ``None``.
        See the `signal` module for details on signal numbers and signal
        handlers.
        """
    for (signal_number, handler) in signal_handler_map.items():
        signal.signal(signal_number, handler)
