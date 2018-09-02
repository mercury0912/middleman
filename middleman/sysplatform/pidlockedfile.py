import errno
import fcntl
import os
import stat


class PidLockedFileError(OSError):
    pass


class PidLockedFileLockedError(PidLockedFileError):
    pass


class PidLockedFile:
    FILE_MODE = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH

    def __init__(self, path):
        self.path = path
        self.fd = -1

    def write_pid_to_pidfile(self):
        if self.fd == -1:
            self.lock_pidfile()
        try:
            os.ftruncate(self.fd, 0)

            # According to the FHS 2.3 section on PID files in /var/run:
            #
            #   The file must consist of the process identifier in
            #   ASCII-encoded decimal, followed by a newline character. For
            #   example, if crond was process number 25, /var/run/crond.pid
            #   would contain three characters: two, five, and newline.

            os.write(self.fd, ("%s\n" % os.getpid()).encode('ascii'))
        except OSError as exc:
            raise PidLockedFileError(
                "Unable to write pid to {file} ({exc})".format(
                    exc=exc, file=self.path))

    def lock_pidfile(self):
        try:
            fd = os.open(self.path, os.O_WRONLY | os.O_CREAT,
                         PidLockedFile.FILE_MODE)
        except Exception as exc:
            raise PidLockedFileError("Can't open pid file (%s)" % exc)
        try:
            fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            if exc.errno == errno.EACCES or exc.errno == errno.EAGAIN:
                raise PidLockedFileLockedError(
                    "{file} ({exc})".format(file=self.path, exc=exc))
            else:
                raise PidLockedFileError(
                    "Can't lock {file} ({exc})".format(file=self.path,
                                                       exc=exc))
        flags = fcntl.fcntl(fd, fcntl.F_GETFD)
        flags |= fcntl.FD_CLOEXEC
        fcntl.fcntl(fd, fcntl.F_SETFD, flags)
        self.fd = fd

    def try_lock(self):
        try:
            fd = os.open(self.path, os.O_WRONLY)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                return False
            else:
                raise PidLockedFileError(
                    "Can't open {file} ({exc})".format(file=self.path,
                                                       exc=exc))
        try:
            fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            if exc.errno == errno.EACCES or exc.errno == errno.EAGAIN:
                raise PidLockedFileLockedError(
                    "{file} ({exc})".format(file=self.path, exc=exc))
            else:
                raise PidLockedFileError(
                    "Can't lock {file} ({exc})".format(file=self.path,
                                                       exc=exc))
        self.close_fd()
        self.fd = fd
        return True

    def unlock(self):
        try:
            if self.fd > 0:
                fcntl.lockf(self.fd, fcntl.LOCK_UN)
        except OSError as exc:
            raise PidLockedFileError(
                "Can't unlock {file} ({exc})".format(file=self.path,
                                                     exc=exc))
        self.close_fd()

    def close_fd(self):
        if self.fd > 0:
            os.close(self.fd)
            self.fd = -1

    def read_pid_from_pidfile(self):
        pid = None
        try:
            pidfile = open(self.path, 'r')
        except OSError as exc:
            raise PidLockedFileError(
                "Unable to read pid from {file} ({exc})".format(
                    exc=exc, file=self.path))

        # According to the FHS 2.3 section on PID files in /var/run:
        #
        #   The file must consist of the process identifier in
        #   ASCII-encoded decimal, followed by a newline character.
        #
        #   Programs that read PID files should be somewhat flexible
        #   in what they accept; i.e., they should ignore extra
        #   whitespace, leading zeroes, absence of the trailing
        #   newline, or additional lines in the PID file.

        num = pidfile.readline().strip()
        try:
            pid = int(num)
        except ValueError:
            pass
        pidfile.close()

        return pid

    def remove_existing_pidfile(self):
        self.close_fd()
        try:
            if self.path:
                os.remove(self.path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise PidLockedFileError(
                "Unable to remove {file} ({exc})".format(
                    exc=exc, file=self.path))
