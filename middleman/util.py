import sys


def _print_message(message, file=None):
    if message:
        if file is None:
            file = sys.stderr
        file.write(message)
        file.write('\n')


def exit_prog(status=0, message=None):
    if message:
        _print_message(message, sys.stderr)
    sys.exit(status)


def fileobj_to_fd(fileobj):
    """Return a file descriptor from a file object.

    Parameters:
    fileobj -- file object or file descriptor

    Returns:
    corresponding file descriptor

    Raises:
    ValueError if the object is invalid
    """
    if isinstance(fileobj, int):
        fd = fileobj
    else:
        try:
            fd = int(fileobj.fileno())
        except (AttributeError, TypeError, ValueError):
            raise ValueError("Invalid file object: "
                             "{!r}".format(fileobj)) from None
    if fd < 0:
        raise ValueError("Invalid file descriptor: {}".format(fd))
    return fd
