import os

from middleman.log import gen_log

try:
    import multiprocessing
except ImportError:
    # Multiprocessing is not available on Google App Engine.
    multiprocessing = None


def cpu_count():
    """Returns the number of processors on this machine."""
    if multiprocessing is None:
        return 1
    try:
        return multiprocessing.cpu_count()
    except NotImplementedError:
        pass
    try:
        return os.sysconf("SC_NPROCESSORS_CONF")
    except (AttributeError, ValueError):
        pass
    gen_log.error("Could not detect number of processors; assuming 1")
    return 1