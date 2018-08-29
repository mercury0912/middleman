__all__ = ['set_close_exec', 'DAEMON_RUNNING', 'daemonize', 'Waker']

import os


if os.name == 'nt':
    from middleman.platform.common import Waker
    from middleman.platform.windows import (
        set_close_exec, daemonize, DAEMON_RUNNING)
else:
    from middleman.platform.posix import (
        set_close_exec, daemonize, DAEMON_RUNNING, Waker)
