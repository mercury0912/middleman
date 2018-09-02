__all__ = ['set_close_exec', 'DAEMON_RUNNING', 'daemonize', 'Waker']

import os


if os.name == 'nt':
    from middleman.sysplatform.common import Waker
    from middleman.sysplatform.windows import (
        set_close_exec, daemonize, DAEMON_RUNNING)
else:
    from middleman.sysplatform.posix import (
        set_close_exec, daemonize, DAEMON_RUNNING, Waker)
