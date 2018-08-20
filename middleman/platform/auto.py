__all__ = ['DAEMON_RUNNING', 'daemonize']

import os


if os.name == 'nt':
    from middleman.platform.windows import DAEMON_RUNNING
    from middleman.platform.windows import daemonize
else:
    from middleman.platform.posix import DAEMON_RUNNING
    from middleman.platform.posix import daemonize
