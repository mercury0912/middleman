import errno
import os
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE
import threading
import time

from middleman.log import gen_log


_POLL_TIMEOUT = 3600.0


class IOLoop:
    """A level-triggered I/O loop.

    We use ``DefaultSelector`` which is an alias to the most efficient
    implementation available on the current platform. If you are
    implementing a system that needs to handle thousands of
    simultaneous connections, you should use a system that supports
    either ``epoll`` or ``kqueue``.
    """
    # Our events map exactly to the selectors events
    READ = EVENT_READ
    WRITE = EVENT_WRITE

    # Global lock for creating global IOLoop instance
    _instance_lock = threading.Lock()

    _current = threading.local()

    def __init__(self,
                 make_current=None,
                 impl=DefaultSelector(),
                 time_func=None):

        if make_current is None:
            if IOLoop.current(instance=False) is None:
                self.make_current()
        elif make_current:
            if IOLoop.current(instance=False) is not None:
                raise RuntimeError("current IOLoop already exists")
            self.make_current()

        self._impl = impl
        self.time_func = time_func or time.time
        self._stopped = False
        self._running = False
        self._closing = False
        self._thread_ident = None

    @staticmethod
    def instance():
        """Returns a global `IOLoop` instance.

        Most applications have a single, global `IOLoop` running on the
        main thread.  Use this method to get this instance from
        another thread.  In most other cases, it is better to use `current()`
        to get the current thread's `IOLoop`.
        """
        if not hasattr(IOLoop, "_instance"):
            with IOLoop._instance_lock:
                if not hasattr(IOLoop, "_instance"):
                    # New instance after double check
                    IOLoop._instance = IOLoop()
        return IOLoop._instance

    @staticmethod
    def initialized():
        return hasattr(IOLoop, "_instance")

    @staticmethod
    def clear_instance():
        """Clear the global `IOLoop` instance."""
        if hasattr(IOLoop, "_instance"):
            del IOLoop._instance

    @staticmethod
    def current(instance=True):
        """Returns the current thread's `IOLoop`.

        If an `IOLoop` is currently running or has been marked as
        current by `make_current`, returns that instance.  If there is
        no current `IOLoop`, returns `IOLoop.instance()` (i.e. the
        main thread's `IOLoop`, creating one if necessary) if ``instance``
        is true.

        In general you should use `IOLoop.current` as the default when
        constructing an asynchronous object, and use `IOLoop.instance`
        when you mean to communicate to the main thread from a different
        one.

        .. versionchanged:: 4.1
           Added ``instance`` argument to control the fallback to
           `IOLoop.instance()`.
        """
        current = getattr(IOLoop._current, "instance", None)
        if current is None and instance:
            return IOLoop.instance()
        return current

    def make_current(self):
        """Makes this the `IOLoop` for the current thread.

        An `IOLoop` automatically becomes current for its thread
        when it is started, but it is sometimes useful to call
        `make_current` explicitly before starting the `IOLoop`,
        so that code run at startup time can find the right
        instance.

        .. versionchanged:: 4.1
           An `IOLoop` created while there is no current `IOLoop`
           will automatically become current.
        """
        IOLoop._current.instance = self

    @staticmethod
    def clear_current():
        IOLoop._current.instance = None

    def time(self):
        """Returns the current time according to the `IOLoop`'s clock.

        The return value is a floating-point number relative to an
        unspecified time in the past.

        By default, the `IOLoop`'s time function is `time.time`.  However,
        it may be configured to use e.g. `time.monotonic` instead.
        Calls to `add_timeout` that pass a number instead of a
        `datetime.timedelta` should use this function to compute the
        appropriate time, so they can work no matter what time function
        is chosen.
        """
        return self.time_func()

    @staticmethod
    def close_fd(fd):
        try:
            try:
                os.close(fd)
            except TypeError:
                fd.close()
        except OSError:
            pass

    @staticmethod
    def handle_callback_exception(callback):
        gen_log.error("Exception in callback %r", callback, exc_info=True)

    def close(self, all_fds=False):
        self._closing = True
        if all_fds:
            for fd in self._impl.get_map():
                self.close_fd(fd)
        self._impl.close()

    def add_handler(self, fileobj, handler, events):
        try:
            self._impl.register(fileobj, events, handler)
        except (ValueError, KeyError):
            gen_log.exception("Error adding fileobj to IOLoop")

    def update_handler(self, fileobj, events, handler=None):
        try:
            self._impl.modify(fileobj, events, handler)
        except (ValueError, KeyError):
            gen_log.exception("Error updating fileobj to IOLoop")

    def remove_handler(self, fileobj):
        try:
            self._impl.unregister(fileobj)
        except (ValueError, KeyError):
            gen_log.exception("Error deleting fileobj from IOLoop")

    def start(self):
        if self._running:
            raise RuntimeError("IOLoop is already running")
        if self._stopped:
            self._stopped = False
            return
        old_current = getattr(IOLoop._current, "instance", None)
        IOLoop._current.instance = self
        self._thread_ident = threading.get_ident()
        self._running = True

        try:
            while True:
                poll_timeout = _POLL_TIMEOUT

                if not self._running:
                    break

                try:
                    events = self._impl.select(poll_timeout)
                except OSError as exc:
                    if exc.errno == errno.EINTR:
                        continue
                    else:
                        raise
                for key, mask in events:
                    try:
                        handler_func = key.data
                        handler_func(key.fileobj, mask)
                    except OSError as exc:
                        if exc.errno == errno.EPIPE:
                            # Happens when the client closes the connection
                            pass
                        else:
                            self.handle_callback_exception(key)
                    except Exception:
                        self.handle_callback_exception(key)
        finally:
            self._stopped = False
            IOLoop._current.instance = old_current

    def stop(self):
        self._running = False
        self._stopped = True
