import collections
import errno
import functools
import heapq
import itertools
import math
import numbers
import os
import random
import selectors
import threading
import time

from concurrent.futures import ThreadPoolExecutor

from middleman import util
from middleman.log import gen_log
from middleman.process import cpu_count
from middleman.sysplatform.auto import set_close_exec, Waker


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
    READ = selectors.EVENT_READ
    WRITE = selectors.EVENT_WRITE

    # Global lock for creating global IOLoop instance
    _instance_lock = threading.Lock()

    _current = threading.local()

    def __init__(self,
                 make_current=None,
                 impl=selectors.DefaultSelector(),
                 time_func=None):

        if make_current is None:
            if IOLoop.current(instance=False) is None:
                self.make_current()
        elif make_current:
            if IOLoop.current(instance=False) is not None:
                raise RuntimeError("current IOLoop already exists")
            self.make_current()

        self._impl = impl
        if hasattr(self._impl, 'fileno'):
            set_close_exec(self._impl.fileno())
        self._events = {}
        self._callbacks = collections.deque()
        self._timeouts = []
        self._cancellations = 0
        self.time_func = time_func or time.time
        self._stopped = False
        self._running = False
        self._closing = False
        self._thread_ident = None
        self._executor = None

        # unique sequence count
        self._timeout_counter = itertools.count()

        # Create a pipe that we send bogus data to when we want to wake
        # the I/O loop when it is idle
        self._waker = Waker()
        self.add_handler(self._waker.fileno(),
                         self.READ,
                         lambda fd, events: self._waker.consume())

    def close(self, all_fds=False):
        self._closing = True
        self.remove_handler(self._waker.fileno())

        if all_fds:
            for fd in self._impl.get_map():
                self.close_fd(fd)
        self._impl.close()
        self._waker.close()
        self._impl.close()
        self._callbacks = None
        self._timeouts = None
        if self._executor is not None:
            self._executor.shutdown()

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

    def add_handler(self, fileobj, events, data):
        try:
            self._impl.register(fileobj, events, data)
        except (ValueError, KeyError):
            gen_log.exception("Error adding fileobj to IOLoop")

    def update_handler(self, fileobj, events, data):
        try:
                self._impl.modify(fileobj, events, data)
        except (ValueError, KeyError):
            gen_log.exception("Error updating fileobj to IOLoop")

    def remove_event(self, fileobj, mask):
        fd = util.fileobj_to_fd(fileobj)
        key_mask = self._events.get(fd, None)
        if key_mask is not None and key_mask[1] & ~mask == 0:
            self._events.pop(fd)

    def remove_handler(self, fileobj):
        try:
            fd = util.fileobj_to_fd(fileobj)
            self._events.pop(fd, None)
            self._impl.unregister(fileobj)
        except (ValueError, KeyError):
            gen_log.exception("Error deleting fileobj from IOLoop")

    def get_key(self, fileobj):
        try:
            return self._impl.get_key(fileobj)
        except KeyError:
            gen_log.exception("Error getting key from IOLoop")

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
        self._timeout_counter = itertools.count()

        try:
            while True:
                # Prevent IO event starvation by delaying new callbacks
                # to the next iteration of the event loop.
                ncallbacks = len(self._callbacks)

                # Add any timeouts that have come due to the callback list.
                # Do not run anything until we have determined which ones
                # are ready, so timeouts that call add_timeout cannot
                # schedule anything in this iteration.
                due_timeouts = []
                if self._timeouts:
                    now = self.time()
                    while self._timeouts:
                        if self._timeouts[0].callback is None:
                            # The timeout was cancelled. Note that the
                            # cancellation check is repeated below for timeouts
                            # that are cancelled by another timeout or callback.
                            heapq.heappop(self._timeouts)
                            self._cancellations -= 1
                        elif self._timeouts[0].deadline <= now:
                            due_timeouts.append(heapq.heappop(self._timeouts))
                        else:
                            break
                    if (self._cancellations > 512 and
                            self._cancellations > (len(self._timeouts) >> 1)):
                        # Clean up the timeout queue when it gets large and
                        # it's more than half queue length
                        self._cancellations = 0
                        self._timeouts = [x for x in self._timeouts
                                          if x.callback is not None]
                        heapq.heapify(self._timeouts)

                for i in range(ncallbacks):
                    self._run_callback(self._callbacks.popleft())
                for timeout in due_timeouts:
                    if timeout.callback is not None:
                        self._run_callback(timeout.callback)
                # Closures may be holding on to a lot of memory, so allow
                # them to be freed before we go into our poll wait.
                due_timeouts = timeout = None

                if self._callbacks:
                    # If any callbacks or timeouts called add_callback,
                    # we don't want to wait in poll() before we run them.
                    poll_timeout = 0.0
                elif self._timeouts:
                    # if there are any timeouts, schedule the first one.
                    # Use self.time() instead of 'now' to account for time
                    # spent running callbacks.
                    poll_timeout = self._timeouts[0].deadline - self.time()
                    poll_timeout = max(0, min(poll_timeout, _POLL_TIMEOUT))
                else:
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

                # Pop one fd at a time from the set of pending fds and run
                # its handler. Since that handler may perform actions on
                # other file descriptors, there may be reentrant calls to
                # this IOLoop that modify self._events
                event_pairs = {key.fd: (key, mask) for key, mask in events}
                self._events.update(event_pairs)
                while self._events:
                    fd, key_mask = self._events.popitem()
                    try:
                        handler_func = key_mask[0].data
                        handler_func(key_mask[0].fileobj, key_mask[1])
                    except OSError as exc:
                        if exc.errno == errno.EPIPE:
                            # Happens when the client closes the connection
                            pass
                        else:
                            self.handle_callback_exception(key_mask)
                    except Exception:
                        self.handle_callback_exception(key_mask)
                    # Release resources held by us
                    key_mask = handler_func = None
        finally:
            self._stopped = False
            IOLoop._current.instance = old_current

    def stop(self):
        self._running = False
        self._stopped = True
        self._waker.wake()

    def add_timeout(self, deadline, callback, *args, **kwargs):
        """Runs the ``callback`` at the time ``deadline`` from the I/O loop.

        Returns an opaque handle that may be passed to
        `remove_timeout` to cancel.

        ``deadline`` may be a number denoting a time (on the same
        scale as `IOLoop.time`, normally `time.time`)

        Note that it is not safe to call `add_timeout` from other threads.
        Instead, you must use `add_callback` to transfer control to the
        `IOLoop`'s thread, and then call `add_timeout` from there.
        """
        if isinstance(deadline, numbers.Real):
            return self.call_at(deadline, callback, *args, **kwargs)
        else:
            raise TypeError("Unsupport deadline %r" % deadline)

    def call_at(self, deadline, callback, *args, **kwargs):
        timeout = _Timeout(
            deadline,
            functools.partial(callback, *args, **kwargs),
            self)
        heapq.heappush(self._timeouts, timeout)
        return timeout

    def remove_timeout(self, timeout):
        # Removing from a heap is complicated, so just leave the defunct
        # timeout object in the queue (see discussion in
        # http://docs.python.org/library/heapq.html).
        # If this turns out to be a problem, we could add a garbage
        # collection pass whenever there are too many dead timeouts.
        timeout.callback = None
        self._cancellations += 1

    def _run_callback(self, callback):
        """Runs a callback with error handing."""
        try:
            callback()
        except Exception:
            self.handle_callback_exception(callback)

    def _add_done_callback(self, callback, future):
        self.add_callback(callback, future)

    def add_future(self, future, callback):
        if future.done():
            callback(future)
        else:
            func = functools.partial(self._add_done_callback, callback)
            future.add_done_callback(func)

    def run_in_exector(self, callback, func, *args):
        """Runs a function in a ``concurrent.futures.Executor``. If
        ``executor`` is ``None``, the IO loop's default executor will be used.

        Use `functools.partial` to pass keyword arguments to ``func``.

        .. versionadded:: 5.0
        """
        if ThreadPoolExecutor is None:
            raise RuntimeError(
                "concurrent.futures is required to use IOLoop.run_in_executor")
        # Changed in version 3.5: If max_workers is None or not given,
        # it will default to the number of processors on the machine,
        # multiplied by 5, assuming that ThreadPoolExecutor is often
        # used to overlap I/O instead of CPU work and the number of
        #  workers should be higher than the number of workers
        # for ProcessPoolExecutor.
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=(cpu_count() * 5))
        future = self._executor.submit(func, *args)
        self.add_future(future, callback)
        return future

    def add_callback(self, callback, *args, **kwargs):
        if self._closing:
            return
        # Blindly insert into self._callbacks. This is safe even
        # from signal handlers because deque.append is atomic.
        self._callbacks.append(functools.partial(
            callback, *args, **kwargs))
        if threading.get_ident() != self._thread_ident:
            # This will write one byte but Waker.consume() reads many
            # at once, so it's ok to write even when not strictly
            # necessary.
            self._waker.wake()
        else:
            # If we're on the IOLoop's thread, we don't need to wake anyone.
            pass


class _Timeout:
    """An IOLoop timeout, a UNIX timestamp and a callback"""

    # Reduce memory overhead when there are lots of pending callbacks
    __slots__ = ['deadline', 'callback', 'tdeadline']

    def __init__(self, deadline, callback, io_ioop):
        if not isinstance(deadline, numbers.Real):
            raise TypeError("Unsupported deadline %r" % deadline)
        self.deadline = deadline
        self.callback = callback
        self.tdeadline = (deadline, next(io_ioop._timeout_counter))

    # Comparison methods to sort by deadline, with object id as a tiebreaker
    # to guarantee a consistent ordering.  The heapq module uses __le__
    # in python2.5, and __lt__ in 2.6+ (sort() and most other comparisons
    # use __lt__).
    def __lt__(self, other):
        return self.tdeadline < other.tdeadline

    def __le__(self, other):
        return self.tdeadline <= other.tdeadline


class PeriodicCallback:
    """Schedules the given callback to be called periodically.

    The callback is called every ``callback_time`` milliseconds.

    If ``jitter`` is specified, each callback time will be randomly
    selected within a window of ``jitter * callback_time`` milliseconds.
    Jitter can be used to reduce alignment of events with similar periods.
    A jitter of 0.1 means allowing a 10% variation in callback time.
    The window is centered on ``callback_time`` so the total number of
    calls within a given interval should not be significantly affected by
    adding jitter.

    If the callback runs for longer than ``callback_time`` milliseconds,
    subsequent invocations will be skipped to get back on schedule.

    `start` must be called after the `PeriodicCallback` is created."""
    def __init__(self, callback, callback_time, jitter=0):
        self.callback = callback
        if callback_time <= 0:
            raise ValueError("Periodic callback must have a positive callback_time")
        self.callback_time = callback_time
        self.jitter = jitter
        self._running = False
        self._timeout = None

    def start(self):
        """Starts the timer."""
        # Looking up the IOLoop here allows to first instantiate the
        # PeriodicCallback in another thread, then start it using
        # IOLoop.add_callback().
        self.io_loop = IOLoop.current()
        self._running = True
        self._next_timeout = self.io_loop.time()
        self._schedule_next()

    def stop(self):
        """Stops the timer."""
        self._running = False
        if self._timeout is not None:
            self.io_loop.remove_timeout(self._timeout)
            self._timeout = None
        self.callback = None

    def is_running(self):
        """Return True if this `.PeriodicCallback` has been started."""
        return self._running

    def _run(self):
        if not self._running:
            return
        self._timeout = None
        try:
            return self.callback()
        except Exception:
            self.io_loop.handle_callback_exception(self.callback)
        finally:
            self._schedule_next()

    def _schedule_next(self):
        if self._running:
            self._update_next(self.io_loop.time())
            self._timeout = self.io_loop.add_timeout(self._next_timeout, self._run)

    def _update_next(self, current_time):
        callback_time_sec = self.callback_time / 1000.0
        if self.jitter:
            # apply jitter fraction
            callback_time_sec *= 1 + (self.jitter * (random.random() - 0.5))
        if self._next_timeout <= current_time:
            # The period should be measured from the start of one call
            # to the start of the next. If one call takes too long,
            # skip cycles to get back to a multiple of the original
            # schedule.
            self._next_timeout += (math.floor((current_time - self._next_timeout) /
                                              callback_time_sec) + 1) * callback_time_sec
        else:
            # If the clock moved backwards, ensure we advance the next
            # timeout instead of recomputing the same value again.
            # This may result in long gaps between callbacks if the
            # clock jumps backwards by a lot, but the far more common
            # scenario is a small NTP adjustment that should just be
            # ignored.
            #
            # Note that on some systems if time.time() runs slower
            # than time.monotonic() (most common on windows), we
            # effectively experience a small backwards time jump on
            # every iteration because PeriodicCallback uses
            # time.time() while asyncio schedules callbacks using
            # time.monotonic().
            # https://github.com/tornadoweb/tornado/issues/2333
            self._next_timeout += callback_time_sec
