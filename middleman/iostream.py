import collections
import errno
import sys

from middleman import ioloop
from middleman.log import gen_log

# These errnos indicate that a non-blocking operation must be retried
# at a later time.  On most platforms they're the same value, but on
# some they differ.
_ERRNO_WOULDBLOCK = (errno.EWOULDBLOCK, errno.EAGAIN)

if hasattr(errno, "WSAEWOULDBLOCK"):
    _ERRNO_WOULDBLOCK += (errno.WSAEWOULDBLOCK,)

# These errnos indicate that a connection has been abruptly terminated.
# They should be caught and handled less noisily than other errors.
_ERRNO_CONNRESET = (errno.ECONNRESET, errno.ECONNABORTED, errno.EPIPE,
                    errno.ETIMEDOUT)

if hasattr(errno, "WSAECONNRESET"):
    _ERRNO_CONNRESET += (errno.WSAECONNRESET, errno.WSAECONNABORTED, errno.WSAETIMEDOUT)  # type: ignore # noqa: E501

if sys.platform == 'darwin':
    # OSX appears to have a race condition that causes send(2) to return
    # EPROTOTYPE if called while a socket is being torn down:
    # http://erickt.github.io/blog/2014/11/19/adventures-in-debugging-a-potential-osx-kernel-bug/
    # Since the socket is being closed anyway, treat this as an ECONNRESET
    # instead of an unexpected error.
    _ERRNO_CONNRESET += (errno.EPROTOTYPE,)  # type: ignore

# More non-portable errnos:
_ERRNO_INPROGRESS = (errno.EINPROGRESS,)

if hasattr(errno, "WSAEINPROGRESS"):
    _ERRNO_INPROGRESS += (errno.WSAEINPROGRESS,)  # type: ignore

_WINDOWS = sys.platform.startswith('win')


class StreamBufferFullError(BufferError):
    """Exception raised by `IOStream` methods when the buffer is full.
    """


class StreamClosedError(IOError):
    """Exception raised by `IOStream` methods when the stream is closed."""


class StreamConnresetError(OSError):
    """Exception raised bye ``_ERRNO_CONNRESET``"""


class _StreamBuffer(object):
    """
    A specialized buffer that tries to avoid copies when large pieces
    of data are encountered.
    """

    def __init__(self):
        # A sequence of (False, bytearray) and (True, memoryview) objects
        self._buffers = collections.deque()
        # Position in the first buffer
        self._first_pos = 0
        self._size = 0

    def __len__(self):
        return self._size

    # Data above this size will be appended separately instead
    # of extending an existing bytearray
    _large_buf_threshold = 2048

    def append(self, data):
        """
        Append the given piece of data (should be a buffer-compatible object).
        """
        size = len(data)
        if size > self._large_buf_threshold:
            if not isinstance(data, memoryview):
                data = memoryview(data)
            self._buffers.append((True, data))
        elif size > 0:
            if self._buffers:
                is_memview, b = self._buffers[-1]
                new_buf = is_memview or len(b) >= self._large_buf_threshold
                if not new_buf:
                    b += data
            else:
                new_buf = True
            if new_buf:
                self._buffers.append((False, bytearray(data)))

        self._size += size

    def peek(self, size):
        """
        Get a view over at most ``size`` bytes (possibly fewer) at the
        current buffer position.
        """
        assert size > 0
        try:
            is_memview, b = self._buffers[0]
        except IndexError:
            return memoryview(b'')

        pos = self._first_pos
        if is_memview:
            return b[pos:pos + size]
        else:
            return memoryview(b)[pos:pos + size]

    def advance(self, size):
        """
        Advance the current buffer position by ``size`` bytes.
        """
        assert 0 < size <= self._size
        self._size -= size
        pos = self._first_pos

        buffers = self._buffers
        while buffers and size > 0:
            is_large, b = buffers[0]
            b_remain = len(b) - size - pos
            if b_remain <= 0:
                buffers.popleft()
                size -= len(b) - pos
                pos = 0
            elif is_large:
                pos += size
                size = 0
            else:
                # Amortized O(1) shrink for Python 2
                pos += size
                if len(b) <= 2 * pos:
                    del b[:pos]
                    pos = 0
                size = 0

        assert size == 0
        self._first_pos = pos


class IOStream:
    READ_CHUNK_SIZE = 1024 * 16
    MAX_BUFFER_SIZE = 104857600

    def __init__(self, socket,
                 max_buffer_size=MAX_BUFFER_SIZE,
                 read_chunk_size=READ_CHUNK_SIZE,
                 max_write_buffer_size=MAX_BUFFER_SIZE):
        self.socket = socket
        self.socket.setblocking(False)
        self._closed = False
        self.io_loop = ioloop.IOLoop.current()
        self._state = None
        self.max_buffer_size = max_buffer_size
        self.read_chunk_size = read_chunk_size
        self._read_chunk = memoryview(bytearray(self.read_chunk_size))
        self.read_buffer = bytearray()
        self.read_buffer_pos = 0
        self.read_buffer_size = 0
        self.read_target_bytes = None
        self._total_read_done_index = 0
        self._write_buffer = _StreamBuffer()
        self.max_write_buffer_size = max_write_buffer_size
        self._total_write_done_index = 0
        self.FIN_received = False
        self._connecting = False

    def read_from_fd(self, buf):
        try:
            return self.socket.recv_into(buf)
        except BlockingIOError:
            return None
        finally:
            buf = None

    def handle_read(self):
        while True:
            try:
                bytes_read = self.read_from_fd(self._read_chunk)
            except OSError as e:
                if e.errno == errno.EINTR:
                    continue
                else:
                    # Treat ECONNRESET as a connection close rather than
                    # an error to minimize log spam  (the exception will
                    # be raised for apps that care).
                    if e.errno in _ERRNO_CONNRESET:
                        gen_log.info("Read error on fd %s: %s",
                                     self.fileno(), e)
                        raise StreamConnresetError
                    else:
                        gen_log.error("Read error on fd %s: %s",
                                      self.fileno(), e)
                        raise
            else:
                break
        # BlockingIOError
        if bytes_read is None:
            return None

        if bytes_read == 0:
            self.FIN_received = True
            return 0
        self._total_read_done_index += bytes_read
        self.read_buffer += self._read_chunk[:bytes_read]
        self.read_buffer_size += bytes_read
        if self.read_buffer_size > self.max_buffer_size:
            gen_log.error("Reached maximum read buffer size %s",
                          self.max_buffer_size)
            raise StreamBufferFullError("Reached maximum read buffer size")

        if (self.read_target_bytes is not None and
                self.read_target_bytes > self.read_buffer_size):
            return None
        return bytes_read

    def consume(self, length):
        assert 0 <= length <= self.read_buffer_size
        self.read_buffer_pos += length
        self.read_buffer_size -= length
        # Amortized O(1) shrink
        # (this heuristic is implemented natively in Python 3.4+
        #  but is replicated here for Python 2)
        if self.read_buffer_pos > self.read_buffer_size:
            del self.read_buffer[:self.read_buffer_pos]
            self.read_buffer_pos = 0

    def write_to_fd(self, data):
        try:
            return self.socket.send(data)
        finally:
            # Avoid keeping to data, which can be a memoryview.
            # See https://github.com/tornadoweb/tornado/pull/2008
            del data

    def handle_write(self):
        total_written = 0
        while True:
            size = len(self._write_buffer)
            if not size:
                break
            try:
                if _WINDOWS:
                    # On windows, socket.send blows up if given a
                    # write buffer that's too large, instead of just
                    # returning the number of bytes it was able to
                    # process.  Therefore we must not call socket.send
                    # with more than 128KB at a time.
                    size = 128 * 1024
                num_bytes = self.write_to_fd(self._write_buffer.peek(size))
            except BlockingIOError:
                break
            except OSError as e:
                if e.errno == errno.EINTR:
                    continue
                elif e.errno in _ERRNO_CONNRESET:
                    # Broken pipe errors are usually caused by connection
                    # reset, and its better to not log EPIPE errors to
                    # minimize log spam
                    gen_log.info("Write error on fd %s: %s",
                                 self.fileno(), e)
                    raise StreamConnresetError
                else:
                    gen_log.error("Write error on fd %s: %s",
                                  self.fileno(), e)
                    raise
            else:
                if num_bytes == 0:
                    break
                self._write_buffer.advance(num_bytes)
                self._total_write_done_index += num_bytes
                total_written += num_bytes
        return total_written

    def write(self, data):
        if self._closed:
            raise StreamClosedError('Stream is closed')
        if data:
            if (self.max_write_buffer_size is not None and
                    len(self._write_buffer) + len(data) > self.max_write_buffer_size):
                raise StreamBufferFullError("Reached maximum write buffer size %s" %
                                            self.max_write_buffer_size)
            self._write_buffer.append(data)
        nwrite = self.handle_write()
        if self._write_buffer:
            self.add_io_state(self.io_loop.WRITE)
        return nwrite

    def writing(self):
        """Returns true if we are currently writing to the stream."""
        return bool(self._write_buffer)

    def connect(self, address, handler):
        self._connecting = True
        try:
            self.socket.connect(address)
        except OSError as e:
            # In non-blocking mode we expect connect() to raise an
            # exception with EINPROGRESS or EWOULDBLOCK.
            #
            # On freebsd, other errors such as ECONNREFUSED may be
            # returned immediately when attempting to connect to
            # localhost, so handle them the same way as an error
            # reported later in _handle_connect.
            if (e.errno not in _ERRNO_INPROGRESS and
                    e.errno not in _ERRNO_WOULDBLOCK):
                gen_log.warning("Connect error on fd %s: %s",
                                self.socket.fileno(), e)
                raise
            else:
                return -1
        self.add_io_state(self.io_loop.WRITE, handler)
        return 0

    def add_io_state(self, state, handler=None):
        if self._closed:
            return
        if self._state is None:
            assert handler is not None
            self._state = state
            self.io_loop.add_handler(self.socket, self._state, handler)
        elif not self._state & state:
            self._state = self._state | state
            self.io_loop.update_handler(self.socket, self._state)

    def update_io_state(self, state):
        if self._state != state:
            self.io_loop.update_handler(self.socket, self._state)

    def remove_event(self, mask):
        self.io_loop.remove_event(self.socket, mask)

    def fileno(self):
        return self.socket.fileno()

    def close(self):
        if not self._closed:
            self._closed = True
            if self._state is not None:
                self.io_loop.remove_handler(self.socket)
                self._state = None
            self.socket.close()
            self.socket = None
            self._read_chunk = None
            self.read_buffer = None
            self._write_buffer = None
