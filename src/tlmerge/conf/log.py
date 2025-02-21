from collections import deque
from enum import Enum
from os import PathLike
from pathlib import Path
import sys
from typing import Literal, Self
import warnings

import coloredlogs
import logging
from logging import Filter, Formatter, Handler, LogRecord, Logger, StreamHandler
from logging.handlers import RotatingFileHandler


class LogLevel(Enum):
    VERBOSE = 0
    DEFAULT = 1
    QUIET = 2
    SILENT = 3


def configure_log(file: PathLike | str | None,
                  console: LogLevel | None):
    """
    Configure the logging module.

    Note that in console 'verbose' mode, all warnings are logged. This will
    also affect the log file, if enabled.

    :param file: The path to the output file for logs, or None to disable file
     logging.
    :param console: Whether to log debug messages (verbose), only
     warnings/errors (quiet), or nothing at all (silent) in the console.
    :return: None
    """

    # Configure the root logger
    root: Logger = logging.getLogger()

    if console == LogLevel.SILENT:
        # Suppress all console messages
        root.setLevel(logging.CRITICAL + 1)
        to_console = False
    else:
        if console == LogLevel.VERBOSE:
            level = logging.DEBUG
        elif console == LogLevel.QUIET:
            level = logging.WARNING
        else:
            level = logging.INFO

        warnings.simplefilter('always')
        _add_console_logger(root, level)
        to_console = True

    # Add log file if enabled
    to_file = False
    if file is not None:
        # Set root level to ensure all DEBUG messages to go log file
        root.setLevel(logging.DEBUG)
        _add_file_handler(root, Path(file))

    ########################################

    # Capture warnings in logger
    logging.captureWarnings(True)

    # Suppress debug messages from PIL that spam the logs with EXIF data
    logging.getLogger('PIL').setLevel(logging.INFO)

    ########################################

    # Log a success message
    if to_file and to_console:
        msg = f'writing to console and log file: "{file}"'
    elif to_file:
        msg = f'console disabled, writing to log file: "{file}"'
    elif to_console:
        msg = 'writing to console, log file enabled'
    else:
        msg = 'both console and log file disabled'

    logging.getLogger(__name__).debug('Initialized logger: ' + msg)


def _add_console_logger(root: Logger, level: int) -> None:
    """
    Install a coloredlogs console handler to the roto logger.

    :param root: The root logger.
    :param level: The minimum level threshold to log.
    :return: None
    """

    # Add console handler (with color)
    coloredlogs.install(
        level=level,
        logger=root,
        fmt='%(asctime)s %(threadName)-10.10s '
            '%(module)-8.8s %(levelname)-8s %(message)s',
        datefmt='%H:%M:%S',
        field_styles={
            'asctime': {'color': 'green', 'bright': True},
            'threadName': {'color': 'yellow', 'bright': True},
            'levelname': {'color': 'white', 'bright': True},
            'module': {'color': 'cyan'},
        },
        level_styles={
            'debug': {'color': 'white', 'faint': True},
            'info': {'color': 'green', 'bright': True},
            'warning': {'color': 'yellow', 'bright': True},
            'error': {'color': 'red', 'bright': True},
            'critical': {'color': 'red', 'bold': True},
        },
    )


def _add_file_handler(root: Logger, file: Path) -> None:
    """
    Add a rotating file handler to the root logger.

    :param root: The root logger.
    :param file: The path to the log file.
    :return: None
    :raises SystemExit: If the given file is actually a directory.
    """

    # Double check that the log file isn't a directory
    if file.is_dir():
        sys.exit(f"Invalid log file \"{file}\": that's a directory")

    # Ensure directory exists
    file.parent.mkdir(parents=True, exist_ok=True)

    # Add file handler to root
    file_handler = RotatingFileHandler(
        file,
        maxBytes=8 * 1024 * 1024,  # 8 MB
        backupCount=10,
        encoding='utf-8'
    )

    file_handler.setFormatter(Formatter(
        fmt='%(asctime)s.%(msecs)03d - '
            '%(threadName)-15.15s %(name)-20.20s %(levelname)-8s '
            '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))

    root.addHandler(file_handler)


class LogBuffer(Filter):
    def __init__(self,
                 handler: Handler,
                 max_size: int | None = 100) -> None:
        """
        Initialize a log buffer on the given handler. This temporarily
        suppresses and saves all log messages sent to the handler and then
        sends them all at once after the buffer is released.

        Use the max_size parameter to prevent excessive memory usage with lots
        of log messages. When the max size is reached, messages are discarded
        based on their log level and age. That is, all DEBUG messages are
        removed first (starting with the oldest one), followed by INFO,
        WARNING, etc. If the incoming message is a lower level than everything
        in the buffer, then incoming message is itself ignored.

        Call start() or open this buffer in a context manager to begin
        buffering messages.

        :param handler: The logging handler to which to apply this buffer.
        :param max_size: The maximum number of messages that can be stored
         in the buffer at once, or None for an unbounded buffer. Defaults to
         100.
        :return: None
        """

        super().__init__()

        self._handler: Handler = handler

        # Main deque that backs this buffer
        self._buffer: deque[LogRecord] = deque(maxlen=max_size)

        # Keep track of the lowest level record in the buffer. Initialize to
        # an arbitrarily high value certainly out of the typical log level
        # number range
        self._min_level: int = 1000

    def filter(self, record: LogRecord) -> Literal[False]:
        """
        Filter an incoming record to the attached handler.

        :param record: The incoming log record.
        :return: False to block all log records from printing to the console.
        """

        # Easy buffering: max size not yet reached
        if len(self._buffer) < self._buffer.maxlen:
            self._buffer.append(record)
            if record.levelno < self._min_level:
                self._min_level = record.levelno
            return False

        # Buffer is full. Need to find something to remove
        ####################

        # If incoming record would be the lowest level, ignore it
        if record.levelno < self._min_level:
            return False

        # Remove the lowest level record in the deque starting with the oldest
        for i, r in enumerate(self._buffer):
            if r.levelno == self._min_level:
                del self._buffer[i]
                self._buffer.append(record)
                return False

        # Now we're at the rare case where the _min_level was wrong, because
        # in the last call we removed the last remaining record at that level.
        # This requires another iteration over the deque to set the min level,
        # but it's rare
        self._min_level = 1000  # arbitrarily large; much higher than CRITICAL
        for r in self._buffer:
            if r.levelno < self._min_level:
                self._min_level = r.levelno

        # If the incoming record is less than the new min_level, ignore it
        if record.levelno < self._min_level:
            return False

        # Now remove the first record at this NEW min level
        for i, r in enumerate(self._buffer):
            if r.levelno == self._min_level:
                del self._buffer[i]
                self._buffer.append(record)
                return False

        # Unreachable
        raise RuntimeError('Unreachable: no log record in buffer at newly '
                           f'calculated min level {self._min_level}')

    def start(self) -> Self:
        """
        Start buffering log messages to the handler specified at
        initialization. Call release() to stop buffering and release
        all messages.

        :return: Self
        """

        self._handler.addFilter(self)
        return self

    def release(self) -> None:
        """
        Stop buffering log messages, and release all buffered messages to the
        handler.

        :return: None
        """

        self._handler.removeFilter(self)
        while self._buffer:
            self._handler.handle(self._buffer.popleft())

    def __enter__(self) -> Self:
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


def buffer_console_log(**kwargs) -> LogBuffer:
    """
    Get a LogBuffer that will buffer log messages sent to the console. This
    uses the first handler it finds on the root log that is (a) a
    StreamHandler and (b) streams to `sys.stdout` or `sys.stderr`.

    Start the buffer with LogBuffer.start() or by opening it in a context
    manager.

    **kwargs: Additional arguments to pass to LogBuffer().
    :return: A buffer that will run on the console handler.
    :raise RuntimeError: If no console handler can be found.
    """

    for handler in logging.getLogger().handlers:
        if isinstance(handler, StreamHandler) and \
                handler.stream in {sys.stdout, sys.stderr}:
            return LogBuffer(handler, **kwargs)

    raise RuntimeError('Cannot find a console log handler to buffer')
