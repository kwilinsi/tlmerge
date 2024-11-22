from pathlib import Path
from typing import Literal
import sys
import warnings

import coloredlogs
import logging.handlers


def configure_log(file: Path | None,
                  console: Literal['verbose', 'quiet', 'silent'] | None):
    """
    Configure the logging module.

    :param file: The output file for logs, or None to disable file logging.
    :param console: Whether to log debug messages (verbose), only
     warnings/errors (quiet), or nothing at all (silent) in the console.
    :return: None
    """

    # Configure the root logger
    root = logging.getLogger()

    # Capture warnings in logger
    logging.captureWarnings(True)

    if console == 'silent':
        # Suppress all console messages
        root.setLevel(logging.CRITICAL + 1)
        to_console = False
    else:
        # Add console handler (with color)
        to_console = True
        coloredlogs.install(
            level=(logging.DEBUG if console == 'verbose'
                   else logging.WARN if console == 'quiet' else logging.INFO),
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

        # Show ALL warnings (incl. duplicates) in verbose mode
        if console == 'verbose':
            warnings.simplefilter('always')

    # Add log file if enabled
    to_file = False
    if file is not None:
        # Set root level to ensure all DEBUG messages to go log file
        root.setLevel(logging.DEBUG)

        to_file = True
        # Make sure it's not a directory. Generally this was validated earlier,
        # but that may not have occurred if the log file was specified in the
        # global config file
        if file.is_dir():
            sys.exit(1 if console == 'silent' else
                     f"Invalid log file \"{file}\": that's a directory")

        # Ensure directory exists
        file.parent.mkdir(parents=True, exist_ok=True)

        # Add file handler to root
        file_handler = logging.handlers.RotatingFileHandler(
            file,
            maxBytes=1024 * 1024,  # 1 MB
            backupCount=5,
            encoding='utf-8'
        )

        file_handler.setFormatter(logging.Formatter(
            fmt='%(asctime)s.%(msecs)03d - '
                '%(threadName)-15.15s %(name)-20.20s %(levelname)-8s '
                '%(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))

        root.addHandler(file_handler)

    # Suppress debug messages from PIL that spam the logs with EXIF data
    logging.getLogger('PIL').setLevel(logging.INFO)

    # Log a success message
    if to_file and to_console:
        msg = f'writing to console and log file: "{file}"'
    elif to_file:
        msg = f'console disabled, writing to log file: "{file}"'
    elif to_console:
        msg = 'writing to console, log file disabled'
    else:
        msg = 'both console and log file disabled'

    logging.getLogger(__name__).debug('Initialized logger: ' + msg)
