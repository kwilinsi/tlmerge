from argparse import ArgumentParser, Namespace, SUPPRESS
from datetime import datetime
import os
from pathlib import Path
import sys

from .validation import validate_log_level
from .const import DEFAULT_CONFIG_FILE, DEFAULT_DATABASE_FILE, DEFAULT_LOG_FILE


def _build_parser() -> ArgumentParser:
    """
    Build the parser for parsing the command line arguments.

    :return: The argument parser.
    """

    parser: ArgumentParser = ArgumentParser(
        description="Timelapse merging arguments. Command line arguments will "
                    "override configuration settings."
    )

    # The running mode
    parser.add_argument(
        'mode',
        choices=['scan'],
        help='Select an execution mode to process the timelapse images.'
    )

    # Positional argument for the timelapse project directory
    parser.add_argument(
        '-p', '--project',
        metavar='PATH',
        type=Path,
        default=Path(os.getcwd()),
        help='Path to the timelapse project directory. Defaults to the '
             'current working directory.'
    )

    parser.add_argument(
        '-c', '--config',
        metavar='FILE',
        type=Path,
        default=None,
        help='The name of the configuration file(s), or a path to the global '
             f'config file. Defaults to "{DEFAULT_CONFIG_FILE}" in the '
             'timelapse project root.'
    )

    parser.add_argument(
        '-d', '--database',
        metavar='FILE',
        type=Path,
        default=None,
        help='Path to the database file. Defaults to '
             f'"{DEFAULT_DATABASE_FILE}" in the timelapse project root.'
    )

    parser.add_argument(
        '--make_config',
        action='store_true',
        help="If the global config file doesn't already exist, create it"
             "using the default configuration"
    )

    #################### LOGGING CONTROLS ####################

    parser.add_argument(
        '--log',
        metavar='FILE',
        nargs='?',
        type=Path,
        default=SUPPRESS,
        help="Path to the log file. If omitted, this defaults to "
             f"\"{DEFAULT_LOG_FILE}\". If this is a file name path, it's "
             "located in the timelapse project directory. If this is some "
             "other relative path, it's resolved relative to the current "
             "working directory. And if you include the --log flag but don't "
             "specify a file, the log file is not used. Using --log (without "
             "a file) and --silent together disables all logging."
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Log all debug messages in the console.'
    )

    # Optional argument for quiet mode
    parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help="Don't log info messages in the console. "
             "Only show warnings and errors."
    )

    # Optional argument for silent mode
    parser.add_argument(
        '-s', '--silent',
        action='store_true',
        help="Don't log anything to the console."
    )

    #################### DATES AND GROUPS ####################

    parser.add_argument(
        '--date_format',
        metavar='FORMAT',
        default=SUPPRESS,
        help="The date format used in the directory names for each date. "
             "Defaults to yyyy-mm-dd."
    )

    parser.add_argument(
        '--include_dates',
        metavar='DATE',
        nargs='*',
        default=SUPPRESS,
        help="Zero or more dates to specifically include. Note that all dates "
             "are included by default. This is potentially useful to override "
             "excluded dates."
    )

    parser.add_argument(
        '--exclude_dates',
        metavar='DATE',
        nargs='*',
        default=SUPPRESS,
        help="Zero or more dates to exclude. This is overridden by "
             "include_dates."
    )

    parser.add_argument(
        '--include_groups',
        metavar='DATE/GROUP',
        nargs='*',
        default=SUPPRESS,
        help="Zero or more groups to specifically include. Each group must "
             "include the associated date (e.g. "
             f"{datetime.today().strftime('%Y-%m-%d')}{os.path.sep}a). "
             "Note that all groups are included by default. This is "
             "potentially useful to override excluded groups."
    )

    parser.add_argument(
        '--exclude_groups',
        metavar='DATE/GROUP',
        nargs='*',
        default=SUPPRESS,
        help="Zero or more groups to exclude. Each group must include the "
             "associated date (e.g. "
             f"{datetime.today().strftime('%Y-%m-%d')}{os.path.sep}b). "
             "This is overridden by include_groups."
    )

    parser.add_argument(
        '--group_ordering',
        choices=['abc', 'natural', 'num'],
        help="Specify the order in which to process groups. Use 'natural' to "
             "use the natural sort order for strings. Use 'abc' to for an "
             "intuitive order with a/b/c groups: 'a', 'b', ..., 'y', 'z', "
             "'aa', 'ab', ..., 'az', 'ba', etc. Or use 'num' to sort groups "
             "in numerical order (this only works if all group names "
             "exclusively use digits. This does support decimals). The "
             "default is 'abc'."
    )

    #################### CAMERA SETTINGS ####################

    parser.add_argument(
        '--white_balance',
        metavar=('RED', 'GREEN1', 'BLUE', 'GREEN2'),
        type=tuple[float, float, float, float],
        nargs=4,
        default=SUPPRESS,
        help="White balance multipliers for raw images: red, green, blue, "
             "green. Defaults to 1.0 for each. See dcraw documentation for "
             "-r flag."
    )

    parser.add_argument(
        '--chromatic_aberration',
        metavar=('RED', 'BLUE'),
        type=tuple[float, float],
        nargs=2,
        default=SUPPRESS,
        help="Chromatic aberration multipliers for raw images: red and blue. "
             "Defaults to 1.0 to each. See dcraw documentation for -C flag."
    )

    parser.add_argument(
        '--median_filter',
        metavar='PASSES',
        type=int,
        default=SUPPRESS,
        help="The number of passes with a 3x3 median filter. Defaults to 0. "
             "See dcraw documentation for -m flag."
    )

    parser.add_argument(
        '--dark_frame',
        metavar='FILE',
        default=SUPPRESS,
        help="An optional dark frame to subtract from raw images. See dcraw "
             "documentation for -K flag."
    )

    return parser


def _resolve_file_path(args: Namespace,
                       path: Path | None,
                       default: str | Path,
                       name: str) -> Path | None:
    """
    This is a helper function for resolving paths to the config and database
    files. It resolves the user provided path.

    :param args: The parsed command line arguments.
    :param path: The current path to resolve.
    :param default: The default file name to use if the path is None.
    :param name: The name of this file, used in sys.exit() error messages.
    :return: The resolved path.
    """

    # Switch to the default value if the given path is None
    if path is None:
        path = default if isinstance(default, Path) else Path(default)

    try:
        if path.is_absolute():
            # If the user gave an absolute path, go with that
            pass
        elif path.parent == Path('.'):
            # If the user-provided path is just a file name, resolve it
            # relative to the root directory
            path = args.project / path
        else:
            # Otherwise, if the user gave a path like "foobar/something.txt"
            # or "../file.tlmerge" resolve relative to the CWD
            path = path.resolve()
    except KeyboardInterrupt:
        raise
    except Exception as e:
        _exit(args, f'Invalid {name} file "{path}". '
                    f'{e.__class__.__name__}: {e}')

    # Ensure that the file isn't a directory
    if path.is_dir():
        _exit(args, f'Invalid {name} file: "{path}" is a directory')

    # Return the absolute path
    return path


def _validate(args: Namespace) -> None:
    """
    Validate parsed command line arguments. This will call sys.exit() if the
    user chose incompatible arguments.

    The paths for the timelapse directory, config file, and database file, are
    all resolved and validated.

    :param args: The parsed command line arguments.
    :return: None
    """

    # Make sure the user didn't try to use multiple log level flags
    try:
        validate_log_level(args.verbose, args.quiet, args.silent)
    except ValueError as e:
        _exit(args, str(e))

    # Resolve and validate the timelapse project directory
    try:
        if not args.project.is_absolute():
            args.project = args.project.resolve()

        # Ensure it's a directory
        if not args.project.exists():
            _exit(args, "Invalid timelapse project directory: "
                        f"\"{args.project}\" doesn't exist")
        elif not args.project.is_dir():
            _exit(args, "Invalid timelapse project directory: "
                        f"\"{args.project}\" isn't a directory")
    except KeyboardInterrupt:
        raise
    except Exception as e:
        _exit(args, f'Invalid timelapse project directory: '
                    f'"{args.project}". {e.__class__.__name__}: {e}')

    # Resolve the config file path
    args.config = _resolve_file_path(
        args,
        args.config,
        DEFAULT_CONFIG_FILE,
        'configuration'
    )

    # Resolve the database file path
    args.database = _resolve_file_path(
        args,
        args.database,
        DEFAULT_DATABASE_FILE,
        'database'
    )

    # Resolve the log file path only if the flag was specified
    if hasattr(args, 'log'):
        args.log = _resolve_file_path(
            args,
            args.log,
            DEFAULT_LOG_FILE,
            'log'
        )


def _exit(args: Namespace, msg: str) -> None:
    """
    Call sys.exit() to immediately exit the program with the given error
    message. Note that if the silent flag is enabled, the message is omitted.
    Either way, the program exits with error code 1.

    :param args: The parsed command line arguments.
    :param msg: The message to print.
    :return: None
    """

    sys.exit(1 if args.silent else msg)


def parse_cli() -> Namespace:
    """
    Parse and validate the command line arguments.

    :return: The parsed command line argument namespace.
    """

    parser = _build_parser()

    # Parse command line arguments
    namespace: Namespace = parser.parse_args()

    # Validate the arguments
    _validate(namespace)

    return namespace