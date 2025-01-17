from argparse import Namespace
import logging
import sys

from .conf import configure_log, ConfigManager, parse_cli, write_default_config
from .db import DB
from .run import run

_silent: bool = False


def main() -> None:
    global _silent

    # Parse command line arguments
    args: Namespace = parse_cli()

    # Update the global silent variable, used by the 'if name == main' line
    # below to know whether to suppress error output
    _silent = args.silent

    # Create the config manager
    config = ConfigManager(args.project)
    root_cfg = config.root

    # Silent mode may have changed if specified in an environment variable
    _silent = root_cfg.silent()

    # Load the root configuration file
    loaded_root_file, _ = config.update_root(file=args.config, cli=args)

    # Silent mode may have changed again if specified in config file
    _silent = root_cfg.silent()

    # Initialize the logger
    configure_log(root_cfg.log(), root_cfg.log_level())
    log = logging.getLogger(__name__)

    # Load the sub-config files
    n = config.load_all_config_files()
    if n + loaded_root_file == 0:
        log.info('No config files found')
    elif n == 0:
        log.info('Loaded 1 (global) config file')
    elif not loaded_root_file:
        log.info(f"Loaded {n} (non-global) config file{'' if n == 1 else 's'}")
    else:
        log.info(f'Loaded {n + 1} total config files')

    # Reapply the command line arguments
    config.apply_cli_args(args)

    # If the config file doesn't exist and --make_config flag is present,
    # create the file with default configuration
    if not args.config.exists() and args.make_config:
        write_default_config(args.config)
        log.info(f'Saved default configuration to "{args.config}"')

    # Initialize the database
    DB.initialize(root_cfg.database())

    # Run the selected mode
    try:
        run(args.mode, config)
    except Exception as e:  # noqa
        log.critical(f"Fatal error while running '{args.mode}': {e}",
                     exc_info=True)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt as e:
        sys.exit(1 if _silent else 'Keyboard interrupt: terminating')
    except BaseException as e:
        if _silent:
            sys.exit(1)
        else:
            raise
