import asyncio
from argparse import Namespace
import logging
import sys

from .conf import configure_log, CONFIG, parse_cli, write_default_config
from . import run


def main():
    # Parse command line arguments
    args: Namespace = parse_cli()

    # Load the global configuration file
    try:
        global_cfg = CONFIG.update_root(args.config, args)
    except Exception:
        if args.silent:
            sys.exit(1)
        else:
            raise

    # Initialize the logger
    root_config = CONFIG.root
    configure_log(root_config.log, root_config.log_level())
    log = logging.getLogger(__name__)

    # Load the sub-config files
    n = CONFIG.load_all_config_files(args.project, args)
    if n + global_cfg == 0:
        log.info('No config files found')
    elif n == 0:
        log.info('Loaded 1 (global) config file')
    elif not global_cfg:
        log.info(f"Loaded {n} (non-global) config file{'' if n == 1 else 's'}")
    else:
        log.info(f'Loaded {n + 1} total config files')

    # If the config file doesn't exist and --make_config flag is present,
    # create the file with default configuration
    if not args.config.exists() and args.make_config:
        write_default_config(args.config)
        log.info(f'Saved default configuration to "{args.config}"')

    # Get the appropriate task for the user-selected mode
    if args.mode == 'scan':
        task = run.scan(args.project)
    else:
        sys.exit(1 if args.silent else f"Invalid execution mode '{args.mode}'")

    # Run the task in the asyncio event loop
    asyncio.run(task)


if __name__ == '__main__':
    main()
