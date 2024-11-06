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
        CONFIG.update_root(args.config, args)
    except Exception:
        if args.silent:
            sys.exit(1)
        else:
            raise

    # Initialize the logger
    root_config = CONFIG.root
    configure_log(root_config.log, root_config.log_level())
    log = logging.getLogger(__name__)

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
