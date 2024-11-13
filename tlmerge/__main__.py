import asyncio
from argparse import Namespace
import logging
import sys

from .conf import configure_log, CONFIG, parse_cli, write_default_config
from .db import DB
from .run import run

_silent: bool = False


async def main():
    # Parse command line arguments
    args: Namespace = parse_cli()

    # Load the global configuration file
    try:
        global_cfg = CONFIG.update_root(file=args.config, args=args)
    except Exception:
        if args.silent:
            sys.exit(1)
        else:
            raise

    # Get the root config record
    root_config = CONFIG.root
    global _silent
    _silent = root_config.silent

    # Initialize the logger
    configure_log(root_config.log, root_config.log_level())
    log = logging.getLogger(__name__)

    # Load the sub-config files
    n = await CONFIG.load_all_config_files(args.project, args)
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

    # Initialize the database
    await DB.initialize(root_config.database)

    # Run the selected mode
    try:
        await run(args.mode, args.project)
    except Exception as e:  # noqa
        log.critical(f"Fatal error while running '{args.mode}': {e}",
                     exc_info=True)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt as e:
        sys.exit(1 if _silent else f'Keyboard interrupt: terminating')
    except BaseException as e:
        if _silent:
            sys.exit(1)
        else:
            raise
