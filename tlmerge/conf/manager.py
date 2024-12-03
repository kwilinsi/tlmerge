from argparse import Namespace
from datetime import date
import logging
import os
from pathlib import Path
from typing import Optional

from ruamel.yaml import YAML

from .config import Config, ConfigView, GlobalConfig, GlobalConfigView
from .validation import GlobalConfigModel
from . import DEFAULT_DATABASE_FILE, DEFAULT_CONFIG_FILE

# Define the YAML reader for parsing config files
_yaml = YAML()
_yaml.sequence_indent = 4
_yaml.sequence_dash_offset = 2


class ConfigManager:
    def __init__(self):
        # Root config node
        self._modifiable_root = GlobalConfig()
        self._root_view = GlobalConfigView(self._modifiable_root)

        # Config records in this tree inherit from the view
        self._tree: dict[tuple[str, Optional[str]], Config] = {}

        # Same as the _tree but with views for each config
        self._view_tree: dict[tuple[str, Optional[str]], ConfigView] = {}

    @property
    def modifiable_root(self) -> GlobalConfig:
        return self._modifiable_root

    @property
    def root(self) -> GlobalConfigView:
        return self._root_view

    def __getitem__(self, key) -> ConfigView:
        """
        Get a config view via indexing by specifying a date and group. If
        both are omitted, this returns a view of the root/global config. For
        date configs, the group index can be omitted.

        :param key: Zero, one, or two strings. Either the last string or both
        of them can be None.
        :return: A view of the most specific Config record for this date/group.
        """

        # Use the root config if the index is None, an empty tuple, (None),
        # or (None, None).
        if key is None or (isinstance(key, tuple) and
                           (len(key) == 0 or key == (None,) or
                            key == (None, None))):
            return self.root

        # Validate key type, and separate into date and group
        if isinstance(key, str):
            # Allow a single string
            dt, grp = key, None
        elif isinstance(key, tuple) and len(key) <= 2 and \
                all(k is None or isinstance(k, str) for k in key):
            # Allow a tuple if it's 1 or 2 strings or None
            if len(key) == 2:
                dt, grp = key
            else:
                dt, grp = key[0], None
        else:
            # Reject everything else
            raise KeyError('Expected (date, group) indices when getting '
                           f'config record: got "{key}"')

        # If either is blank, raise an error
        if (dt is not None and not dt.strip()) or \
                (grp is not None and not grp.strip()):
            raise KeyError('Invalid date/group indices for config record: '
                           f"keys can't be blank, but got \"{key}\"")

        if dt is None:
            raise ValueError(
                f"Can't get config for group '{grp}' with no date. You must "
                "specify a date if you specify a group"
            )
        elif grp is None:
            return self._view_tree.get((dt, None), self.root)  # noqa
        else:
            # Get the group config; if that's not found, get the date config;
            # if that's not found, get the root config
            return self._view_tree.get(
                (dt, grp),
                self._view_tree.get((dt, None), self.root)
            )

    def get_modifiable(self,
                       date_str: str | None = None,
                       group: str | None = None) -> Config:
        """
        Get a modifiable Config record for a particular date/group. If there
        isn't a record for that date/group yet, it is created by cloning the
        next Config record up the tree.

        :param date_str: The date.
        :param group: The group within that date.
        :return: The (possibly new) Config record.
        """

        # If date and group aren't specified, use the root
        if date is None and group is None:
            return self.modifiable_root

        # Can't have a date without a group
        if date is None:
            raise ValueError(
                f"Can't get config for group '{group}' with no date. You must "
                "specify a date if you specify a group"
            )

        # Get the config for the date (i.e. child node in tree)
        if group is None:
            if (date, None) in self._tree:
                return self._tree[(date_str, None)]
            else:
                # Clone the root for this date
                return self._clone_config(self.modifiable_root,
                                          date_str, None)

        # Get the config for a particular group (i.e. grandchild node in tree)
        if (date, group) in self._tree:
            return self._tree[(date_str, group)]
        elif (date, None) in self._tree:
            # Clone the config for the date down to the group
            # (i.e. clone the child node to the grandchild)
            return self._clone_config(self._tree[(date_str, None)],
                                      date_str, group)
        else:
            # Clone the base config to the date and then to the group
            # (i.e. clone root node down to child and grandchild)
            c = self._clone_config(self.modifiable_root, date_str, None)
            return self._clone_config(c, date_str, group)

    def _clone_config(self,
                      config: Config,
                      date_str: str,
                      group: str | None) -> Config:
        """
        Clone the given config entry, saving it in the tree with the given date
        and group key pair. An unmodifiable view of the new Config record is
        also saved in the view tree.

        This is private as it must never be called if there is already a config
        record at the specified date/group index or (worse) if that record
        has children.

        :param config: The config to clone.
        :param date_str: The date with which to associate with cloned record.
        :param group: The group with which to associate the record (optional).
        :return: The cloned Config record (not the view).
        """

        clone = config.clone()
        self._tree[(date_str, group)] = clone
        self._view_tree[(date_str, group)] = ConfigView(clone)
        return clone

    def update_root(self,
                    project_path: Path | None = None,
                    file: Path | None = None,
                    args: Namespace | None = None) -> bool:
        """
        Update the root config based on a configuration file (and possibly the
        command line arguments).

        If the config file doesn't exist, only the command line arguments are
        applied. If those aren't given either, nothing happens.

        :param project_path: The path to the project directory. This may also
         be derived from the command line arguments (which override this value).
        :param file: The path to the config file, if there is one. Defaults to
         None.
        :param args: The command line arguments, if there are any. Defaults to
         None.
        :return: Whether a config file exists and was applied.
        """

        # Set the project path if given
        if project_path is not None:
            self.modifiable_root.project = project_path

        applied_config_file = False

        # Process the config file only if it exists
        if file is not None and file.exists():
            # Parse YAML
            documents: tuple = _load_config_file(file)

            # Validate each doc with Pydantic
            for doc in documents:
                GlobalConfigModel.model_validate(doc)

            # Apply the documents
            for doc in documents:
                _apply_root_config(doc, file, self.modifiable_root, args)

            applied_config_file = True

        # Apply the command line arguments
        if args is not None:
            _apply_global_cli(args, self.modifiable_root)

        # Return whether a global config file was used
        return applied_config_file

    def load_all_config_files(self,
                              project: Path,
                              args: Namespace | None = None) -> int:
        """
        Load all the sub-config files in the project directory. This is every
        config file except for the global one, which should have already been
        loaded.

        Each config file can overwrite some of the global settings for its
        individual date or group. However, command line arguments always take
        precedence: wherever present, they override all config files.

        :param project: The path to the project directory.
        :param args: The parsed command line arguments, if applicable.
        :return: The number of separate config files that were loaded.
        """

        # Define the logger here, as it can't be used in earlier steps while
        # processing the global config (as the settings there affect the logger)
        log = logging.getLogger(__name__)

        # Count the number of config files
        counter = 0

        # Importing here to avoid circular import ¯\_(ツ)_/¯
        from tlmerge.scan import iter_all_dates, iter_all_groups

        # Scan each date directory
        for date_dir in iter_all_dates():
            found_any_files = False
            file, n = _find_and_apply_config_file(
                date_dir, self.modifiable_root, date_dir.name
            )
            if n > 0:
                log.debug(
                    f"Loaded config \".{os.sep}{file.relative_to(project)}\" "
                    f"with {n} YAML document{'' if n == 1 else 's'}"
                )
                counter += n
                found_any_files = True

            # Get the config instance for this date
            cfg = CONFIG.get_modifiable(date_dir.name)

            # Scan each group directory within this date
            for group_dir in iter_all_groups(date_dir):
                file, n = _find_and_apply_config_file(
                    group_dir, cfg, date_dir.name, group_dir.name
                )
                if n > 0:
                    log.debug(f"Loaded config "
                              f"\".{os.sep}{file.relative_to(project)}\" "
                              f"with {n} YAML document{'' if n == 1 else 's'}")
                    counter += n
                    found_any_files = True

            # If any Config records were updated, apply the command line args
            if found_any_files and args is not None:
                _apply_date_cli(args, cfg)

        # Return the total config file count
        return counter


CONFIG: ConfigManager = ConfigManager()


def _load_config_file(file: Path) -> tuple:
    """
    Given the path to a YAML-formatted config file, load it as a tuple of one
    or more YAML documents. This handles and re-raises exceptions with an
    appropriate error message.

    :param file: The path to the config file.
    :return: A tuple with one or more YAML documents.
    """

    try:
        return tuple(_yaml.load_all(file))
    except Exception as e:
        raise ValueError(f'Invalid/unparseable config file "{file}": '
                         f'{e.__class__.__name__}: {e}')


def _find_and_apply_config_file(directory: Path,
                                parent_config: Config | ConfigView,
                                date_str: str,
                                group_str: str | None = None) -> \
        tuple[Path | None, int]:
    """
    Given a directory, check to see if it contains a config file. If it does,
    parse that file, and update the appropriate Config record.

    :param directory: The directory to check.
    :param parent_config: The parent Config record.
    :param date_str: The date string, for obtaining the new Config record.
    :param group_str: The group string (if applicable). Defaults to None.
    :return: The path to the config file that was checked and the number of
     parsed documents. If the number of documents is 0, the file likely does
     not exist.
    """

    i = 0
    file = directory / DEFAULT_CONFIG_FILE
    if file.is_file():
        # Parse the file
        documents = _load_config_file(file)

        # Load the documents
        for doc in documents:
            _apply_child_config(doc, parent_config, date_str, group_str)
            i += 1

    return file, i


def _apply_root_config(document,
                       document_path: Path,
                       config: GlobalConfig,
                       args: Namespace | None = None) -> None:
    """
    Apply a single YAML document with configuration information to the given
    Config record.

    :param document: The parsed YAML document to apply.
    :param document_path: The path document file.
    :param config: The Config record to modify.
    :param args: The command line arguments. Defaults to None.
    :return: None
    """

    if hasattr(args, 'date_format'):
        config.date_format = args.date_format
    elif 'date_format' in document:
        # Note that this is ignored if the date_format is specified via the CLI
        config.date_format = document['date_format']

    # Update the date info. It's important that the date format was added first
    # (and possibly overwritten by command line args)
    if 'include_dates' in document:
        config.include_dates = document['include_dates']
    if 'exclude_dates' in document:
        config.exclude_dates = document['exclude_dates']

    # Update the group info
    if 'include_groups' in document:
        config.include_groups = document['include_groups']
    if 'exclude_groups' in document:
        config.exclude_groups = document['exclude_groups']
    if 'group_ordering' in document:
        config.group_ordering = document['group_ordering']

    # Update the camera config
    _apply_camera_config(document, config)

    # Update the logging options
    if 'log' in document:
        if not document['log']:
            config.log = None
        else:
            log_path = Path(document['log'])
            if not log_path.is_absolute():
                log_path = document_path.joinpath(log_path).resolve()
            if log_path.is_dir():
                raise ValueError(f"Invalid log file: \"{log_path}\" is "
                                 f"a directory")
            config.log = log_path

    if 'verbose' in document:
        config.verbose = document['verbose']
    if 'quiet' in document:
        config.quiet = document['quiet']
    if 'silent' in document:
        config.silent = document['silent']

    # Execution
    if 'workers' in document:
        config.workers = document['workers']
    if 'max_processing_errors' in document:
        config.max_processing_errors = document['max_processing_errors']
    if 'sample' in document:
        config.sample = document['sample']

    # Database
    if 'database' in document:
        db_path = Path(document['database'])
        if not db_path.is_absolute():
            db_path = document_path.joinpath(db_path).resolve()
        if db_path.is_dir():
            raise ValueError(f"Invalid database file: \"{db_path}\" "
                             f"is a directory")
        config.database = db_path

    # Apply an overrides for child config records
    if 'overrides' in document:
        for override in document['overrides']:
            _apply_child_config(override, config)

    # Apply the command line arguments. These recursively propagate to
    # children records, so no need to separately apply CLI args to the children
    # created from overrides above
    _apply_global_cli(args, config)


def _apply_child_config(document,
                        parent_config: Config | ConfigView,
                        date_str: str | None = None,
                        group_str: str | None = None) -> None:
    """
    Update a config record for a date or group config file. This also applies
    to overrides in a parent config file.

    :param document: The parsed YAML document to apply.
    :param parent_config: The parent Config record, used exclusively for
    getting the date format (if necessary).
    :param date_str: The date (as a string) if known. If this is omitted (e.g.
     for overrides) the 'date' attribute of the document is used.
    :param group_str: The group if known. If omitted, the 'group' attribute of
     the document is used if available.
    :return: None
    """

    # Get the appropriate config record, creating one if it doesn't exist
    d = None
    config: Config
    if date_str is not None:
        if group_str is not None:
            config = CONFIG.get_modifiable(date_str, group_str)
        else:
            config = CONFIG.get_modifiable(date_str,
                                           document.get('group', None))
    elif 'date' in document:
        d = document['date']
        if isinstance(d, date):
            d = d.strftime(parent_config.date_format)
        config: Config = CONFIG.get_modifiable(d, document.get('group', None))
    else:
        raise ValueError(f"Cannot apply document to child config: date_str "
                         f"is None and document is missing 'date' attribute.")

    # Update the group info. If this is a group override, this'll simply be
    # skipped as these options only exist for date overrides
    if 'include_groups' in document:
        config.include_groups = document['include_groups']
    if 'exclude_groups' in document:
        config.exclude_groups = document['exclude_groups']
    if 'group_ordering' in document:
        config.group_ordering = document['group_ordering']

    # Update the camera config
    _apply_camera_config(document, config)

    # Add any group overrides. This can only happen if this override is for a
    # date, in which case `d` was set to document['date']
    if 'overrides' in document:
        assert d is not None
        for override in document['overrides']:
            _apply_child_config(override, parent_config, date_str=d)


def _apply_camera_config(document, config: Config) -> None:
    """
    Apply the basic camera configurations from a document to the given Config
    record.

    :param document: The parsed YAML document to apply.
    :param config: The Config record to modify.
    :return: None
    """

    if 'white_balance' in document:
        config.white_balance = document['white_balance']
    if 'chromatic_aberration' in document:
        config.chromatic_aberration = document['chromatic_aberration']
    if 'median_filter' in document:
        config.median_filter = document['median_filter']
    if 'dark_frame' in document:
        config.dark_frame = document['dark_frame']


def _apply_base_cli(args: Namespace, config: Config) -> None:
    """
    Apply the basic command line arguments to the given Config record. This
    covers all the arguments for a group-level record.

    :param args: The command line arguments.
    :param config: The Config record to modify.
    :return: None
    """

    # Camera settings
    if hasattr(args, 'white_balance'):
        config.white_balance = {
            'red': args.white_balance[0],
            'green_1': args.white_balance[1],
            'blue': args.white_balance[2],
            'green_2': args.white_balance[3]
        }
    if hasattr(args, 'chromatic_aberration'):
        config.chromatic_aberration = {
            'red': args.chromatic_aberration[0],
            'blue': args.chromatic_aberration[1]
        }
    if hasattr(args, 'median_filter'):
        config.median_filter = args.median_filter
    if hasattr(args, 'dark_frame'):
        config.dark_frame = args.dark_frame


def _apply_date_cli(args: Namespace, config: Config) -> None:
    """
    Apply the date-level command line arguments pertaining to the given Config
    record. This also calls _apply_base_cli() for group-level arguments.

    :param args: The command line arguments.
    :param config: The Config record to modify.
    :return: None
    """

    _apply_base_cli(args, config)

    # Groups
    if hasattr(args, 'group_ordering'):
        config.group_ordering = args.group_ordering
    if hasattr(args, 'include_groups'):
        config.include_groups = args.include_groups
    if hasattr(args, 'exclude_groups'):
        config.exclude_groups = args.exclude_groups


def _apply_global_cli(args: Namespace, config: GlobalConfig) -> None:
    """
    Apply all the command line arguments to the given global config record
    (and its children, as that propagates automatically). This calls
    _apply_date_cli() for date- and group-level arguments.

    :param args: The parsed command line arguments.
    :param config: The Config record to modify.
    :return: None
    """

    _apply_date_cli(args, config)

    # Project path
    if hasattr(args, 'project'):
        config.project = args.project

    # Dates
    if hasattr(args, 'date_format'):
        config.date_format = args.date_format
    if hasattr(args, 'include_dates'):
        config.include_dates = args.include_dates
    if hasattr(args, 'exclude_dates'):
        config.exclude_dates = args.exclude_dates

    # Logging
    if hasattr(args, 'log'):
        config.log = args.log
    if hasattr(args, 'verbose'):
        config.verbose = args.verbose
    if hasattr(args, 'quiet'):
        config.quiet = args.quiet
    if hasattr(args, 'silent'):
        config.silent = args.silent

    # Execution
    if hasattr(args, 'workers'):
        config.workers = args.workers
    if hasattr(args, 'max_processing_errors'):
        config.max_processing_errors = args.max_processing_errors
    if hasattr(args, 'sample'):
        config.sample = args.sample

    # Database
    if hasattr(args, 'database'):
        config.database = args.database


def write_default_config(file: Path) -> None:
    """
    Save the default (global) config settings to the given file.

    :param file: The output file path.
    :return: None
    """

    _yaml.dump({
        'log': 'tlmerge.log',
        'verbose': False,
        'quiet': False,
        'silent': False,
        'workers': 20,
        'max_processing_errors': 5,
        'sample': None,
        'database': DEFAULT_DATABASE_FILE,
        'include_dates': [],
        'exclude_dates': [],
        'include_groups': [],
        'exclude_groups': [],
        'group_ordering': 'abc',
        'date_format': 'yyyy-mm-dd',
        'white_balance': {
            'red': 1.0,
            'green_1': 1.0,
            'blue': 1.0,
            'green_2': 1.0
        },
        'chromatic_aberration': {
            'red': 1.0,
            'blue': 1.0
        },
        'median_filter': 1,
        'dark_frame': None,
        'exclude_photos': [],
        'overrides': []
    }, file)
