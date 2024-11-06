from argparse import Namespace
from datetime import date
from pathlib import Path
from typing import Optional

from ruamel.yaml import YAML

from .config import Config, ConfigView, GlobalConfig, GlobalConfigView
from .validation import GlobalConfigModel
from . import DEFAULT_DATABASE_FILE

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

        # If not None, the key must be a tuple with one or two elements, both
        # either string or None
        if not isinstance(key, tuple) or len(key) > 2 or \
                any(k is not None and not isinstance(k, str) for k in key):
            raise KeyError('Expected (date, group) indices when getting '
                           f'config record: got "{key}"')

        # Separate into date and group keys
        dt, grp = key

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

    def update_root(self, file: Path, args: Namespace | None = None) -> None:
        """
        Update the root config based on a configuration file (and possibly the
        command line arguments).

        If the config file doesn't exist, only the command line arguments are
        applied. If those aren't given either, nothing happens.

        :param file: The path to the config file.
        :param args: The command line arguments. Defaults to None.
        :return: None
        """

        # Process the config file only if it exists
        if file.exists():
            # Load
            documents: tuple = _load_config_file(file)

            # Validate each doc with Pydantic
            for doc in documents:
                GlobalConfigModel.model_validate(doc)

            # Apply the documents
            for doc in documents:
                _apply_root_config(doc, file, self.modifiable_root, args)

        # Apply the command line arguments
        if args is not None:
            _apply_cli(args, self.modifiable_root)


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
    except KeyboardInterrupt:
        raise
    except Exception as e:
        raise ValueError(f'Invalid/unparseable config file "{file}": '
                         f'{e.__class__.__name__}: {e}')


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

    # Apply the command line arguments
    _apply_cli(args, config)


def _apply_child_config(document,
                        parent_config: Config,
                        parent_date: str | None = None) -> None:
    """
    Update a config record for a date or group config file. This also applies
    to overrides in a parent config file.

    :param document: The parsed YAML document to apply.
    :param parent_config: The parent Config record.
    :param parent_date: The parent date (as a string) if the parent is a date
     config file.
    :return: None
    """

    # Get the appropriate config record, creating one if it doesn't exist
    d = None
    if 'date' in document:
        d = document['date']
        if isinstance(d, date):
            d = d.strftime(parent_config.date_format)
        config: Config = CONFIG.get_modifiable(d, document.get('group', None))
    else:
        config: Config = CONFIG.get_modifiable(parent_date, document['group'])

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
            _apply_child_config(override, parent_config, parent_date=d)


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


def _apply_cli(args: Namespace, config: GlobalConfig) -> None:
    """
    Apply the command line arguments to the given config record (and its
    children, as changes to config records propagate automatically).

    :param args: The parsed command line arguments.
    :param config: The config record to modify.
    :return: None
    """

    # Dates
    if hasattr(args, 'date_format'):
        config.date_format = args.date_format
    if hasattr(args, 'include_dates'):
        config.include_dates = args.include_dates
    if hasattr(args, 'exclude_dates'):
        config.include_dates = args.include_dates

    # Groups
    if hasattr(args, 'group_ordering'):
        config.group_ordering = args.group_ordering
    if hasattr(args, 'include_groups'):
        config.include_groups = args.include_groups
    if hasattr(args, 'exclude_groups'):
        config.exclude_groups = args.exclude_groups

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

    # Logging
    if hasattr(args, 'log'):
        config.log = args.log
    if hasattr(args, 'verbose'):
        config.verbose = args.verbose
    if hasattr(args, 'quiet'):
        config.quiet = args.quiet
    if hasattr(args, 'silent'):
        config.silent = args.silent

    # Database
    if hasattr(args, 'database'):
        config.database = args.database


def write_default_config(file: Path):
    _yaml.dump({
        'log': 'tlmerge.log',
        'verbose': False,
        'quiet': False,
        'silent': False,
        'database': DEFAULT_DATABASE_FILE,
        'include_dates': [],
        'exclude_dates': [],
        'include_groups': [],
        'exclude_groups': [],
        'group_ordering': 'abc',
        'group_date_format': 'yyyy-mm-dd',
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
