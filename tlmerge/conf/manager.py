from argparse import Namespace
from datetime import date, datetime
import logging
from logging import Logger
import os
from pathlib import Path
from typing import Any, Optional

from ruamel.yaml import YAML

from .config import DateConfig, GroupConfig, RootConfig
from .const import DEFAULT_CONFIG_FILE

# Define the YAML reader for parsing config files
_yaml = YAML()
_yaml.sequence_indent = 4
_yaml.sequence_dash_offset = 2


# noinspection PyProtectedMember
class ConfigManager:
    """
    The ConfigManager is responsible for managing the configuration tree and
    serving the appropriate config record at each level of the project.

    It keeps track of the central RootConfig as well as its children
    (DateConfigs) and grandchildren (GroupConfigs).
    """

    def __init__(self, project: str | Path | None) -> None:
        """
        Initialize the config manager, which is responsible for managing the
        configuration tree and serving the appropriate config record at each
        level of the project.

        :param project: The absolute path to the project directory. If this is
         None, the environment variable `TLMERGE_PROJECT` is checked. If that's
         not set, this will raise an error.
        :return: None
        :raises ValueError: If `project` is invalid, or it's not set and can't
         be determined from the environment variable.
        """

        # Root node
        self._root: RootConfig = RootConfig(project)

        # Config records in this tree inherit from the view
        self._tree: dict[tuple[str, Optional[str]],
        DateConfig | GroupConfig] = {}

    @property
    def root(self) -> RootConfig:
        return self._root

    def __getitem__(self,
                    key: None | tuple[()] | str | tuple[str | None] | \
                         tuple[str, str | None] | tuple[None, None]) -> \
            RootConfig | DateConfig | GroupConfig:
        """
        Get a config view via indexing by specifying a date and group. If
        both are omitted, this returns the root config. For date configs, the
        group index can be omitted.

        :param key: Zero, one, or two strings. Either the last string or both
         of them can be None. Using an empty tuple, None, or a tuple containing
         only None yields the root config.
        :return: The most specific Config record for this date/group.
        :raises TypeError: If the key type is invalid.
        :raises KeyError: If given an empty string for the date or group name.
        """

        # Use the root config if the index is None, an empty tuple, (None),
        # or (None, None).
        if key is None or (isinstance(key, tuple) and
                           (len(key) == 0 or key == (None,) or
                            key == (None, None))):
            return self.root

        # Validate key type, and separate into date and group
        if isinstance(key, str):
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
            raise TypeError('Expected (date, group) indices when getting '
                            f'config record: got "{key}" ({type(key)})')

        # If either is blank, raise an error
        if (dt is not None and not dt.strip()) or \
                (grp is not None and not grp.strip()):
            raise KeyError('Invalid date/group indices for config record: '
                           f"keys can't be blank, but got \"{key}\"")

        if dt is None:
            raise TypeError('Expected (date, group) indices when getting '
                            f'config record: got group "{grp}" but no date')
        elif grp is None:
            # (PyCharm thinks `dt` is None for some inexplicable reason)
            # noinspection PyTypeChecker
            return self._tree.get((dt, None), self.root)
        else:
            # Get the group config; if that's not found, get the date config;
            # if that's not found, get the root config
            return self._tree.get(
                (dt, grp),
                self._tree.get((dt, None), self.root)
            )

    def new_date(self, date_dir: str, **kwargs) -> DateConfig:
        """
        Create a new DateConfig based on the existing root that's only
        applicable to the specified date directory.

        :param date_dir: The name of the date directory to which the new config
         record will apply. This is validated to make sure it adheres to the
         date format according to the root config.
        :param kwargs: Additional arguments to pass to the DateConfig
         constructor to override particular configurations. This is a shortcut
         to individually calling setters for those values later.
        :return: The new DateConfig record.
        :raises ValueError: If `date_dir` doesn't match the `date_format`
         root configuration.
        """

        # Make sure the date_dir name matches the date format
        try:
            datetime.strptime(date_dir, self._root.date_format())
        except ValueError:
            raise ValueError(
                f"Invalid date directory \"{date_dir}\": format doesn't "
                f"match date_format config \"{self._root.date_format()}\""
            )

        # Create DateConfig child
        cfg = self._root._make_child(date_dir, **kwargs)
        self._tree[(date_dir, None)] = cfg
        return cfg

    def new_group(self,
                  date_dir: str,
                  group_dir: str,
                  **kwargs) -> GroupConfig:
        """
        Create a new DateConfig based on the existing root that's only
        applicable to the specified date directory.

        As a side effect, this also creates (but does not return) a config
        record for the date directory containing this group if it doesn't
        already exist.

        :param date_dir: The name of the date directory containing the group.
        :param group_dir: The name of the group directory to which the new
         config record will apply.
        :param kwargs: Additional arguments to pass to the GroupConfig
         constructor to override particular configurations. This is a shortcut
         to individually calling setters for those values later.
        :return: The new GroupConfig record.
        :raises ValueError: If a new `DateConfig` must be created, and the
          `date_dir` doesn't match the `date_format` root configuration,
          causing `self.new_date()` to raise an error.
        """

        # Create a config for the date parent if it doesn't already exist
        date_cfg: DateConfig | None = self._tree.get((date_dir, None))
        if date_cfg is None:
            date_cfg: DateConfig = self.new_date(date_dir)

        # Create the config for the group
        cfg = date_cfg._make_child(group_dir, **kwargs)
        self._tree[(date_dir, group_dir)] = cfg
        return cfg

    def get(self,
            date_dir: str | None = None,
            group_dir: str | None = None, /) -> \
            RootConfig | DateConfig | GroupConfig:
        """
        Get a config record specific to the given date and group. If such a
        record does not already exist, it is created. In that respect, this is
        different from indexing the `ConfigManager` with `__getitem__`, which
        returns the most specific config for the specified date and group
        without creating any new ones.

        If both the `date_dir` and `group_dir` are `None`, this returns the
        `RootConfig`. If only the `group_dir` is `None`, this returns a
        `DateConfig`. And if neither are `None`, it returns a `GroupConfig`.

        If the `group_dir` is given while `date_dir`, is `None`, this raises
        an error.

        :param date_dir: The name of the date directory. Defaults to None.
        :param group_dir: The name of the group directory. Defaults to None.
        :return: A config record specific to the given date and group.
        :raises ValueError: If `group_dir` is given but not `date_dir`.
        """

        # If the date is omitted, the group must also be omitted
        if date_dir is None:
            if group_dir is None:
                return self._root
            else:
                raise ValueError(
                    "Can't get a config record for a specific group "
                    f"(\"{group_dir}\" without specifying the date"
                )

        # Get the requested DateConfig or GroupConfig.
        # If it doesn't already exist, make it
        config = self._tree.get((date_dir, group_dir))

        if config is not None:
            return config
        elif group_dir is None:
            return self.new_date(date_dir)
        else:
            return self.new_group(date_dir, group_dir)

    def update_root(self, *,
                    file: Path | None = None,
                    cli: Namespace | None = None) -> tuple[bool, bool]:
        """
        Update the root config based on a configuration file (and possibly the
        command line arguments). If neither a file nor command line arguments
        are provided, nothing happens.

        :param file: The path to the root config file, if there is one. If this
         file doesn't exist, it's considered None. Defaults to None.
        :param cli: The command line arguments, if there are any. Defaults to
         None.
        :return: Two booleans indicating respectively (1) whether a config file
         exists and was applied and (2) whether command line arguments were
         given and applied.
        :raises ValueError: If the config file exists but points to a
         directory instead of a file.
        """

        used_file = used_cli = False

        # Process the config file only if it exists
        if file is not None and file.exists():
            if not file.is_file():
                raise ValueError(f'The config file {file} exists but is not '
                                 'a valid file. Is it a directory?')

            # Parse and apply the documents
            for doc in _load_config_file(file):
                self._apply_root_config_document(doc, cli=cli)

            used_file = True

        # Apply the command line arguments if given
        if cli is not None:
            self.apply_cli_args(cli)
            used_cli = True

        # Return what was used
        return used_file, used_cli

    def apply_cli_args(self, args: Namespace) -> None:
        """
        Apply all the command line arguments to the root config record (and its
        children, as that propagates automatically).

        :param args: The parsed command line arguments.
        :return: None
        """

        # Iterate over all the available command line arguments
        for attr in dir(args):
            # Ignore private attributes of the argparse Namespace
            if attr.startswith('_'):
                continue

            if adder := getattr(self._root, 'add_' + attr, None):
                # If there's an adder method, use it
                adder(getattr(args, attr))
            elif setter := getattr(self._root, 'set_' + attr, None):
                # Otherwise, if there's a setter, use it
                setter(getattr(args, attr))

            # Ignore other attributes. Can't differentiate between
            # invalid/errant CLI settings vs attributes of the Namespace that
            # we don't care about. I.e., we can't raise a ValueError on unknown
            # CLI settings without a bunch of false-positives. dir() is a rather
            # clunky way to look for possible settings. Also, settings like
            # `make_config` are intended for use by main() but not here.

    def _apply_root_config_document(self,
                                    document: dict[str, Any],
                                    cli: Namespace | None = None) -> None:
        """
        Apply a single YAML document from a config file to the RootConfig
        record `self._root`.

        :param document: The parsed configuration dictionary to apply. All keys
         are assumed to be lowercase.
        :param cli: The parsed command line arguments, if there are any. This is
         used for configurations like `date_format` that affect the way other
         configurations (e.g. `exclude_dates`) are parsed. Defaults to None.
        :return: None
        :raises ValueError: If the document contains invalid keys or values.
        """

        overrides: list | None = None

        # Apply date format first, as this affects the parsing of other config
        # elements. Prefer the CLI if available
        if cli is not None and hasattr(cli, 'date_format'):
            self._root.set_date_format(cli.date_format)
        elif v := document.get('date_format'):
            self._root.set_date_format(v)

        # Apply all configurations for which there are setters to the root
        for key, value in document.items():
            if key == 'date_format':
                pass  # Already applied
            elif key == 'overrides':
                # Wait to apply any overrides last
                overrides = value if isinstance(value, list) else [value]
            elif setter := getattr(self._root, 'add_' + key, None):
                # If there's an add_... method (e.g. for sets), use it
                setter(value)
            elif setter := getattr(self._root, 'set_' + key, None):
                # If there's a setter, use it
                setter(value)
            else:
                raise ValueError(
                    f'Unknown configuration option "{key}" is '
                    'not supported for the root configuration file'
                )

        # Apply overrides for sub-configs last
        if overrides is not None:
            for o in overrides:
                self._apply_override(o)

    def _apply_date_group_config_document(
            self,
            document: dict[str, Any],
            config: DateConfig | GroupConfig) -> None:
        """
        Apply a single YAML document from a config file to either `DateConfigs`
        or `GroupConfigs`.

        :param document: The parsed configuration dictionary to apply. All keys
         are assumed to be lowercase.
        :param config: The config record to update with the configuration from
         the document.
        :return: None
        :raises ValueError: If the document contains invalid keys or values.
        """

        # Initialize overrides to apply last. This is only used for DateConfigs
        overrides: list | None = None

        # Apply all configurations for which there are setters to the root
        for key, value in document.items():
            if key == 'overrides':
                if isinstance(config, GroupConfig):
                    raise ValueError(
                        'Invalid configuration key "overrides" for a group '
                        'configuration. Overrides are only supported at the '
                        'root and date levels.'
                    )
                else:
                    overrides = value if isinstance(value, list) else [value]
            elif setter := getattr(self._root, 'add_' + key, None):
                # If there's an add_... method (e.g. for sets), use it
                setter(value)
            elif setter := getattr(config, 'set_' + key):
                # If there's a setter, use it
                setter(value)
            else:
                raise ValueError(
                    f'Unknown configuration option "{key}" is '
                    f'not supported for {config.__class__.__name__}'
                )

        # Apply overrides for sub-configs last
        if overrides is not None:
            for o in overrides:
                self._apply_override(o, config.date_dir())

    def _get_override_date_group(self,
                                 document: dict[str, Any],
                                 get_date: bool) -> str | None:
        """
        This is a utility method specify for `ConfigManager._apply_override()`
        for getting the `"date"` or `"group"` attribute from a parsed YAML
        document. It accepts a document (normalized to a dictionary) and
        extracts either the date or group directory name based on the
        `get_date` parameter.

        This has a few possibilities:

        1. The expected key (`"date"` or `"group"`) is not present. This
           returns `None`.
        2. The key maps to `None` or a blank string. This returns None.
        3. The key maps to a non-blank string. This returns that string.
        4. `get_date=True`, and the `"date"` key maps to a `date` or
           `datetime` object. This means that the YAML reader automatically
           detected that a string was a date and parsed it as such. Nice, but
           not really what we want. In this case, turn it back into a string
           using `datetime.strftime()` with the format from
           `self._root.date_format()`.
        5. The key maps to something else unexpected. This raises an error.

        :param document: A dictionary mapping strings to values.
        :param get_date: Whether to get the date (True) or group (False) name.
        :return: The date or group name, or None.
        :raises ValueError: If the date or group value is unexpected.
        """

        value: Any = document.get('date' if get_date else 'group')

        if value is None:
            return None

        # The value should be a string
        if isinstance(value, str):
            # An empty string is the same as None
            if not value.strip():
                return None
            else:
                return value

        # It's not a string. Maybe it's a datetime?
        if get_date and isinstance(value, (date, datetime)):
            return value.strftime(self._root.date_format())

        # Unexpected data format
        raise ValueError(
            f'Invalid override configuration. The '
            f'"{'date' if get_date else 'group'}" must '
            f'be a a string, not a "{value.__class__.__name__}"'
        )

    def _apply_override(self,
                        document: dict[str, Any],
                        date_context: str | None = None) -> None:
        """
        Apply some configuration override. This will likely create a new config
        record for a date or group and apply the configuration there.

        :param document: The parsed configuration dictionary to apply. All keys
         are assumed to be lowercase.
        :param date_context: If this override was in a date config file, this
         is the date as a string, which is necessary context to identify the
         group. On the other hand, if this override was in the root config file,
         this is None.
        :return: None
        :raises ValueError: If the date/group values are missing or incorrect.
        """

        # Get date and group identifiers in the overrides
        date_str: str | None = self._get_override_date_group(document, True)
        group_str: str | None = self._get_override_date_group(document, False)

        # Check the date
        if date_context is None:
            # If no date context is given, the date_str must be specified
            if date_str is None:
                g = '' if group_str is None else f' for group "{group_str}"'
                raise ValueError(
                    f'You must specify a date for the config override{g} '
                    'in the root config file.'
                )
        elif date_str is not None and date_context != date_str:
            # If both date_str and date_context are given, they must match
            raise ValueError(
                f'Unexpected date "{date_str}" specified in a configuration '
                f'override within the "{date_context}" config file.'
            )

        # Check the group
        if group_str is None:
            if date_context is not None or date_str is None:
                # If within a date context, you must specify a group
                raise ValueError(
                    'You must specify a group for the config override in '
                    f'the "{date_context}" config file.'
                )
            elif 'group' in document:
                # This isn't impossible to parse, but it's weird if the user
                # gives a blank "group:" tag in the override
                raise ValueError(
                    'A config override in the root configuration file for '
                    f'"{date_str}" has an unexpected blank "group" tag. '
                    'Did you forget to specify a group?'
                )

        # Get the appropriate config record, creating one if it doesn't exist
        config = self.get(date_str, group_str)

        # Remove keys used for identifying the override
        document.pop('date', None)
        document.pop('group', None)

        # Apply configuration
        self._apply_date_group_config_document(document, config)

    def load_all_config_files(self) -> int:
        """
        Load all the sub-config files in the project directory. This is every
        config file except for the global one, which should have already been
        loaded.

        Each config file can overwrite some of the global settings for its
        individual date or group. However, command line arguments always take
        precedence: wherever present, they override all config files.

        :return: The number of separate config files that were loaded.
        """

        # Define the logger here, as it can't be used in earlier steps while
        # processing the global config (as the settings there affect the logger)
        log: Logger = logging.getLogger(__name__)

        # Count the number of config files
        n = 0

        # Importing here to avoid circular import ¯\_(ツ)_/¯
        from tlmerge.scan import iter_all_dates, iter_all_groups

        # Scan each date directory
        for date_dir in iter_all_dates(self):

            # Check for and apply a config file for the date
            n += self._load_config_file(date_dir, log, True)

            # Scan each group directory within this date
            for group_dir in iter_all_groups(date_dir, self):
                # Check for and apply a config file for the date
                n += self._load_config_file(group_dir, log, False)

        # Return the total config file count
        return n

    def _load_config_file(self,
                          directory: Path,
                          log: Logger,
                          is_date: bool) -> bool:
        """
        Given a directory, check to see if it contains a config file. If it
        does, parse that file, and update the appropriate Config record.

        :param directory: The directory to check.
        :param log: The logging instance, used to issue debug messages if a
         file is found and loaded.
        :param is_date: Whether the given directory points to a date (True)
         or group (False).
        :return: True if and only if a configuration file exists and is loaded.
        """

        file = directory / DEFAULT_CONFIG_FILE

        if not file.is_file():
            return False

        # Get the name of the date (and possibly group) directory
        if is_date:
            date_str, group_str = directory.name, None
        else:
            date_str, group_str = directory.parent.name, directory.name

        # Load, parse, and apply the YAML document(s) in the file
        documents = _load_config_file(file)
        n = len(documents)

        for doc in documents:
            self._apply_date_group_config_document(
                doc, self.get(date_str, group_str)
            )

        log.debug(
            f"Loaded config "
            f"\".{os.sep}{file.relative_to(self._root.project())}\" "
            f"with {n} YAML document{'' if n == 1 else 's'}"
        )

        return True


def _load_config_file(file: Path) -> list[dict[str, Any]]:
    """
    Given the path to a YAML-formatted config file, load it as a tuple of one
    or more YAML documents. This handles and re-raises exceptions with an
    appropriate error message.

    Each document comes in as a CommentedMap and is converted to a dict.
    (A CommentedSeq will trigger an error). All keys are converted to
    lowercase, and duplicate keys trigger an error. The keys of any nested
    constructs are made lowercase and checked as well.

    :param file: The path to the config file.
    :return: A list with one or more parsed YAML documents.
    :raises ValueError: If there's any error loading and parsing the file.
    """

    try:
        docs = list(_yaml.load_all(file))
    except Exception as e:
        raise ValueError(f'Invalid/unparseable config file "{file}": '
                         f'{e.__class__.__name__}: {e}')

    # Make sure there's at least one document
    if len(docs) == 0:
        raise ValueError(f'Invalid config file "{file}": '
                         "Couldn't find any YAML documents. Is it empty?")

    # Normalize documents
    docs: list[dict[str, Any]] = _normalize_yaml_construct(docs)

    # Make sure each document is a dictionary
    for doc in docs:
        if not isinstance(doc, dict):
            raise ValueError(
                f'Got "{doc.__class__.__name__}" from "{file}": expected a '
                'dict. Is your configuration file formatted incorrectly?'
            )

    return docs


def _normalize_yaml_construct(construct: Any) -> Any:
    """
    Given some data from a YAML config file, normalize it. Make all the keys
    in dictionaries/CommentedMaps lowercase, and prevent duplicate keys. This
    also works recursively, normalizing nested constructs in dictionary keys
    or iterables.

    If given any dict-like object, this returns a dict. If given any iterable,
    this returns a list.

    :param construct: The construct to normalize.
    :return: The normalized construct.
    """

    # If it's a dictionary (including YAML CommentedMap), make keys lowercase,
    # and normalize all the values. Also check for duplicate keys
    if isinstance(construct, dict):
        d = {}

        for k, v in construct.items():
            if not isinstance(k, str):
                raise ValueError("Expect string key in YAML map, but got "
                                 f"{type(k)}")
            k = k.lower()
            if k in d:
                raise ValueError(
                    f"Found duplicate key {k} in config file. Remember that "
                    "configuration keys are case-insensitive."
                )
            d[k] = _normalize_yaml_construct(v)

        return d

    # For lists, tuples, and sets, normalize each of their sub-elements, and
    # convert all these collections to lists
    if isinstance(construct, (list, tuple, set)):
        return [_normalize_yaml_construct(e) for e in construct]

    # Otherwise return unchanged
    return construct


def write_default_config(file: Path) -> None:
    """
    Save the default (root/global) config settings to the given file.

    :param file: The output file path.
    :return: None
    """

    _yaml.dump(RootConfig(
        os.getcwd()  # Just a dummy value. Can be anything PathLike
    ).dump(), file)
