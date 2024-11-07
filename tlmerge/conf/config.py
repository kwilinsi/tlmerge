from __future__ import annotations

from datetime import date

from pathlib import Path
from typing import Literal

from .const import DEFAULT_DATABASE_FILE, DEFAULT_LOG_FILE


def coerce_date_format(format_str: str) -> str:
    """
    Given some date format string, coerce it into a format recognized by
    datetime.strptime(). For example, this converts "yyyy-mm-dd" to "%Y-%m-%d"
    and returns "%y/%m/%d" unchanged.

    This replaces character sequences as follows:
    - "yyyy" -> "%Y"
    - "yy" -> "%y"
    - "mm" or "m" -> "%m"
    - "dd" or "d" -> "%d"

    All replacements are case-insensitive. To prevent replacement of these
    character sequences, escape them with a backslash. Multiple characters
    require multiple backslashes. For example, "\\d" leaves a literal "d", and
    "\\y\\y\\y\\y" leaves a literal string "yyyy". Escape a backslash with
    another backslash.

    :param format_str: The initial format string.
    :return: A format string usable by strptime to parse dates.
    """

    if not format_str:
        return format_str

    i, result, length = 0, "", len(format_str)
    while i < length:
        # If this is a backslash, add the next character unaltered, and
        # skip ahead
        if format_str[i] == '\\':
            if i + 1 == length:
                return result + '\\'
            else:
                result += format_str[i + 1]
                i += 2
                continue

        # If this is a percent sign, it can escape y, m, d, or another %
        if format_str[i] == '%':
            if i + 1 == length:
                return result + '%'
            elif format_str[i + 1].lower() in ('y', 'm', 'd', '%'):
                result += '%' + format_str[i + 1]
                i += 2
                continue

        next_chars: str = format_str[i:i + 4].lower()
        if next_chars.startswith('yy'):
            # Determine whether it's a run of 2 or 4 Ys
            if next_chars.startswith('yyyy'):
                result += '%Y'
                i += 4
            else:
                result += '%y'
                i += 2
        elif next_chars[0] == 'm':
            result += '%m'
            # Check for a double m
            i += 2 if next_chars.startswith('mm') else 1
        elif next_chars[0] == 'd':
            result += '%d'
            # Check for a double d
            i += 2 if next_chars.startswith('dd') else 1
        else:
            # Add the next character unaltered
            result += format_str[i]
            i += 1

    return result


class Config:
    def __init__(self,
                 date_format: str = 'yyyy-mm-dd',
                 include_dates: list[str | date] = None,
                 exclude_dates: list[str | date] = None,
                 include_groups: list[Path] = None,
                 exclude_groups: list[Path] = None,
                 group_ordering: Literal['abc', 'natural', 'num'] = 'abc',
                 white_balance: dict[str, float] = None,
                 chromatic_aberration: dict[str, float] = None,
                 median_filter: int = 0,
                 dark_frame: Path | None = None):
        """
        Initialize a configuration object with the default settings.
        """

        # Dates and groups
        self._date_format: str = coerce_date_format(date_format)
        self._include_dates: list[str] = \
            [] if include_dates is None else include_dates
        self._exclude_dates: list[str] = \
            [] if exclude_dates is None else exclude_dates
        self._include_groups: list[Path] = \
            [] if include_groups is None else include_groups
        self._exclude_groups: list[Path] = \
            [] if exclude_groups is None else exclude_groups
        self._group_ordering: Literal['abc', 'natural', 'num'] = group_ordering

        # Camera settings
        self._white_balance: dict[str, float] = \
            {} if white_balance is None else white_balance
        self._chromatic_aberration: dict[str, float] = \
            {} if chromatic_aberration is None else chromatic_aberration
        self._median_filter: int = median_filter
        self._dark_frame: Path | None = dark_frame

        self.children: list[Config] = []

    @property
    def date_format(self) -> str:
        return self._date_format

    @date_format.setter
    def date_format(self, date_format: str) -> None:
        df = coerce_date_format(date_format)
        self._date_format = df
        for child in self.children:
            child.date_format = date_format

    @property
    def include_dates(self) -> list[str]:
        return self._include_dates

    @include_dates.setter
    def include_dates(self, include_dates: list[str | date]) -> None:
        # Process children first so they use their own date_formats, just in
        # case they're somehow different (probably shouldn't be, though)
        for child in self.children:
            child.include_dates = include_dates

        # Convert any date objects to strings
        for i in range(len(include_dates)):
            if isinstance(include_dates[i], date):
                include_dates[i] = include_dates[i].strftime(self.date_format)
        self._include_dates = include_dates

    @property
    def exclude_dates(self) -> list[str]:
        return self._exclude_dates

    @exclude_dates.setter
    def exclude_dates(self, exclude_dates: list[str | date]) -> None:
        # See include_dates setter
        for child in self.children:
            child.exclude_dates = exclude_dates

        for i in range(len(exclude_dates)):
            if isinstance(exclude_dates[i], date):
                exclude_dates[i] = exclude_dates[i].strftime(self.date_format)
        self._exclude_dates = exclude_dates

    @property
    def include_groups(self) -> list[Path]:
        return self._include_groups

    @include_groups.setter
    def include_groups(self, include_groups: list[Path]) -> None:
        self._include_groups = include_groups
        for child in self.children:
            child.include_groups = include_groups

    @property
    def exclude_groups(self) -> list[Path]:
        return self._exclude_groups

    @exclude_groups.setter
    def exclude_groups(self, exclude_groups: list[Path]) -> None:
        self._exclude_groups = exclude_groups
        for child in self.children:
            child.exclude_groups = exclude_groups

    @property
    def group_ordering(self) -> Literal['abc', 'natural', 'num']:
        return self._group_ordering

    @group_ordering.setter
    def group_ordering(
            self,
            group_ordering: Literal['abc', 'natural', 'num']) -> None:
        self._group_ordering = group_ordering
        for child in self.children:
            child.group_ordering = group_ordering

    @property
    def white_balance(self) -> dict[str, float]:
        return self._white_balance

    @white_balance.setter
    def white_balance(self, white_balance: dict[str, float]) -> None:
        self._white_balance = white_balance
        for child in self.children:
            child.white_balance = white_balance

    @property
    def chromatic_aberration(self) -> dict[str, float]:
        return self._chromatic_aberration

    @chromatic_aberration.setter
    def chromatic_aberration(self,
                             chromatic_aberration: dict[str, float]) -> None:
        self._chromatic_aberration = chromatic_aberration
        for child in self.children:
            child.chromatic_aberration = chromatic_aberration

    @property
    def median_filter(self) -> int:
        return self._median_filter

    @median_filter.setter
    def median_filter(self, median_filter: int) -> None:
        self._median_filter = median_filter
        for child in self.children:
            child.median_filter = median_filter

    @property
    def dark_frame(self) -> Path | None:
        return self._dark_frame

    @dark_frame.setter
    def dark_frame(self, dark_frame: Path | None) -> None:
        self._dark_frame = dark_frame
        for child in self.children:
            child.dark_frame = dark_frame

    def clone(self) -> Config:
        c = Config(
            self.date_format,
            self.include_dates,
            self.exclude_dates,
            self.include_groups,
            self.exclude_groups,
            self.group_ordering,
            self.white_balance,
            self.chromatic_aberration,
            self.median_filter,
            self.dark_frame
        )

        self.children.append(c)
        return c


class ConfigView:
    """
    This is a read-only view of a Config record. Attempting to modify its
    settings will raise an AttributeError.
    """

    def __init__(self, config: Config) -> None:
        """
        Initialize a view wrapping the given Config record.

        :param config: The Config record to wrap.
        """

        self._config = config

    @property
    def date_format(self) -> str:
        return self._config.date_format

    @property
    def include_dates(self) -> list[str]:
        return self._config.include_dates

    @property
    def exclude_dates(self) -> list[str]:
        return self._config.exclude_dates

    @property
    def include_groups(self) -> list[Path]:
        return self._config.include_groups

    @property
    def exclude_groups(self) -> list[Path]:
        return self._config.exclude_groups

    @property
    def group_ordering(self) -> Literal['abc', 'natural', 'num']:
        return self._config.group_ordering

    @property
    def white_balance(self) -> dict[str, float]:
        return self._config.white_balance

    @property
    def chromatic_aberration(self) -> dict[str, float]:
        return self._config.chromatic_aberration

    @property
    def median_filter(self) -> int:
        return self._config.median_filter

    @property
    def dark_frame(self) -> Path | None:
        return self._config.dark_frame


class GlobalConfig(Config):
    def __init__(self,
                 log: Path | None = DEFAULT_LOG_FILE,
                 verbose: bool = False,
                 quiet: bool = False,
                 silent: bool = False,
                 database: Path = DEFAULT_DATABASE_FILE,
                 date_format: str = 'yyyy-mm-dd',
                 include_dates: list[str | date] = None,
                 exclude_dates: list[str | date] = None,
                 include_groups: list[Path] = None,
                 exclude_groups: list[Path] = None,
                 group_ordering: Literal['abc', 'natural', 'num'] = 'abc',
                 white_balance: dict[str, float] = None,
                 chromatic_aberration: dict[str, float] = None,
                 median_filter: int = 0,
                 dark_frame: Path | None = None):
        """
        Initialize a configuration object with the default settings.
        """

        super().__init__(
            date_format,
            include_dates,
            exclude_dates,
            include_groups,
            exclude_groups,
            group_ordering,
            white_balance,
            chromatic_aberration,
            median_filter,
            dark_frame
        )

        # Log settings
        self._log: Path | None = log
        self._verbose: bool = verbose
        self._quiet: bool = quiet
        self._silent: bool = silent

        # Database file
        self._database: Path = database

    @property
    def log(self) -> Path | None:
        return self._log

    @log.setter
    def log(self, log: Path | None) -> None:
        self._log = log

    @property
    def database(self) -> Path | None:
        return self._database

    @database.setter
    def database(self, database: Path | None) -> None:
        self._database = database

    @property
    def verbose(self) -> bool:
        return self._verbose

    @verbose.setter
    def verbose(self, verbose: bool) -> None:
        self._verbose = verbose

        # If true, this has the side effect of making quiet and silent False
        if verbose:
            self.quiet = False
            self.silent = False

    @property
    def quiet(self) -> bool:
        return self._quiet

    @quiet.setter
    def quiet(self, quiet: bool) -> None:
        self._quiet = quiet

        # If true, this has the side effect of making verbose and silent False
        if quiet:
            self.verbose = False
            self.silent = False

    @property
    def silent(self) -> bool:
        return self._silent

    @silent.setter
    def silent(self, silent: bool) -> None:
        self._silent = silent

        # If true, this has the side effect of making verbose and quiet False
        if silent:
            self.verbose = False
            self.quiet = False

    def log_level(self) -> Literal['verbose', 'quiet', 'silent'] | None:
        """
        Get a string with the selected log mode: verbose, quiet, or silent. If
        all the log flags are False, this returns None (indicating the normal
        log level, INFO).

        :return: The log level.
        """

        if self.verbose:
            return 'verbose'
        elif self.quiet:
            return 'quiet'
        elif self.silent:
            return 'silent'
        else:
            return None


class GlobalConfigView(ConfigView):
    """
    This is a read-only view of a GlobalConfig record. Attempting to modify its
    settings will raise an AttributeError.
    """

    def __init__(self, config: GlobalConfig):
        super().__init__(config)

    @property
    def log(self) -> Path | None:
        return self._config.log  # noqa

    @property
    def database(self) -> Path | None:
        return self._config.database  # noqa

    @property
    def verbose(self) -> bool:
        return self._config.verbose  # noqa

    @property
    def quiet(self) -> bool:
        return self._config.quiet  # noqa

    @property
    def silent(self) -> bool:
        return self._config.silent  # noqa

    def log_level(self) -> Literal['verbose', 'quiet', 'silent'] | None:
        return self._config.log_level()  # noqa
