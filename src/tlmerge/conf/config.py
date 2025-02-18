from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from copy import copy
import csv
import inspect
from io import StringIO
import logging
import os
from pathlib import Path
import re
from typing import Annotated, Any, Literal, Self

from pydantic import (AfterValidator, BeforeValidator, ConfigDict,
                      Field, validate_call)

from .const import ENV_VAR_PREFIX, DEFAULT_DATABASE_FILE, DEFAULT_LOG_FILE
from .log import LogLevel
from .config_structs import (ChromaticAberrationModel, FlipRotate,
                             ThumbLocation, WhiteBalanceModel, WhiteBalanceType)

_log = logging.getLogger(__name__)

# This is the main config used for validating most types. It ignores excess
# whitespace in string values, but that's it. Strings are still case-sensitive.
MAIN_PYDANTIC_CONFIG = ConfigDict(str_strip_whitespace=True)

# This config is used for things like the group ordering policy, where we're
# expecting one of Literal set of lowercase strings. It makes sense to be
# case-insensitive and ignore whitespace. This should NOT be used for something
# like a file name, where it is case-sensitive.
STRING_PYDANTIC_CONFIG = ConfigDict(str_to_lower=True,
                                    str_strip_whitespace=True)


def coerce_float_tuple(raw: Any) -> Any:
    """
    Test whether the given raw data is a string. If it is, attempt to coerce it
    into a tuple of floats. They can be separated by commas, semicolons,
    and/or spaces. If this isn't possible, or if the raw data is not a string,
    it's returned unmodified.

    :param raw: The raw data.
    :return: Either the parsed tuple of floats or the unmodified raw data.
    """

    if not isinstance(raw, str):
        return raw  # nope, not a string

    # Split by (possibly consecutive) commas, spaces, and/or semicolons
    parts = re.split(r"[,\s;]+", raw.strip())

    try:
        # Parse as floats
        return tuple(float(p) for p in parts)
    except ValueError:
        return raw  # nope, these aren't floats


def coerce_csv_list(raw: Any) -> Any:
    """
    Test whether the given raw data is a single string. If it is, attempt to
    parse it as a csv into a list of substrings. If not, return the raw data
    unmodified.

    Parsing is done using the builtin `csv` module, with a comma as the
    delimiter and a backslash as the escape character. Each substring is
    stripped of leading and trailing whitespace.

    If given a single empty *or blank* string, this returns an empty list.

    :param raw: The raw data.
    :return: Either a list of parsed substrings or the unmodified raw data.
    """

    if not isinstance(raw, str):
        return raw  # nope, not a string

    # Treat an empty/blank string as an empty list
    if not raw.strip():
        return []

    # Parse as csv-encoded string
    return [item.strip() for item in
            next(csv.reader(StringIO(raw), escapechar='\\'))]


def process_date_format(format_str: str) -> str:
    """
    Given some date format string, update it to ensure that it will be
    recognized by datetime.strptime(). For example, this converts
    "yyyy-mm-dd" to "%Y-%m-%d" and returns "%y/%m/%d" unchanged.

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


def infer_white_balance_green(raw: Any) -> Any:
    """
    This function converts a tuple of three values (representing RGB white
    balance coefficients) to a tuple of four values by duplicating the green
    channel: RGBG.

    If the input isn't a tuple of length 3, this does nothing and returns it
    unmodified.

    This is used by the validator for `set_white_balance()`.

    :param raw: The raw data.
    :return: Either a tuple updated to have four values or the raw input
     unmodified.
    """

    if isinstance(raw, tuple) and len(raw) == 3:
        return *raw, raw[1]

    return raw


def path_validator(name: str,
                   is_file: bool = True,
                   must_exist: bool = False,
                   relative_to: Path = Path(os.getcwd())) -> \
        Callable[[os.PathLike | str | None], Path | None]:
    """
    Get a function that can be used to validate a path. The function accepts
    either a PathLike object or a string, coerces it into a `pathlib.Path`,
    and validates it. It ensures that the path does (or can) point to a file
    or directory, and optionally that it's absolute.

    The validator function raises a `ValueError` if the path is not valid.

    Note that the validator function allows the given path to be None. In that
    case, it is returned unchanged without any validation.

    :param name: What this path represents/points to. This is used
     exclusively for error messages.
    :param is_file: Whether the path must be a file (True) or a directory
     (False). Defaults to True.
    :param must_exist: Whether the path must already exist. Defaults to False.
    :param relative_to: If the given path is not already absolute (i.e.
     relative), it is resolved relative to this path. Defaults to the current
     working directory `Path(os.getcwd())`
    :return: A function that validates a PathLike object or string and returns
     a `pathlib.Path`.
    """

    # Define the validator function
    def validate(path: os.PathLike | str | None) -> Path | None:
        # Skip validation on None
        if path is None:
            return None

        # Convert to pathlib.Path
        if not isinstance(path, Path):
            path = Path(path)

        # Resolve if not absolute
        if not path.is_absolute():
            path = (relative_to / path).resolve()

        # If it's a file but should be a directory or a directory but should
        # be a file, raise an error
        if is_file and path.is_dir():
            raise ValueError(
                f"The {name} path must be a file, but \"{path}\" is a directory"
            )

        if not is_file and path.is_file():
            raise ValueError(
                f"The {name} path must be a directory, but \"{path}\" is a file"
            )

        # If it doesn't need to exist, this is enough validation
        if not must_exist:
            return path

        # Make sure it exists
        if not path.exists():
            raise ValueError(f"The {name} path \"{path}\" does not exist")

        # If it must be a file, make sure it's indeed a file
        if is_file and not path.is_file():
            raise ValueError(f"The {name} path \"{path}\" isn't a file")

        # Same for a directory
        if not is_file and not path.is_dir():
            raise ValueError(f"The {name} path \"{path}\" isn't a directory")

        # Validation passed
        return path

    # Return the validator function
    return validate


def coerce_none(v: Any) -> Any:
    """
    Given any value, if it's the boolean False or some falsy string, return
    None. Otherwise, return the value unchanged (which may still be None).

    This recognizes the following falsy, case-insensitive strings, ignoring
    whitespace: `''` (empty string), `'o'`, `'na'`, `'no'`, `'n/a'`, `'off'`,
    `'false'`, and `'disable'`.

    :param v: The input value.
    :return: Either None or the input unmodified.
    """

    if v is False or (isinstance(v, str) and v.lower().strip() in
                      ('', '0', 'na', 'no', 'n/a', 'off', 'false', 'disable')):
        return None
    else:
        return v


def blank_str_none(v: Any) -> Any:
    """
    Given some value, if it's a blank string (i.e. empty or only whitespace),
    return None. Otherwise, return it unmodified.

    :param v: The value, which may be anything.
    :return: The input value unmodified, unless it's a blank string, in which
     case None.
    """

    if isinstance(v, str) and len(v.strip()) == 0:
        return None

    return v


def str_lower_trim(v: Any) -> Any:
    """
    Given some value, if it's a string, return it converted to lowercase with
    leading/trailing whitespace trimmed. Otherwise, return the input unmodified.

    :param v: The value, which may be anything.
    :return: The input value unchanged, unless it was a string, in which case
     this will be lowercase and have leading/trailing whitespace trimmed.
    """

    if isinstance(v, str):
        return v.lower().strip()
    else:
        return v


def coerce_int(v: Any) -> Any:
    """
    Attempt to convert the given value to an integer. This is done by converting
    it via `float()` if possible and then via `int()` if that wouldn't change
    the value. For example, the string `"102.0"` would be converted to the
    integer `102`, but a datetime object and `"0.01"` would be left unchanged.

    :param v: The value, which may be anything.
    :return: The input value as an int if possible, otherwise unchanged.
    """

    try:
        f = float(v)
        i = int(f)
        if f == i:
            return i
    except ValueError:
        return v


# noinspection PyAttributeOutsideInit
class BaseConfig(ABC):
    """
    These parameters can be set and overridden at every level of the config
    hierarchy: globally, per-date, and per-group.
    """

    def __init__(self, *_, **kwargs) -> None:
        """
        Initialize a configuration instance derived from BaseConfig.

        :param kwargs: Any parameters you want to set right away instead of
         calling setters. For example, pass `median_filter=2` as a proxy for
         `set_median_filter(2)`. Omit parameters to use their default values.
        :return: None
        """

        # The list of config records that derive from this one and inherit their
        # configuration accordingly
        self._children: list[BaseConfig] = []

        # Declare config values
        self._white_balance: WhiteBalanceType
        self._chromatic_aberration: tuple[float, float]
        self._median_filter: int
        self._dark_frame: str | None
        self._flip_rotate: FlipRotate
        self._exclude_photos: set[str]
        self._include_photos: set[str]

        # Thumbnail extraction
        self._thumbnail_location: ThumbLocation
        self._thumbnail_path: Path
        self._use_embedded_thumbnail: bool
        self._thumbnail_resize_factor: float
        self._thumbnail_quality: int

        # Initialize config values
        for attr in ('white_balance', 'chromatic_aberration', 'median_filter',
                     'dark_frame', 'flip_rotate', 'exclude_photos',
                     'include_photos', 'thumbnail_location', 'thumbnail_path',
                     'use_embedded_thumbnail', 'thumbnail_resize_factor',
                     'thumbnail_quality'):
            self._init_value(attr, kwargs)

    def _init_value(self,
                    name: str,
                    kwargs: dict[str, Any],
                    use_default: bool = True) -> None:
        """
        Initialize the specified configuration option in this config record.

        :param name: The name of the configuration option, all lowercase,
         just like the getter function.
        :param kwargs: Keyword arguments (if any) that were passed to the
         config constructor to specify a particular value.
        :param use_default: Whether to call the setter with no parameters (thus
         using its default value) if no other value can be found for the
         option. If False and no value is found, this raises an error. Defaults
         to True.
        :return: None
        :raises ValueError: If no value was given in `kwargs`, the associated
         environment variable is not set, and `use_default` is False.
        :raises ValidationError: If a value is found but fails the Pydantic
         validation on the config option's setter function.
        """

        setter = getattr(self, 'set_' + name)

        # Check for manually specified value in kwargs
        if name in kwargs:
            setter(kwargs[name])
            return

        # Check for environment variable
        v = os.getenv(ENV_VAR_PREFIX + '_' + name.upper())
        if v is not None:
            setter(v)
            return

        # Call setter with no params to use default
        if use_default:
            setter()
        else:
            raise ValueError(
                f'The {self.__class__.__name__} "{name}" is required, but no '
                f'value was given, and the environment variable '
                f'{ENV_VAR_PREFIX}_{name.upper()} is not set'
            )

    @abstractmethod
    def _make_child[T: BaseConfig](self, cls: type[T], *args, **kwargs) -> T:
        """
        Make a new config record of the specified type that's a child of this
        one, inheriting this configuration where applicable.

        :param cls: The class to use for the child config record.
        :param args: Arguments to pass to the class constructor for the child.
        :param kwargs: Keyword-argument configurations to pass to the class
         constructor for the child to save time instead of using setters.
        :return: The child config record.
        :raises ValueError: If any of the given keyword arguments aren't
         supported by the child config class.
        """

        # Get the getters for this config (parent) and the child class to
        # determine which configurations they support
        parent_getters = {s[4:] for s in dir(self.__class__)
                          if s.startswith('set_')}
        child_getters = {s[4:] for s in dir(cls) if s.startswith('set_')}

        # Raise an error if extra kwargs were given
        for kwarg in kwargs:
            if kwarg not in child_getters:
                raise ValueError(f'Invalid configuration "{kwarg}" '
                                 f'not supported by {cls.__name__}')

        # Copy over config from parent to child by updating kwargs
        for getter in parent_getters & child_getters:  # set intersection
            kwargs.setdefault(getter, getattr(self, getter)())

        # Initialize child with given params
        child = cls(*args, **kwargs)

        # Add new child config
        self._children.append(child)
        return child

    @abstractmethod
    def trunc_path(self,
                   path: str, *,
                   level: int,
                   file: bool) -> str | None:
        """
        Given some path, truncate it to the relevant information based on the
        scope of this configuration record. For example, if this is a
        group-specific configuration record for "2025-01-01/my_group", then
        the path "2025-01-01/my_group/file.dng" is truncated to "file.dng".

        For the root/global config, this has no effect.

        If the given path is not applicable to this configuration record, then
        this returns None. In the aforementioned group example, the path
        "2025-01-03/foobar" would return None.

        This is mostly intended for internal purposes. To truncate paths
        relative to the project root, see `RootConfig.rel_path()`.

        :param path: The input path to possibly truncate.
        :param level: The level of the target item in the project directory
         structure. Date directories are level 1 (directly in the project
         root), groups are level 2, and photo files are level 3.
        :param file: Whether the path is expected to point to a file (True) or
         a directory (False). If a directory is expected, paths with a file
         extension trigger a warning, and vice versa.
        :return: The possibly truncated path, or None if it is outside the
         scope of this configuration record.
        """

        # This super() implementation in the abstract base config checks
        # whether there's a file extension and possibly logs a warning.
        # It also returns None on empty/blank strings

        if file:
            if not os.path.splitext(path) and os.path.basename(path) != '*':
                _log.warning(
                    f'Expected a file path for {self.__class__.__name__}.'
                    f'{inspect.stack()[3].function}, but "{path}" is missing '
                    f'a file extension. Is it a directory?'
                )
        else:
            if os.path.splitext(path)[1]:
                _log.warning(
                    f'Expected a directory path for {self.__class__.__name__}.'
                    f'{inspect.stack()[3].function}, but "{path}" has a file '
                    f'extension. Is it a file?'
                )

        # Ignore empty/blank strings
        if not path.strip():
            return None

        return path

    def dump(self, **additional_args) -> dict[str, Any]:
        """
        Get all the settings for this config object as a dictionary. This is
        primarily used for dumping configuration to a YAML-based `tlmerge`
        config file.
        :param additional_args: (Optional) additional arguments to include in
        the dump.
        :return: A dictionary mapping all the configurable options to values
         for this config object.
        """

        wb_r, wb_g1, wb_b, wb_g2 = self.white_balance()
        ca_r, ca_b = self.chromatic_aberration()

        return {
            'white_balance': {
                'red': wb_r,
                'green_1': wb_g1,
                'blue': wb_b,
                'green_2': wb_g2
            },
            'chromatic_aberration': {
                'red': ca_r,
                'blue': ca_b
            },
            'median_filter': self.median_filter(),
            'dark_frame': self.dark_frame(),
            'exclude_photos': self.exclude_photos(),
            'include_photos': self.include_photos(),
            **additional_args
        }

    def _trunc_photo(self, path: str) -> str | None:
        """
        This is a wrapper for `trunc_path()` intended for photo files, setting
        `level=3` and `file=True`.

        :param path: The path to possibly truncate.
        :return: The possibly truncated path, or None if it is outside the
         scope of this configuration record.
        """

        return self.trunc_path(path, level=3, file=True)

    # The insane type annotation on the `wb` parameter is designed to give
    # instructions to Pydantic, which validates all the input to this function.
    # It says to do the following:
    #
    # 1. Accept one of the following types:
    #    - A tuple of four non-negative floats (RGBG).
    #    - A tuple of three non-negative floats (RGB). We'll assume that both
    #      greens are the same.
    #    - The strings "auto", "camera", or "default" (case-insensitive and
    #      ignoring leading/trailing whitespace).
    #    - A instance of the WhiteBalanceModel (defined above).
    #    - Anything that can be coerced to a WhiteBalanceModel, like a
    #      dictionary with the keys "r", "g1", "b", and "g2", each mapped to
    #      non-negative floats. (As with the three-float tuple, g2 is optional).
    # 2. But first, if you get a single string, use coerce_float_tuple to try
    #    to parse that out as a tuple of floats.
    # 3. If you get a WhiteBalanceModel, convert that to a tuple of four floats
    #    in RGBG order.
    # 4. If you got the aforementioned tuple of only three floats, convert that
    #    to a tuple of four floats by duplicating green: RGB becomes RGBG
    #
    # The result is that the `wb` parameter is always either a tuple of
    # floats or one of the predefined lowercase strings.
    @validate_call(config=STRING_PYDANTIC_CONFIG)
    def set_white_balance(
            self,
            wb: Annotated[
                tuple[Annotated[float, Field(ge=0)],
                Annotated[float, Field(ge=0)],
                Annotated[float, Field(ge=0)],
                Annotated[float, Field(ge=0)]] |
                tuple[Annotated[float, Field(ge=0)],
                Annotated[float, Field(ge=0)],
                Annotated[float, Field(ge=0)]] |
                Literal['auto', 'camera', 'default'] |
                WhiteBalanceModel,
                BeforeValidator(str_lower_trim),
                BeforeValidator(blank_str_none),
                BeforeValidator(coerce_float_tuple),
                AfterValidator(WhiteBalanceModel.to_tuple),
                AfterValidator(infer_white_balance_green)
            ] = (1, 1, 1, 1), /) -> Self:

        self._white_balance = wb
        for child in self._children:
            # Use raw_function. No-need to revalidate input
            child.set_white_balance.raw_function(child, wb)

        return self

    def white_balance(self) -> WhiteBalanceType:
        return self._white_balance  # noqa

    # The type annotation on the `ca` parameter works much like the more complex
    # one used in set_white_balance(). It gives instructions to Pydantic, which
    # validates all the input to this function. It says to do the following:
    #
    # 1. Accept one of the following types:
    #    - A tuple of two non-negative floats (red and blue).
    #    - A instance of the ChromaticAberrationModel (defined above)
    #    - Anything that can be coerced to a ChromaticAberrationModel, like a
    #      dictionary with the keys "r" and "b", each mapped to non-negative
    #      floats.
    # 2. But first, if you get a single string, use coerce_float_tuple to try
    #    to parse that out as a tuple of floats.
    # 3. If you get a ChromaticAberrationModel, convert that to a tuple of two
    #    floats: red and blue, in that order.
    #
    # The result is that the `ca` parameter is always a tuple of exactly two
    # non-negative floats.
    @validate_call(config=STRING_PYDANTIC_CONFIG)
    def set_chromatic_aberration(
            self,
            ca: Annotated[
                tuple[Annotated[float, Field(ge=0)],
                Annotated[float, Field(ge=0)]] | \
                ChromaticAberrationModel,
                BeforeValidator(str_lower_trim),
                BeforeValidator(blank_str_none),
                BeforeValidator(coerce_float_tuple),
                AfterValidator(ChromaticAberrationModel.to_tuple)
            ] = (1, 1), /) -> Self:

        self._chromatic_aberration = ca
        for child in self._children:
            child.set_chromatic_aberration.raw_function(child, ca)

        return self

    def chromatic_aberration(self) -> tuple[float, float]:
        return self._chromatic_aberration

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_median_filter(
            self,
            mf: Annotated[int, Field(ge=0)] = 0, /) -> Self:

        self._median_filter = mf
        for child in self._children:
            child.set_median_filter.raw_function(child, mf)

        return self

    def median_filter(self) -> int:
        return self._median_filter

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_dark_frame(self, df: str | None = None, /) -> Self:
        self._dark_frame = df
        for child in self._children:
            child.set_dark_frame.raw_function(child, df)

        return self

    def dark_frame(self) -> str:
        return self._dark_frame

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_flip_rotate(
            self,
            fr: Annotated[FlipRotate | Literal[90, 180, 270],
            BeforeValidator(str_lower_trim),
            BeforeValidator(blank_str_none),
            BeforeValidator(coerce_int)] = FlipRotate.DEFAULT,
            /) -> Self:

        # Convert degree amounts to rotation
        if fr == 90:
            fr = FlipRotate.ROTATE_CW
        elif fr == 180:
            fr = FlipRotate.HALF_ROTATION
        elif fr == 270:
            fr = FlipRotate.ROTATE_CCW

        self._flip_rotate = fr
        for child in self._children:
            child.set_flip_rotate.raw_function(child, fr)

        return self

    def flip_rotate(self) -> FlipRotate:
        # (PyCharm dumb and thinks _flip_rotate could be 90, 180, or 270)
        return self._flip_rotate  # noqa

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_exclude_photos(self, ep: Annotated[
        Iterable[str],
        BeforeValidator(coerce_csv_list)
    ] = tuple(), /) -> Self:

        ep = {p for path in ep if (p := self._trunc_photo(path)) is not None}

        self._exclude_photos = ep
        for child in self._children:
            child.set_exclude_photos.raw_function(child, ep)

        return self

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def add_exclude_photos(self, ep: Annotated[
        Iterable[str],
        BeforeValidator(coerce_csv_list)
    ], /) -> Self:

        ep = {p for path in ep if (p := self._trunc_photo(path)) is not None}

        self._exclude_photos.update(ep)
        for child in self._children:
            child.add_exclude_photos.raw_function(child, ep)

        return self

    def exclude_photos(self) -> set[str]:
        """
        Get a copy of the set of excluded photos. If called on the root config,
        this is a set of relative paths from the root: "date/group/photo".

        If called on a date-level config, this is a set of relative paths
        from a date: "group/photo".

        And if called from a group-level config, this is simply a set of
        photo files in that group.

        :return: The excluded photos.
        """

        return copy(self._exclude_photos)

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_include_photos(self, ip: Annotated[
        Iterable[str],
        BeforeValidator(coerce_csv_list)
    ] = tuple(), /) -> Self:

        ip = {p for path in ip if (p := self._trunc_photo(path)) is not None}

        self._include_photos = ip
        for child in self._children:
            child.set_include_photos.raw_function(child, ip)

        return self

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def add_include_photos(self, ip: Annotated[
        Iterable[str],
        BeforeValidator(coerce_csv_list)
    ], /) -> Self:

        ip = {p for path in ip if (p := self._trunc_photo(path)) is not None}

        self._include_photos.update(ip)
        for child in self._children:
            child.add_include_photos.raw_function(child, ip)

        return self

    def include_photos(self) -> set[str]:
        """
        Get a copy of the list of included photos. See `exclude_photos()`.

        :return: The included photos.
        """

        return copy(self._include_photos)

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_thumbnail_location(
            self,
            l: Annotated[ThumbLocation | Literal['root'] | Literal['project'] |
                         Literal['date'] | Literal['group'] |
                         Literal['custom'] | Literal['other'],
            BeforeValidator(str_lower_trim),
            BeforeValidator(blank_str_none)] = ThumbLocation.ROOT,
            /) -> Self:

        if isinstance(l, ThumbLocation):
            self._thumbnail_location = l
        elif l == 'project':
            self._thumbnail_location = l = ThumbLocation.ROOT
        elif l == 'other':
            self._thumbnail_location = l = ThumbLocation.CUSTOM
        else:
            self._thumbnail_location = l = ThumbLocation[l.upper()]

        for child in self._children:
            child.set_thumbnail_location.raw_function(child, l)

        return self

    def thumbnail_location(self) -> ThumbLocation:
        return self._thumbnail_location

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_thumbnail_path(
            self,
            p: Annotated[os.PathLike | str,
            BeforeValidator(blank_str_none)] = 'thumb',
            /) -> Self:

        if not isinstance(p, Path):
            p = Path(p)

        # If using a custom path (which must be absolute) and the given path
        # is not absolute, resolve it relative to the current working directory
        if self.thumbnail_location() == ThumbLocation.CUSTOM and \
                not p.is_absolute():
            p = (Path(os.getcwd()) / p).resolve()

        self._thumbnail_path = p

        for child in self._children:
            child.set_thumbnail_location.raw_function(child, p)

        return self

    def thumbnail_path(self) -> Path:
        return self._thumbnail_path

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_use_embedded_thumbnail(
            self,
            l: Annotated[bool | Literal['true'] | Literal['false'],
            BeforeValidator(str_lower_trim)] = True,
            /) -> Self:

        self._use_embedded_thumbnail: bool = l is True or l == 'true'
        for child in self._children:
            child.set_use_embedded_thumbnail.raw_function(child, l)

        return self

    def use_embedded_thumbnail(self) -> bool:
        return self._use_embedded_thumbnail

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_thumbnail_resize_factor(
            self,
            f: Annotated[float, Field(gt=0, le=1)] = 1,
            /) -> Self:

        self._thumbnail_resize_factor: float = f
        for child in self._children:
            child.set_thumbnail_resize_factor.raw_function(child, f)

        return self

    def thumbnail_resize_factor(self) -> float:
        return self._thumbnail_resize_factor

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_thumbnail_quality(
            self,
            q: Annotated[int, Field(ge=0, le=100)] = 75,
            /) -> Self:

        self._thumbnail_quality: int = q
        for child in self._children:
            child.set_thumbnail_quality.raw_function(child, q)

        return self

    def thumbnail_quality(self) -> int:
        return self._thumbnail_quality


# noinspection PyAttributeOutsideInit
class DateRootConfig(BaseConfig, ABC):
    """
    This abstract class is the parent of RootConfig and DateConfig, allowing
    them to share configuration parameters not used by GroupConfig.
    """

    def __init__(self, **kwargs) -> None:
        """
        Initialize a configuration instance derived from DateRootConfig.

        :param kwargs: Any parameters you want to set right away instead of
         calling setters. For example, pass `group_ordering='num'` as a proxy
         for `set_group_ordering('num')`. Omit parameters to use their default
         values.
        :return: None
        """

        super().__init__(**kwargs)

        # Declare config values
        self._group_ordering: Literal['abc', 'num', 'natural']
        self._exclude_groups: set[str]
        self._include_groups: set[str]

        # Initialize config values
        for attr in ('group_ordering', 'exclude_groups', 'include_groups'):
            self._init_value(attr, kwargs)

    def dump(self, **additional_args) -> dict[str, Any]:
        return super().dump(
            group_ordering=self.group_ordering(),
            exclude_groups=self.exclude_groups(),
            include_groups=self.include_groups(),
            **additional_args
        )

    def _trunc_group(self, path: str) -> str | None:
        """
        This is a wrapper for `trunc_path()` intended for group directories,
        setting `level=2` and `file=False`.

        :param path: The path to possibly truncate.
        :return: The possibly truncated path, or None if it is outside the
         scope of this configuration record.
        """

        return self.trunc_path(path, level=2, file=False)

    @validate_call(config=STRING_PYDANTIC_CONFIG)
    def set_group_ordering(
            self,
            gp: Annotated[Literal['abc', 'num', 'natural'],
            BeforeValidator(str_lower_trim),
            BeforeValidator(blank_str_none)] = 'abc',
            /) -> Self:

        self._group_ordering = gp
        for child in self._children:
            if isinstance(child, DateRootConfig):
                child.set_group_ordering.raw_function(child, gp)

        return self

    def group_ordering(self) -> Literal['abc', 'num', 'natural']:
        return self._group_ordering

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_exclude_groups(self, eg: Annotated[
        Iterable[str],
        BeforeValidator(coerce_csv_list)
    ] = tuple(), /) -> Self:

        eg = {p for path in eg if (p := self._trunc_group(path)) is not None}

        self._exclude_groups = eg
        for child in self._children:
            if isinstance(child, DateRootConfig):
                child.set_exclude_groups.raw_function(child, eg)

        return self

    # noinspection DuplicatedCode
    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def add_exclude_groups(self, eg: Annotated[
        Iterable[str],
        BeforeValidator(coerce_csv_list)
    ], /) -> Self:

        eg = {p for path in eg if (p := self._trunc_group(path)) is not None}

        self._exclude_groups.update(eg)
        for child in self._children:
            if isinstance(child, DateRootConfig):
                child.add_exclude_groups.raw_function(child, eg)

        return self

    def exclude_groups(self) -> set[str]:
        """
        Get a copy of the list of excluded groups. If called on the root config,
        this is a list of relative paths from the root: "date/group". If
        called on a date-level config, this is simply a list of group names.

        :return: The excluded groups.
        """

        return copy(self._exclude_groups)

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_include_groups(self, ig: Annotated[
        Iterable[str],
        BeforeValidator(coerce_csv_list)
    ] = tuple(), /) -> Self:

        ig = {p for path in ig if (p := self._trunc_group(path)) is not None}

        self._include_groups = ig
        for child in self._children:
            if isinstance(child, DateRootConfig):
                child.set_include_groups.raw_function(child, ig)

        return self

    # noinspection DuplicatedCode
    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def add_include_groups(self, ig: Annotated[
        Iterable[str],
        BeforeValidator(coerce_csv_list)
    ], /) -> Self:

        ig = {p for path in ig if (p := self._trunc_group(path)) is not None}

        self._include_groups.update(ig)
        for child in self._children:
            if isinstance(child, DateRootConfig):
                child.add_include_groups.raw_function(child, ig)

        return self

    def include_groups(self) -> set[str]:
        """
        Get a copy of the list of included groups. See `exclude_groups()`.

        :return: The included groups.
        """

        return copy(self._include_groups)


# noinspection PyAttributeOutsideInit
class RootConfig(DateRootConfig):
    """
    The root config is the top level of a configuration tree. It contains date
    config children, which themselves contain group configs.
    """

    def __init__(self, project: os.PathLike | str | None, **kwargs) -> None:
        """
        Initialize the root configuration instance in the config tree.

        :param project: The absolute path to the project directory. If this is
         None, the environment variable `TLMERGE_PROJECT` is checked. If that's
         not set, this will raise an error.
        :param kwargs: Any parameters you want to set right away instead of
         calling setters. For example, pass `verbose=True` as a proxy for
         `set_verbose(True)`. Omit parameters to use their default values.
        :return: None
        :raises ValueError: If the project directory is None and not specified
         via the environment variable.
        """

        super().__init__(**kwargs)

        self._project: Path
        self._log: Path | None

        # Must initialize log level because the setters depend on it
        self._log_level: LogLevel = LogLevel.DEFAULT
        self._workers: int
        self._max_processing_errors: int
        self._sample: str | None
        self._database: Path
        self._date_format: str
        self._exclude_dates: set[str]
        self._include_dates: set[str]

        # Initialize the project value with an error if not set
        self._init_value(
            'project',
            {'project': project} if project else {},
            use_default=False
        )

        for attr in ('log', 'verbose', 'quiet', 'silent', 'workers',
                     'max_processing_errors', 'sample', 'database',
                     'date_format', 'exclude_dates', 'include_dates'):
            self._init_value(attr, kwargs)

    def dump(self, **additional_args) -> dict[str, Any]:
        return super().dump(
            database=self.database(),
            log=self.log(),
            verbose=self.verbose(),
            quiet=self.quiet(),
            silent=self.silent(),
            workers=self.workers(),
            max_processing_errors=self.max_processing_errors(),
            sample=self.sample(),
            date_format=self.date_format(),
            exclude_dates=self.exclude_dates(),
            include_dates=self.include_dates(),
            **additional_args
        )

    def _trunc_date(self, path: str) -> str | None:
        """
        This is a wrapper for `trunc_path()` intended for date directories,
        setting `level=1` and `file=False`.

        :param path: The path to possibly truncate.
        :return: The possibly truncated path, or None if it is outside the
         scope of this configuration record.
        """

        return self.trunc_path(path, level=1, file=False)

    def _make_child(self, date_dir: str, **kwargs) -> DateConfig:
        """
        Make a new DateConfig record that's a child of this RootConfig,
        inheriting all applicable configurations.

        :param date_dir: The name of the date directory to which the child
         config record applies. Note that this is the name of the directory,
         not the actual date (i.e. not a `datetime.date`).
        :param kwargs: Keyword-argument configurations to pass to the
         DateConfig constructor to save time instead of using setters.
        :return: The child DateConfig record.
        :raises ValueError: If any of the given keyword arguments aren't
         supported by DateConfig.
        """

        return super()._make_child(DateConfig, date_dir, **kwargs)

    def trunc_path(self, path: str, *, level: int,
                   file: bool) -> str | None:
        # All paths are in scope for the root
        return super().trunc_path(path, level=level, file=file)

    def rel_path(self, path: os.PathLike | str) -> Path:
        """
        Return the given path object relative to the global config project
        directory.

        For example, say the timelapse project directory is at
        "/home/alice/Pictures/timelapse/foobar/". If given the path
        "/home/alice/Pictures/timelapse/foobar/2025-01-01/a/my_pic.dng", then
        this returns "2025-01-01/a/my_pic.dng".

        This is achieved via `pathlib.Path.relative_to()`.

        :param path: The path to apply relative to the project directory.
        :return: The relative path.
        :raise RuntimeError: If the project directory path was never set.
        :raise ValueError: If the given path is not relative to the project
         directory.
        """

        # If only PathLike or a string, convert to a pathlib.Path
        if not isinstance(path, Path):
            path = Path(path)

        return path.relative_to(self.project())

    # This is the only setter without a default value, as the project path is
    # required in the constructor and cannot be removed. Note that the
    # AfterValidator here converts str values to Path
    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_project(
            self,
            p: Annotated[str | os.PathLike,
            BeforeValidator(blank_str_none),
            AfterValidator(path_validator(
                'project', is_file=False, must_exist=True
            ))]
    ) -> Self:

        self._project: Path = p  # noqa
        for child in self._children:
            if isinstance(child, RootConfig):
                child.set_project.raw_function(child, p)

        return self

    def project(self) -> Path:
        return self._project

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_database(self,
                     d: Annotated[os.PathLike | str,
                     BeforeValidator(blank_str_none),
                     AfterValidator(path_validator('database'))
                     ] = DEFAULT_DATABASE_FILE, /) -> Self:
        self._database: Path = d  # noqa
        for child in self._children:
            if isinstance(child, RootConfig):
                child.set_database.raw_function(child, d)

        return self

    def database(self) -> Path:
        return self._database

    # The type annotation here allows set_log() to both specify some log file
    # (thereby enabling logging) or disable file logging altogether. Passing
    # `None` will disable the log file. The `BeforeValidator` `coerce_none` also
    # treats the literal boolean False and a few falsy strings ("false", "N/A",
    # "0", "Off", etc.) as None, thereby also disabling the log. Any other
    # strings or PathLike objects go through the `AfterValidator`, which
    # ensures that they are absolute paths and don't point to a directory.
    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_log(self,
                l: Annotated[os.PathLike | str | None,
                BeforeValidator(coerce_none),
                AfterValidator(path_validator('log'))
                ] = DEFAULT_LOG_FILE, /) -> Self:

        self._log: Path | None = l  # noqa
        for child in self._children:
            if isinstance(child, RootConfig):
                child.set_log.raw_function(child, l)

        return self

    def log(self) -> Path | None:
        return self._log

    @validate_call(config=STRING_PYDANTIC_CONFIG)
    def set_log_level(
            self,
            l: Annotated[LogLevel | Literal['verbose'] | Literal['default'] | \
                         Literal['quiet'] | Literal['silent'] | None,
            BeforeValidator(str_lower_trim),
            BeforeValidator(blank_str_none)] = LogLevel.DEFAULT, /) -> Self:

        if l is None:
            self._log_level = LogLevel.DEFAULT
        elif isinstance(l, LogLevel):
            self._log_level = l
        else:
            self._log_level = LogLevel[l.upper()]

        for child in self._children:
            if isinstance(child, RootConfig):
                child.set_log_level.raw_function(child, self._log_level)

        return self

    def log_level(self) -> LogLevel:
        return self._log_level

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_verbose(self, v: bool = False, /) -> Self:
        if v:
            self._log_level = LogLevel.VERBOSE
        elif self._log_level == LogLevel.VERBOSE:
            self._log_level = LogLevel.DEFAULT

        for child in self._children:
            if isinstance(child, RootConfig):
                child.set_verbose.raw_function(child, v)

        return self

    def verbose(self) -> bool:
        return self._log_level == LogLevel.VERBOSE

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_quiet(self, q: bool = False, /) -> Self:
        if q:
            self._log_level = LogLevel.QUIET
        elif self._log_level == LogLevel.QUIET:
            self._log_level = LogLevel.DEFAULT

        for child in self._children:
            if isinstance(child, RootConfig):
                child.set_quiet.raw_function(child, q)

        return self

    def quiet(self) -> bool:
        return self._log_level == LogLevel.QUIET

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_silent(self, s: bool = False, /) -> Self:
        if s:
            self._log_level = LogLevel.SILENT
        elif self._log_level == LogLevel.SILENT:
            self._log_level = LogLevel.DEFAULT

        for child in self._children:
            if isinstance(child, RootConfig):
                child.set_silent.raw_function(child, s)

        return self

    def silent(self) -> bool:
        return self._log_level == LogLevel.SILENT

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_workers(self, w: Annotated[int, Field(ge=1)] = 20,
                    /) -> Self:

        self._workers = w
        for child in self._children:
            if isinstance(child, RootConfig):
                child.set_workers.raw_function(child, w)

        return self

    def workers(self) -> int:
        return self._workers

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_max_processing_errors(self, mpe: Annotated[int, Field(ge=1)] = 5,
                                  /) -> Self:

        self._max_processing_errors = mpe
        for child in self._children:
            if isinstance(child, RootConfig):
                child.set_max_processing_errors.raw_function(child, mpe)

        return self

    def max_processing_errors(self) -> int:
        return self._max_processing_errors

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_sample(self, s: str | None = None, /) -> Self:

        self._sample = s
        for child in self._children:
            if isinstance(child, RootConfig):
                child.set_sample.raw_function(child, s)

        return self

    def sample(self) -> str | None:
        return self._sample

    def sample_details(self) -> tuple[bool, bool, int]:
        """
        Get information on the sample if enabled. This returns a
        tuple with three values:

        - `bool`: Whether a sample is active.
        - `bool`: Whether the sample is randomized.
        - `int`: The size of the sample / number of photos.

        If the sample is not active, this returns (False, False, -1)

        :return: A tuple with sample details.
        """

        # Check if it's disabled
        s = self.sample()
        if s is None:
            return False, False, -1

        # If there's a tilde prefix, it's in random mode
        is_random = s.startswith('~')

        # Parse the number of the photos (removing the tilde if necessary)
        return True, is_random, int(s[1:] if is_random else s)

    def sample_size(self) -> int:
        """
        Get the sample size. This is `-1` if sampling is disabled.

        :return: The sample size.
        """

        if self._sample is None:
            return -1
        else:
            return int(self._sample.strip('~'))

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_date_format(self, df: Annotated[
        str, AfterValidator(process_date_format)
    ] = '%Y-%m-%d', /) -> Self:

        self._date_format = df
        for child in self._children:
            if isinstance(child, RootConfig):
                child.set_date_format.raw_function(child, df)

        return self

    def date_format(self) -> str:
        return self._date_format

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_exclude_dates(self, ed: Annotated[
        Iterable[str],
        BeforeValidator(coerce_csv_list)
    ] = tuple(), /) -> Self:

        ed = {p for path in ed if (p := self._trunc_date(path)) is not None}

        self._exclude_dates = ed
        for child in self._children:
            if isinstance(child, RootConfig):
                child.set_exclude_dates.raw_function(child, ed)

        return self

    # noinspection DuplicatedCode
    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def add_exclude_dates(self, ed: Annotated[
        Iterable[str],
        BeforeValidator(coerce_csv_list)
    ], /) -> Self:

        ed = {p for path in ed if (p := self._trunc_date(path)) is not None}

        self._exclude_dates.update(ed)
        for child in self._children:
            if isinstance(child, RootConfig):
                child.add_exclude_dates.raw_function(child, ed)

        return self

    def exclude_dates(self) -> set[str]:
        """
        Get a copy of the list of excluded date directory names.

        :return: The excluded dates.
        """

        return copy(self._exclude_dates)

    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def set_include_dates(self, ind: Annotated[
        Iterable[str],
        BeforeValidator(coerce_csv_list)
    ] = tuple(), /) -> Self:

        ind = {p for path in ind if (p := self._trunc_date(path)) is not None}

        self._include_dates = ind
        for child in self._children:
            if isinstance(child, RootConfig):
                child.set_include_dates.raw_function(child, ind)

        return self

    # noinspection DuplicatedCode
    @validate_call(config=MAIN_PYDANTIC_CONFIG)
    def add_include_dates(self, ind: Annotated[
        Iterable[str],
        BeforeValidator(coerce_csv_list)
    ], /) -> Self:

        ind = {p for path in ind if (p := self._trunc_date(path)) is not None}

        self._include_dates.update(ind)
        for child in self._children:
            if isinstance(child, RootConfig):
                child.add_include_dates.raw_function(child, ind)

        return self

    def include_dates(self) -> set[str]:
        """
        Get a copy of the list of included date directory names.

        :return: The included dates.
        """

        return copy(self._include_dates)


class DateConfig(DateRootConfig):
    """
    The date config is the second level of a configuration tree. It specifies
    configuration for a specific date.
    """

    def __init__(self, date_dir: str, **kwargs) -> None:
        """
        Initialize a date configuration instance for a particular date
        directory.

        :param date_dir: The name of the date directory to which this config
         record applies. Note that this is the name of the directory, not the
         actual date (i.e. not a `datetime.date`). Note that this is *not*
         validated in any way.
        :param kwargs: Any parameters you want to set right away instead of
         calling setters. For example, pass `group_ordering='num'` as a proxy
         for `set_group_ordering('num')`. Omit parameters to use their default
         values.
        :return: None
        """

        self._date_dir: str = date_dir
        super().__init__(**kwargs)

    def _make_child(self, group_dir: str, **kwargs) -> GroupConfig:
        """
        Make a new GroupConfig record that's a child of this DateConfig,
        inheriting all applicable configurations.

        :param group_dir: The name of the group directory to which the child
         config record applies. This doesn't include the name of the date
         directory.
        :param kwargs: Keyword-argument configurations to pass to the
         GroupConfig constructor to save time instead of using setters.
        :return: The child GroupConfig record.
        :raises ValueError: If any of the given keyword arguments aren't
         supported by GroupConfig.
        """

        return super()._make_child(GroupConfig, group_dir, **kwargs)

    def trunc_path(self,
                   path: str, *,
                   level: int,
                   file: bool) -> str | None:
        # Split the path into its parts
        parts = Path(path).parts

        # If the number of parts is less than the level, accept the path
        # without truncating. For example, given "a/0000.dng" (path with two
        # levels) for a level 3 photo, there's no date to check or truncate
        if len(parts) < level:
            return super().trunc_path(path, level=level, file=file)

        # Check whether any of the parts match this date name. If so, remove
        # all the preceding parts, put the path back together, and return it
        try:
            index = parts.index(self._date_dir)
            if len(parts) - index != level:
                # Found a match for the date name, but not in the expected spot
                _log.warning(
                    f'{self.__class__.__name__} "{self._date_dir}" given path '
                    f'"{path}" to truncate by "{inspect.stack()[2].function}"; '
                    f'path part at index {index} unexpectedly matches date '
                    f'name. Do you have a group or photo with the same name '
                    f'as this date?'
                )
                return None
            else:
                # Truncate preceding parts, and put path back together
                return super().trunc_path(str(Path(*parts[index + 1:])),
                                          level=level, file=file)
        except ValueError:
            pass

        # The path doesn't pertain to this date
        return None

    def date_dir(self) -> str:
        """
        Get the name of the date directory to which this configuration data
        applies.

        :return: The date directory name.
        """

        return self._date_dir


class GroupConfig(BaseConfig):
    """
    The group config is the third/bottom level of a configuration tree. It
    specifies configuration for the photos inside a specific group in a
    specific date.
    """

    def __init__(self, group_dir: str, **kwargs) -> None:
        """
        Initialize a group configuration instance for a particular group
        directory.

        :param group_dir: The name of the group directory to which this config
         record applies. This doesn't include the name of the date directory.
         Note that this is *not* validated in any way.
        :param kwargs: Any parameters you want to set right away instead of
         calling setters. For example, pass `median_filter=2` as a proxy for
         `set_median_filter(2)`. Omit parameters to use their default values.
        :return: None
        """

        self._group_dir: str = group_dir
        super().__init__(**kwargs)

    def _make_child(self, *_) -> BaseConfig:
        """
        This is not supported for group configs.

        :raises NotImplementedError: This method is intentionally disabled.
        """

        raise NotImplementedError(
            'Cannot create a child config record of a group config.'
        )

    def trunc_path(self,
                   path: str, *,
                   level: int,
                   file: bool) -> str | None:
        # Split the path into its parts
        parts = Path(path).parts

        # If the path doesn't include the group name, there's nothing to check
        # or truncate
        if len(parts) == 1:
            return super().trunc_path(path, level=level, file=file)

        # Check whether any of the parts match this group name. If so, remove
        # all the preceding parts, put the path back together, and return it
        try:
            index = parts.index(self._group_dir)
            if index + 1 == len(parts):
                # Found a match for the last part, which should be a file name
                _log.warning(
                    f'{self.__class__.__name__} "{self._group_dir}" given path '
                    f'"{path}" to truncate by "{inspect.stack()[2].function}"; '
                    f'the last part at index {index} unexpectedly matches '
                    f'group name. Do you have a photo with the same name a '
                    f'this group?'
                )
                return None
            else:
                # Truncate preceding parts, and put path back together
                return super().trunc_path(str(Path(*parts[index + 1:])),
                                          level=level, file=file)
        except ValueError:
            pass

        # The path doesn't pertain to this group
        return None

    def group_dir(self) -> str:
        """
        Get the name of the group directory to which this configuration data
        applies. This does not include the name of the date directory.

        :return: The group directory name.
        """

        return self._group_dir

    def get_full_thumbnail_path(self,
                                project: Path,
                                date_dir: str) -> Path:
        """
        Get the path to the directory in which to put thumbnails for photos
        from this group. The path is not validated and may not exist.

        :param project: The path to the project directory, used to get an
         absolute path unless the thumbnail location is `ThumbLocation.CUSTOM`.
        :param date_dir: The name of the date directory to which this group
         belongs.
        :return: A complete absolute path to the thumbnail directory.
        """

        loc, path = self.thumbnail_location(), self.thumbnail_path()

        if loc == ThumbLocation.ROOT:
            return project / path / date_dir / self.group_dir()
        elif loc == ThumbLocation.DATE:
            return project / date_dir / path / self.group_dir()
        elif loc == ThumbLocation.GROUP:
            return project / date_dir / self.group_dir() / path
        elif loc == ThumbLocation.CUSTOM:
            return path / date_dir / self.group_dir()
        else:
            raise ValueError(f'Unknown thumbnail location value "{loc}"')
