from datetime import date
from pathlib import Path
from typing import Optional, Literal, Self
from typing_extensions import Annotated

from pydantic import (BaseModel, Field, field_validator,
                      model_validator, ConfigDict)


def validate_log_level(verbose: bool | None,
                       quiet: bool | None,
                       silent: bool | None):
    """
    Validate the log level flags to make sure no more than one is enabled at
    the same time.

    :param verbose: The verbose flag.
    :param quiet: The quiet flag.
    :param silent: The silent flag.
    :return: None
    :raise ValueError: If more than one flag is True.
    """

    if (verbose is True) + (quiet is True) + (silent is True) > 1:
        if verbose and quiet and silent:
            flags = "'verbose', 'silent', and 'quiet'"
        elif verbose and quiet:
            flags = "'verbose' and 'quiet'"
        elif verbose and silent:
            flags = "'verbose' and 'silent'"
        else:
            flags = "'quiet' and 'silent'"
        raise ValueError(f"Can't use log flags {flags} at the same time.")


def validate_date(date_str: str | date) -> None:
    """
    Validate a date string. This converts it to a pathlib Path to confirm that
    it is a plausible directory name.

    :param date_str: The date string or date object. If this is a datetime date
      object, it's accepted automatically, as its exact value depends on the
      date format used.
    :return: None
    """

    if isinstance(date_str, date):
        return

    if not date_str or not date_str.strip():
        raise ValueError("Date cannot be empty.")

    parts = len(Path(date_str).parts)
    if parts != 1:
        raise ValueError(f"Invalid date reference: '{date_str}' can't be a "
                         f"directory name; as a path it has {parts} parts")


def validate_group(group: str, date_context: Path | None = None) -> Path:
    """
    Validate a group reference. The provided string should be the name of some
    group directory. If no date context is given, it must be included in the
    group path.

    :param group: A string referencing some group.
    :param date_context: The date containing this group, or None if the group
    includes a reference to a date.
    :return: The fully qualified and validated group Path with the date.
    :raise ValueError: If the group is invalid.
    """

    if not group or not group.strip():
        raise ValueError(
            'Missing/blank group name' +
            (f" for date '{date_context}'" if date_context else '')
        )

    # Convert to a Path
    group_path = Path(group)

    # The path can't have more than 2 parts. And if there's no date context,
    # it must have exactly 2
    n = len(group_path.parts)
    if n > 2 or (not date_context and n < 2):
        raise ValueError(
            f"Invalid group '{group}': the path has "
            f"{'only 1 part' if n == 1 else f'{n} parts'}; "
            f"expected {'1' if date_context else '2'}"
        )

    # If the date context is given and there's two parts, they should match
    if n == 2:
        if date_context and date_context != group_path.parent:
            raise ValueError(f"Invalid group '{group}' doesn't match "
                             f"date context '{date_context}'")
        return group_path
    else:
        return date_context / group_path


class Base(BaseModel):
    model_config = ConfigDict(extra='forbid')


class WhiteBalanceModel(Base):
    red: Annotated[float, Field(ge=0)]
    green_1: Annotated[float, Field(ge=0)]
    blue: Annotated[float, Field(ge=0)]
    green_2: Annotated[float, Field(ge=0)]


class ChromaticAberrationModel(Base):
    red: Annotated[float, Field(ge=0)]
    blue: Annotated[float, Field(ge=0)]


class BaseConfigModel(Base):
    # LibRaw camera settings
    white_balance: WhiteBalanceModel | None = None
    chromatic_aberration: ChromaticAberrationModel | None = None
    median_filter: Annotated[Optional[int], Field(ge=0)] = None
    dark_frame: str | None = None

    # Photo exclusion
    exclude_photos: Optional[list[str]] = None


class GroupOverrideModel(BaseConfigModel):
    date: str | date | None = None
    group: str

    @model_validator(mode='after')
    def check_group(self) -> Self:
        d = self.date
        if d is not None:
            validate_date(d)
            if isinstance(d, date):
                # Exact date format is unknown here, but this'll work for
                # validating the group
                d = Path(d.strftime('%Y%m%d'))
            else:
                d = Path(d)

        validate_group(self.group, d)
        return self


class DateConfigModel(BaseConfigModel):
    include_groups: Optional[list[str]] = None
    exclude_groups: Optional[list[str]] = None
    group_ordering: Optional[Literal['abc', 'natural', 'num']] = None

    # Group overrides
    overrides: Optional[list[GroupOverrideModel]] = None


class DateOverrideModel(DateConfigModel):
    date: str | date

    @model_validator(mode='after')
    def check_date(self) -> Self:
        validate_date(self.date)
        return self


class GlobalConfigModel(BaseConfigModel):
    # Logging
    log: str | None = None
    verbose: bool | None = None
    quiet: bool | None = None
    silent: bool | None = None

    # Execution
    workers: Annotated[Optional[int], Field(ge=1)] = None
    max_processing_errors: Annotated[Optional[int], Field(ge=0)] = None
    sample: str | None = None

    # Database
    database: str | None = None

    # Date/group inclusion and exclusion
    include_dates: Optional[list[str | date]] = None
    exclude_dates: Optional[list[str | date]] = None
    include_groups: Optional[list[str]] = None
    exclude_groups: Optional[list[str]] = None

    # Group and date settings
    group_ordering: Optional[Literal['abc', 'natural']] = None
    group_date_format: str | None = None

    # Overrides
    overrides: Optional[list[DateOverrideModel | GroupOverrideModel]] = None

    @field_validator('sample')  # noqa
    @classmethod
    def check_sample(cls, v: str) -> str:
        # Remove tilde if present
        samp = v.strip()[1:] if v.strip().startswith('~') else v.strip()

        # Validate: attempt to parse as an integer
        try:
            if int(samp) <= 0:
                raise ValueError()
        except ValueError:
            raise ValueError(f'Invalid sample amount "{v}": you must specify '
                             f'a positive integer (with optional ~ prefix '
                             f'for randomization) or leave blank to disable')

        return v

    @field_validator('database')  # noqa
    @classmethod
    def database_not_none(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("You must specify a database file path if you "
                             "include the database config")

        db = Path(v)
        # Can only validate if it's (1) an absolute path or (2) a relative path
        # longer than a file name
        if db.is_absolute() or db.parent != Path('.'):
            if not db.is_absolute():
                db = db.resolve()
                v = str(db)
            if not db.is_dir():
                raise ValueError(f'Invalid database file: "{db}" '
                                 'is a directory')

        return v

    @model_validator(mode='after')
    def check_log(self) -> Self:
        validate_log_level(self.verbose, self.quiet, self.silent)

        # Best effort validation to ensure the log file isn't an existing
        # directory. If it's just a file name, we can't validate it here, since
        # we don't know what the project directory is
        if self.log is not None:
            log = Path(self.log)
            if log.is_absolute() or log.parent != Path('.'):
                if not log.is_absolute():
                    log = log.resolve()
                if log.is_dir():
                    raise ValueError(f"Invalid log file \"{log}\": that's "
                                     f"a directory")

        return self

    @model_validator(mode='after')
    def check_dates_groups(self) -> Self:
        # Dates
        if self.include_dates:
            for d in self.include_dates:
                validate_date(d)
        if self.exclude_dates:
            for d in self.exclude_dates:
                validate_date(d)

        # Groups
        if self.include_groups:
            for group in self.include_groups:
                validate_group(group, None)

        if self.exclude_groups:
            for group in self.exclude_groups:
                validate_group(group, None)

        return self
