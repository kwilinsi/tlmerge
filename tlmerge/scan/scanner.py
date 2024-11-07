from collections.abc import Generator
from datetime import date, datetime
import logging
import os
from pathlib import Path
from typing import Literal

_log = logging.getLogger(__name__)


def iterate_date_dirs(project: Path,
                      date_format: str,
                      order: bool = False) -> Generator[Path, None, None]:
    """
    Iterate over all the directories in the project root that match the date
    format. These are the date directories, which contain groups, which contain
    photos.

    :param project: The root project directory.
    :param date_format: The date format used in the directory names.
    :param order: Whether to iterate over the dates chronologically.
    :return: A generator yielding paths to matching date directories.
    """

    dirs: list[tuple[date, Path]] = []

    # Iterate over everything in the project root
    for directory in project.iterdir():
        # Make sure it's a directory
        if not directory.is_dir():
            continue

        # Ensure it matches the date format
        try:
            dt = datetime.strptime(directory.name, date_format)
        except ValueError:
            continue

        # Either collect the dates (if ordering them) or yield them here
        if order:
            dirs.append((dt, directory))
        else:
            yield directory

    # Sort all the directories chronologically
    if order:
        yield from (d for _, d in sorted(dirs, key=lambda entry: entry[0]))


def iterate_group_dirs(date_dir: Path,
                       group_ordering: Literal['abc', 'natural', 'num'],
                       order: bool = False) -> Generator[Path, None, None]:
    """
    Iterate over all the group directories within a particular date in a
    project.

    :param date_dir: The path to the date directory, which contains zero or
     more groups.
    :param group_ordering: How the groups are ordered. This doesn't control the
     order the groups are yielded by this function (use the `order` parameter
     for that), but 'abc' and 'num' cause non-matching directories to be
     ignored. If using 'natural' ordering, all directories are yielded.
    :param order: Whether to iterate over the groups in order based on the
     group_ordering.
    :return: A generator yielding paths to matching groups.
    """

    # Get every directory
    directories = [d for d in date_dir.iterdir() if d.is_dir()]

    # In 'natural' ordering mode, yield everything
    if group_ordering == 'natural':
        if order:
            yield from sorted(directories, key=lambda d: d.name)
        else:
            yield from directories

        return

    # In 'num' mode, only yield directories that can be parsed as floats
    if group_ordering == 'num':
        dirs: list[tuple[float, Path]] = []
        for directory in directories:
            try:
                # If ordering, collect directories into a list to sort;
                # otherwise yield immediately
                if order:
                    dirs.append((float(directory.name), directory))
                else:
                    float(directory.name)
                    yield directory
            except ValueError:
                pass

        # Sort the list
        if order:
            yield from (d for _, d in sorted(dirs, key=lambda entry: entry[0]))

        return

    # Finally, in 'abc' mode, only include directories that contain exclusively
    # letters (no digits, spaces, punctuation, etc.)
    dirs: list[tuple[str, Path]] = []
    for directory in directories:
        if directory.name.isalpha():
            # If ordering, collect into a list to sort; otherwise just yield
            if order:
                dirs.append((directory.name, directory))
            else:
                yield directory

    # Sort the list, first by length and then alphabetically ignoring case
    if order:
        yield from (d for _, d in sorted(dirs,
                                         key=lambda entry:
                                         (len(entry[0]), entry[0].lower())))


def iterate_all_photos(project: Path,
                       date_format: str,
                       group_ordering: Literal['abc', 'natural', 'num'],
                       order: bool = False,
                       log_summary: bool = True) -> \
        Generator[Path, None, None]:
    """
    Get a generator that iterates over every photo in the timelapse project.

    :param project: The root project directory.
    :param date_format: The date format used in the directory names.
    :param group_ordering: The method for ordering (and filtering) groups.
    :param order: Iterate over the photos in order.
    :param log_summary: Whether to log summary statistics.
    :return: A generator that yields a path to each photo.
    """

    # Initialize counters. (These aren't used if the log summary is disabled).
    dates, groups, photos = 0, 0, 0

    # Iterate through each date directory
    for date_dir in iterate_date_dirs(project, date_format, order):
        photos_in_date = 0
        dates += 1

        # Then through each group directory
        for group_dir in iterate_group_dirs(date_dir, group_ordering, order):
            photos_in_group = 0
            groups += 1

            # And then each photo
            for photo in group_dir.iterdir():
                # Ignore directories
                if not photo.is_file():
                    continue

                photos += 1
                photos_in_group += 1
                photos_in_date += 1
                yield photo

            # Log summary stats for this group, if enabled
            if log_summary:
                _log.debug(
                    f"Group: found {photos_in_group} "
                    f"photo{'' if photos_in_group == 1 else 's'} "
                    f"in .{os.sep}{group_dir.relative_to(project)}"
                )

        # Log summary stats for this date, if enabled
        if log_summary:
            _log.debug(
                f"Date: found {photos_in_date} "
                f"photo{'' if photos_in_date == 1 else 's'} "
                f"in .{os.sep}{date_dir.relative_to(project)}"
            )

    if log_summary:
        _log.info(f"Found a total of {dates} date{'' if dates == 1 else 's'} "
                  f"containing {groups} group{'' if groups == 1 else 's'} "
                  f"and {photos} photo{'' if photos == 1 else 's'}")
