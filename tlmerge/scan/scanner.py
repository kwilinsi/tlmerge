from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Literal


def iterate_date_dirs(project: Path,
                      date_format: str) -> Generator[Path, None, None]:
    """
    Iterate over all the directories in the project root that match the date
    format. These are the date directories, which contain groups, which contain
    photos.

    :param project: The root project directory.
    :param date_format: The date format used in the directory names.
    :return: A generator yielding matching directories.
    """

    # Iterate over everything in the project root
    for directory in project.iterdir():
        # Make sure it's a directory
        if not directory.is_dir():
            continue

        # Ensure it matches the date format
        try:
            datetime.strptime(directory.name, date_format)
        except ValueError:
            continue

        # This is a date directory
        yield directory


def iterate_group_dirs(date_dir: Path,
                       group_ordering: Literal['abc', 'natural', 'num']) -> \
        Generator[Path, None, None]:
    """
    Iterate over all the group directories within a particular date in a
    project.

    :param date_dir: The path to the date directory, which contains zero or
     more groups.
    :param group_ordering: How the groups are ordered. This doesn't control the
     order the groups are yielded by this function, but 'abc' and 'num' cause
     non-matching directories to be ignored. If using 'natural' ordering, all
     directories are returned.
    :return: A generator yielding matching groups.
    """

    # Get every directory
    directories = [d for d in date_dir.iterdir() if d.is_dir()]

    # In 'natural' ordering mode, yield everything
    if group_ordering == 'natural':
        yield from iter(directories)
        return

    # In 'num' mode, only yield directories that can be parsed as floats
    if group_ordering == 'num':
        for directory in directories:
            try:
                float(directory.name)
                yield directory
            except ValueError:
                pass
        return

    # Finally, in 'abc' mode, only include directories that contain exclusively
    # letters (no digits, spaces, punctuation, etc.)
    for directory in directories:
        if directory.name.isalpha():
            yield directory
