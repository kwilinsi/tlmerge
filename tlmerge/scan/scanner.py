import asyncio
from collections.abc import Callable, Generator
from datetime import date, datetime
import logging
import math
import os
from pathlib import Path
from random import shuffle
from typing import Optional

from tlmerge.conf import CONFIG

_log = logging.getLogger(__name__)


async def iterate_date_dirs(project: Path, *,
                            ignore_excluded: bool = False,
                            order: bool = False) -> Generator[Path, None, None]:
    """
    Iterate over all the directories in the project root that match the date
    format. These are the date directories, which contain groups, which contain
    photos.

    :param project: The root project directory.
    :param ignore_excluded: Whether to ignore the configuration that could
     otherwise exclude certain dates.
    :param order: Whether to iterate over the dates chronologically. This is
     ignored if the sample config is enabled and randomized. This is forced if
     the sample is enabled and NOT randomized.
    :return: A generator yielding paths to matching date directories.
    """

    dirs: list[tuple[date, Path]] = []
    cfg = CONFIG.root

    # Get the date format from root/global config
    date_format = cfg.date_format

    # Determine which (if any) dates to exclude
    if ignore_excluded:
        excluded_dates = []
    else:
        excluded_dates = CONFIG.root.get_excluded_dates()

    # Check whether a sample is active
    sample, s_random, _ = cfg.sample_details()

    # Iterate over everything in the project root
    for directory in await asyncio.to_thread(project.iterdir):
        # Make sure it's a directory
        if not directory.is_dir():
            continue

        # If explicitly excluded, skip it
        if directory.name in excluded_dates:
            continue

        # Ensure it matches the date format
        try:
            dt = datetime.strptime(directory.name, date_format)
        except ValueError:
            continue

        # Either collect the dates (if ordering them or using a sample)
        # or yield them here
        if order or sample:
            dirs.append((dt, directory))
        else:
            yield directory

    # Shuffle/sort based on sample mode and whether to order
    if s_random:
        shuffle(dirs)
        for _, d in dirs:
            yield d
    elif order or sample:
        for _, d in sorted(dirs, key=lambda k: k[0]):
            yield d


async def iterate_group_dirs(date_dir: Path, *,
                             ignore_excluded: bool = False,
                             order: bool = False) -> \
        Generator[Path, None, None]:
    """
    Iterate over all the group directories within a particular date in a
    project.

    :param date_dir: The path to the date directory, which contains zero or
     more groups.
    :param ignore_excluded: Whether to ignore configurations that could
    otherwise exclude certain groups.
    :param order: Whether to iterate over the groups in order based on the
     group_ordering.
    :return: A generator yielding paths to matching groups.
    """

    # Get the Config record specific to this date
    cfg = CONFIG[date_dir.name]

    # Get the group ordering policy from the root/global config
    group_ordering = cfg.group_ordering

    # Determine which (if any) groups to exclude
    if ignore_excluded:
        excluded_groups = []
    else:
        excluded_groups = cfg.get_excluded_groups(date_dir.name)

    # Check whether a sample is active
    sample, s_random, _ = CONFIG.root.sample_details()

    # Get every non-excluded directory
    directories = [d for d in await asyncio.to_thread(date_dir.iterdir)
                   if d.is_dir() and not d.name in excluded_groups]

    if group_ordering == 'num':
        # In 'num' mode, only include directories that can be parsed as floats
        def is_float(directory: Path):
            try:
                float(directory.name)
                return True
            except ValueError:
                return False

        gen = _iterate_group_helper(
            directories, order, sample, s_random,
            lambda d: (float(d.name), d), filter_func=is_float,
        )
    elif group_ordering == 'abc':
        # In 'abc' mode, only include directories that contain exclusively
        # letters (no digits, spaces, punctuation, etc.). Sort order is
        # determined first by length (ascending) and then alphabetical
        # (ignoring case).
        gen = _iterate_group_helper(
            directories, order, sample, s_random,
            lambda d: (len(d.name), d.name.lower()),
            filter_func=lambda d: d.name.isalpha(),
        )
    elif group_ordering == 'natural':
        # In 'natural' ordering mode, yield everything
        gen = _iterate_group_helper(
            directories, order, sample, s_random, lambda d: d.name
        )
    else:
        raise RuntimeError(f'Unsupported group ordering "{group_ordering}"')

    # Now yield everything
    for d in gen:
        yield d


def _iterate_group_helper(
        directories: list[Path],
        order: bool,
        sample: bool,
        s_random: bool,
        sort_key,
        filter_func: Optional[Callable[[Path], bool]] = None) -> \
        Generator[Path, None, None]:
    """
    Helper function for iterate_group_dirs() that handles shuffling/sorting a
    list of paths and yielding only some in sample mode.

    :param directories: The initial list of directory paths.
    :param order: Whether the list of directories should be ordered.
    :param sample: Whether we're in sample mode.
    :param s_random: Whether the sample is randomized (must be False if not in
     sample mode).
    :param sort_key: The sort key to pass to sorted() (ignored if not sorting).
    :param filter_func: A filter function to apply to each directory. If the
     function returns False, the directory is excluded. If None, no filter is
     applied, and the filter_value is ignored. Defaults to None.
    :return: A generator yielding the correct number of directory paths in the
     correct order.
    """

    # Apply filter function if necessary
    if filter_func is not None:
        if order or sample:
            # Replace with a filtered list
            directories = [d for d in directories if filter_func(d)]
        else:
            # Order doesn't matter, so yield everything matching filter
            yield from (d for d in directories if filter_func(d))

    if s_random:
        # In random sample mode, shuffle the directories
        shuffle(directories)
    elif order or sample:
        # In non-random sample mode or if explicitly ordered, sort the dirs
        yield from sorted(directories, key=sort_key)
        return

    yield from directories


async def iterate_all_photos(project: Path,
                             order: bool = False,
                             log_summary: bool = True) -> \
        Generator[Path, None, None]:
    """
    Get a generator that iterates over every photo in the timelapse project.

    :param project: The root project directory.
    :param order: Iterate over the photos in order.
    :param log_summary: Whether to log summary statistics.
    :return: A generator that yields a path to each photo.
    """

    # Initialize counters. (These aren't used if the log summary is disabled).
    dates, groups, photos = 0, 0, 0

    date_dirs = [d async for d in iterate_date_dirs(project, order=order)]

    # Check whether a sample is active. If it's a random sample with a size
    # greater than 1, check the number of date_dirs in order to get a (roughly)
    # stratified sample across dates
    sample, s_random, s_size = CONFIG.root.sample_details()
    photos_per_date, photos_per_group = None, None
    if s_random and s_size > 1:
        photos_per_date = int(math.ceil(s_size / len(date_dirs)))

    # Iterate through each date directory
    for date_dir in date_dirs:
        date_photo_counter = 0
        dates += 1

        # Get all the groups in this date directory
        group_dirs = [g async for g
                      in iterate_group_dirs(date_dir, order=order)]

        # In a randomized sample, check the number of group_dirs to (roughly)
        # stratify the sampling of photos
        if photos_per_date is not None and photos_per_date > 1:
            photos_per_group = int(math.ceil(photos_per_date / len(group_dirs)))
            print('photos per group:', photos_per_group)

        # Then through each group directory
        for group_dir in group_dirs:
            group_photo_counter = 0
            groups += 1

            # Get all photo paths
            photo_paths = await asyncio.to_thread(group_dir.iterdir)

            # Sort or shuffle if necessary
            if s_random:
                photo_paths = list(photo_paths)
                shuffle(photo_paths)
            elif order or sample:
                photo_paths = sorted(photo_paths)

            # And then each photo
            for photo in photo_paths:
                # Ignore directories
                if not photo.is_file():
                    continue

                photos += 1
                group_photo_counter += 1
                date_photo_counter += 1
                yield photo

                # In sample mode, check the counts to possible exit
                if sample:
                    # Check the total photo count
                    if photos == s_size:
                        if log_summary:
                            _log.debug(
                                f"Got complete sample of {photos} "
                                f"photo{'' if photos == 1 else 's'} from "
                                f"{dates} date{'' if dates == 1 else 's'} and "
                                f"{groups} group{'' if groups == 1 else 's'}"
                            )
                        return
                    # Check the number of photos for this group
                    if group_photo_counter == photos_per_group or \
                            date_photo_counter == photos_per_date:
                        break

            # In sample mode, check if enough photos were captured for this
            # date. Otherwise, check log summary stats. (Don't do both, because
            # this log message would be confusing in sample mode).
            if sample:
                if photos_per_date is not None and \
                        date_photo_counter == photos_per_date:
                    break
            elif log_summary:
                _log.debug(
                    f"Group: found {group_photo_counter} "
                    f"photo{'' if group_photo_counter == 1 else 's'} "
                    f"in .{os.sep}{group_dir.relative_to(project)}"
                )

        # Log summary stats for this date, if enabled and not in sample mode
        if not sample and log_summary:
            _log.debug(
                f"Date: found {date_photo_counter} "
                f"photo{'' if date_photo_counter == 1 else 's'} "
                f"in .{os.sep}{date_dir.relative_to(project)}"
            )

    if log_summary:
        _log.info(f"Found a total of {dates} date{'' if dates == 1 else 's'} "
                  f"containing {groups} group{'' if groups == 1 else 's'} "
                  f"and {photos} photo{'' if photos == 1 else 's'}")


async def scan(project: Path) -> None:
    """
    Scan all the files in the timelapse directory to log summary statistics
    on the number of photos. This is a blocking operation with long IO delays.

    :param project: The path to the project directory.
    :return: None
    """

    _log.info(f'Scanning timelapse project "{project}" '
              '(this may take some time)')

    # Scan through the photos. Ordered so the log messages look better
    async for _ in iterate_all_photos(project, order=True):
        # The generator logs summary stats; no need to do anything here
        pass
