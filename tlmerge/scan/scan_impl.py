from collections.abc import Callable, Iterable, Generator
from datetime import datetime
import logging
from pathlib import Path
from random import shuffle
from typing import Optional

import rawpy
from rawpy import LibRawFileUnsupportedError, LibRawIOError, LibRawError

from tlmerge.conf import CONFIG, DEFAULT_CONFIG_FILE
from tlmerge.db import MAX_DATE_LENGTH, MAX_GROUP_LENGTH, MAX_PHOTO_NAME_LENGTH
from .metrics import ScanMetrics

_log = logging.getLogger(__name__)

# These functions are for each of the group ordering policies. Each policy
# is associated with a dual-purpose map/filter function (going from a
# file name to a sort key) and a sorting function for sorting based on that
# key if ordering is enabled.
_GROUP_ORDERING_POLICIES: dict[str, tuple[Optional[Callable], Callable]] = {
    'num': (lambda n: float(n), lambda e: (e[1], e[0])),
    'abc': (lambda n: n.isalpha(),
            lambda e: (len(e[0].name), e[0].name.lower())),
    'natural': (None, lambda e: e[0].name)
}


def iterate(root: Path,
            excluded: list[str],
            max_length: int,
            yield_dirs: bool = True,
            map_func: Callable[[str], any] | None = None) -> \
        Generator[tuple[Path, any], None, None]:
    """
    Iterate over all items in a given directory, yielding the files or
    directories as required.

    :param root: The root directory to scan. Scanning is not recursive.
    :param excluded: A list of any names of any items to exclude. (These
     are strictly file/directory names, not full paths).
    :param max_length: The maximum length of an item. This is used to ensure
     compatibility with the database. Any items that pass the map_func but
     exceed the max_length are skipped and trigger a warning message.
    :param yield_dirs: Whether to yield directories (True) or files (False).
     Defaults to True.
    :param map_func: This is an optional mapping function to apply to the name
     of each item (file/directory) that doubles as a filter. If this returns
     False or throws a ValueError for an item, that item is omitted. Any
     other values besides False are considered a pass. Note that other
     Exceptions are not caught.
    :return: A generator yielding paths to matching files/directories along with
     the result of the name_filter evaluation, if such a filter was used.
    """

    f = None

    # Iterate over everything in the root directory
    for path in root.iterdir():
        # Make sure it's a file/directory as required
        if yield_dirs:
            if not path.is_dir():
                continue
        elif not path.is_file():
            continue

        # If explicitly excluded, skip it
        if path.name in excluded:
            continue

        # Ensure it passes the filter, if given
        if map_func is not None:
            try:
                f = map_func(path.name)
                if f is False:
                    continue
            except ValueError:
                # ValueError is also considered failing the filter
                continue

        # If the name exceeds the max length, skip
        if len(path.name) > max_length:
            _log.warning(
                f'Skipping "{path}", as "{path.name}" exceeds the maximum '
                f'supported length in the database ({max_length} characters)'
            )
            continue

        # Yield the path
        yield path, f


def is_rawpy_compatible(path: str) -> bool:
    """
    Check whether a file is supported by RawPy/LibRaw by attempting to open it.
    Note that this is somewhat performance-heavy.

    Note that any exceptions besides LibRawFileUnsupportedError are propogated.

    :param path: The path to the file as a string.
    :return: True if and only if it is successfully opened with rawpy.imread().
    """

    try:
        with rawpy.imread(path):
            return True
    except (LibRawFileUnsupportedError, LibRawIOError):
        return False
    except LibRawError as e:
        _log.error(f'Got unexpected error while testing "{path}" '
                   f'for RawPy compatibility: {e}')
        return False  # Clearly not compatible


def yield_gen(generator: Iterable[tuple[Path, any]],
              sort_key: Callable[[tuple[Path, any]], any] | bool | None,
              randomize: bool,
              yield_count: bool = False) -> Generator[int | Path, None, None]:
    """
    Process a generator of Paths based on the given configuration.

    :param generator: The source generator (or other iterable).
    :param sort_key: A lambda that accepts an item from the source generator
     (a tuple with a Path and some other object) and uses it as the sort key
     to order the list. If this is True, the list is ordered based on the Path
     only (discarding the second element of the tuple). If this is None or
     False, the generator is not ordered.
    :param randomize: Whether to randomize the generator in a list before
     yielding it.
    :param yield_count: Whether to first count and yield the total number of
     paths in the source generator before yielding from it. Defaults to False.
    :return: A generator yielding only Paths (and first the total count if
     specified).
    """

    if randomize:
        # Convert to a list to shuffle
        paths = [p for p, _ in generator]
        shuffle(paths)
        if yield_count:
            yield len(paths)
        yield from paths
    elif sort_key:
        # Yield in order according to sort key
        if yield_count:
            if sort_key is True:
                paths = sorted(p for p, _ in generator)
            else:
                paths = [p for p, _ in sorted(generator, key=sort_key)]
            yield len(paths)
            yield from paths
        elif sort_key is True:
            yield from sorted(d for d, _ in generator)
        else:
            yield from (d for d, _ in sorted(generator, key=sort_key))
    else:
        # Order doesn't matter, so just extract paths
        if yield_count:
            paths = [p for p, _ in generator]
            yield len(paths)
            yield from paths
        else:
            yield from (p for p, _ in generator)


def iter_dates(project_root: Path,
               date_format: str,
               excluded: list[str], *,
               order: bool = False,
               randomize: bool = False,
               yield_count: bool = False) -> Generator[int | Path, None, None]:
    """
    Iterate over all the directories in the project root that match the date
    format. These are the date directories, which contain groups, which
    contain photos.

    :param project_root: The root project directory containing the dates.
    :param date_format: The format string to use when parsing the dates.
    :param excluded: A list of any dates to exclude.
    :param order: Whether to yield the dates in chronological order. Defaults
     to False.
    :param randomize: Whether to yield the dates in a random order.  Defaults
     to False.
    :param yield_count: Whether to first yield the total number of dates before
     yielding the dates themselves. Defaults to False.
    :return: A generator yielding paths to matching date directories. The first
     item yielded is an integer only if yield_count is True.
    """

    yield from yield_gen(
        iterate(
            project_root,
            excluded,
            MAX_DATE_LENGTH,
            map_func=lambda n: datetime.strptime(n, date_format)
        ),
        (lambda e: e[1]) if order else None,
        randomize,
        yield_count
    )


def iter_groups(date_dir: Path, *,
                order: bool = False,
                randomize: bool = False,
                scan_all: bool = False,
                yield_count: bool = False) -> Generator[int | Path, None, None]:
    """
    Iterate over all the directories in a particular date directory. These
    are the group directories, which contain photos.

    :param date_dir: The date directory containing the groups.
    :param order: Whether to sort and yield the groups in order.  Defaults to
     False.
    :param randomize: Whether to yield the groups in a random order. Defaults
     to False.
    :param scan_all: Whether to ignore any configuration to exclude certain
     groups, yielding all of them. Defaults to False.
    :param yield_count: Whether to first yield the total number of groups
     before yielding the groups themselves. Defaults to False.

    :return: A generator yielding paths to matching group directories.The first
     item yielded is an integer only if yield_count is True.
    """

    # Get the Config record specific to this date
    date_name = date_dir.name
    cfg = CONFIG[date_name]

    # Get the group ordering policy
    group_ordering = cfg.group_ordering

    # Determine which (if any) groups to exclude
    excluded = [] if scan_all else cfg.get_excluded_groups(date_name)

    # Iterate over and yield the groups
    yield from yield_gen(
        iterate(
            date_dir,
            excluded,
            MAX_GROUP_LENGTH,
            map_func=_GROUP_ORDERING_POLICIES[group_ordering][0]
        ),
        _GROUP_ORDERING_POLICIES[group_ordering][1] if order else None,
        randomize,
        yield_count
    )


def iter_photos_in_group(
        group_dir: Path, *,
        order: bool = False,
        randomize: bool = False) -> \
        Generator[Path, None, None]:
    """
    Iterate over all the photos in a particular group directory.

    :param group_dir: The group directory containing the photos to retrieve.
    :param order: Whether to sort and yield the photos in order. Defaults to
     False.
    :param randomize: Whether to yield the photos in a random order. Defaults
     to False.
    :return: A generator yielding paths to matching photo files.
    """

    # TODO add config option for excluding individual photos
    excluded = []

    # Retrieve all the photo files (making sure not to include config files)
    yield from yield_gen(
        iterate(
            group_dir,
            excluded,
            MAX_PHOTO_NAME_LENGTH,
            yield_dirs=False,
            map_func=lambda n: n != DEFAULT_CONFIG_FILE
        ),
        True if order else None,
        randomize
    )


# noinspection PyProtectedMember
def iter_photos(*, metrics: ScanMetrics,
                project_root: Path,
                date_format: str,
                excluded_dates: list[str],
                order: bool,
                validate: bool = False,
                sample: int = -1) -> Generator[Path, None, None]:
    """
    Get a generator that iterates over every photo in the timelapse project.
    Ordering is optional. Use `iter_all_photos_random` to yield in a random
    order.

    :param metrics: Scanner metrics to track the progress and collect summary
     statistics.
    :param project_root: See `iter_dates()`.
    :param date_format: See `iter_dates()`.
    :param excluded_dates: See `iter_dates()` `excluded` parameter.
    :param order: Whether to sort and yield all the photos in order (including
     chronological and group order).
    :param validate: Whether to validate each photo to ensure that it can be
     processed with RawPy (LibRaw). Defaults to False.
    :param sample: The number of photos to yield if sampling. If this is
     negative, all photos are yielded (i.e. no sample). Note that a sample of
     size 0 is not supported and will trigger an error. Defaults to -1.
    :return: A generator that yields the path to each photo.
    :raises ValueError: If `sample` is 0.
    """

    if sample == 0:
        raise ValueError("Invalid sample size: must be a negative "
                         "(no sample) or a positive integer, not 0")

    # Get the date generator so we can grab the first item, the date count
    date_gen = iter_dates(
        project_root,
        date_format,
        excluded_dates,
        order=order,
        yield_count=True
    )
    metrics._start(dates=next(date_gen), sample_size=sample)

    # Iterate over each date
    for date_dir in date_gen:
        # Get the group generator so we can get the group count
        group_gen = iter_groups(date_dir, order=order, yield_count=True)
        metrics._start_date(date_dir.name, next(group_gen))

        # Iterate over each group
        for group_dir in group_gen:
            metrics._start_group(group_dir.name)

            # Iterate over each photo
            for photo in iter_photos_in_group(group_dir, order=order):
                invalid = validate and not is_rawpy_compatible(str(photo))

                if not invalid:
                    yield photo

                # Update metrics. Exit if sample size reached
                if metrics._next_photo(invalid=invalid):
                    metrics._end()
                    return

            metrics._end_group()
        metrics._end_date()
    metrics._end()


# noinspection PyProtectedMember
class DateIterator:
    """
    A DateIterator record is used by `iter_all_photos_random()` to keep track
    of multiple open dates. It stores data about an opened date directory.
    This includes (1) the row number of the date within the progress table,
    (2) a generator yielding photos from one of the groups, and (3) another
    generator yielding the other groups. When the photo_gen is exhausted, the
    next group is opened.
    """

    def __init__(self,
                 date_dir: Path,
                 metrics: ScanMetrics) -> None:
        """
        Initialize a DateData record for the given date.

        :param date_dir: The date directory associated with this data.
        :param metrics: The scan metrics, used to start the date and group and
         get the row number in the progress table.
        :return: None
        :raises StopIteration: If there are no groups in this date directory.
        """

        self.row: int = metrics._start_date(date_dir.name, next_row=True)
        self.group_gen: Generator[Path, None, None] = iter_groups(
            date_dir, randomize=True
        )

        # Load the next group
        group = next(self.group_gen)
        metrics._start_group(group.name)
        self.photo_gen: Generator[Path, None, None] = iter_photos_in_group(
            group, randomize=True
        )

    def next_photo(self, metrics: ScanMetrics) -> Path | None:
        """
        Get the path to the next photo. If the `photo_gen` is exhausted, this
        loads the next group from the `group_gen`. If that too is exhausted,
        this returns None to indicate that the date is entirely exhausted of
        photo.

        :param metrics: The scan metrics, used to update the group count if the
         next one is started. This does not call `_next_photo()`.
        :return: The path to the next photo, or None if there aren't any more
         photos.
        """

        # Continue until finding a photo or exhausting all generators
        while True:
            try:
                # Get the next photo from the active photo generator
                photo = next(self.photo_gen)
                return photo
            except StopIteration:
                # No more photos in this generator: on to next group
                try:
                    group = next(self.group_gen)
                    self.photo_gen = iter_photos(group, randomize=True)
                    metrics._start_group(group.name)
                except StopIteration:
                    # No more groups; this date is done
                    return None


# noinspection PyProtectedMember
def iter_photos_random(metrics: ScanMetrics,
                       project_root: Path,
                       date_format: str,
                       excluded_dates: list[str],
                       sample_size: int,
                       validate: bool = False) -> Generator[Path, None, None]:
    """
    Iterate over all photos in the timelapse project directory in a random
    order, up to the stated sample size.

    In the interest of not using excessive memory or time, this isn't
    totally random. It roughly performs a stratified sampling over each
    date, from which it exhausts each group one at a time in a random
    order.

    (Note that there are some edge cases. For example, with one date
    containing one group, or one date whose n groups each contain one photo,
    this is a perfectly pseudo-random selection).

    :param metrics: Scanner metrics to track the progress and collect summary
     statistics.
    :param project_root: See `iter_dates()`.
    :param date_format: See `iter_dates()`.
    :param excluded_dates: See `iter_dates()` `excluded` parameter.
    :param validate: Whether to validate each photo to ensure that it can be
     processed with RawPy (LibRaw). Defaults to False.
    :param sample_size: The sample size. This method only supports samples.
    :return: A generator that yields the path to random photos.
    :raises ValueError: If `sample_size` is less than 1.
    """

    if sample_size < 1:
        raise ValueError("Invalid sample size: must be a positive integer "
                         f"for random photo iteration: got {sample_size}")

    # Get the date generator so we can grab the first item, the date count.
    # This is set to None once empty
    date_gen = iter_dates(
        project_root,
        date_format,
        excluded_dates,
        randomize=True,
        yield_count=True
    )
    metrics._start(dates=next(date_gen), sample_size=sample_size)

    open_dates: list[DateIterator] = []

    # This index is the active position in date_info
    g = 0

    while True:
        # If there are more date directories we haven't indexed yet, open
        # another date here. However, if there are already enough dates
        # available to select just 1 photo from each remaining dates, skip
        # this step. This will run at least once the first time the main
        # loop runs, as open_dates is initially empty
        if date_gen is not None and \
                len(open_dates) - g < metrics.remaining_photos:
            try:
                date_dir = next(date_gen)
            except StopIteration:
                date_gen = None  # Indicate that there are no more dates
                break

            try:
                open_dates.append(DateIterator(date_dir, metrics))
            except StopIteration:
                # This date is empty; skip it
                _log.debug(f'Date "{date_dir.name}" has no groups')
                metrics.table.update('Groups', 0)
                metrics.table.update('Photos', 0)
                continue

        # If the group index reached the end of the list of groups, go back
        # to the first one
        if g >= len(open_dates):
            if g == 0:
                # We still need more photos, but there aren't any groups
                # left to scan. Exit
                break
            else:
                g = 0

        # Get a photo from the next group
        date_iterator: DateIterator = open_dates[g]
        photo: Path | None = date_iterator.next_photo(metrics)

        if photo is None:
            # No more photos in this date
            del open_dates[g]
            continue

        invalid = validate and not is_rawpy_compatible(str(photo))
        if not invalid:
            yield photo

        if metrics._next_photo(invalid=invalid, row=date_iterator.row):
            break

        g += 1

    # Done
    del open_dates

    # Done with metrics
    metrics._end()
