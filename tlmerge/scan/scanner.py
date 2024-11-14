import asyncio
from collections.abc import Callable, Generator
from datetime import date, datetime
import logging
from pathlib import Path
from random import shuffle

from tlmerge.conf import CONFIG, DEFAULT_CONFIG_FILE

_log = logging.getLogger(__name__)


async def _iter(root: Path,
                excluded: list[str],
                get_dirs: bool = True,
                map_func: Callable[[str], any] | None = None) -> \
        Generator[tuple[Path, any], None, None]:
    """
    Iterate over all items in a given directory, yielding the files or
    directories as required.

    :param root: The root directory to scan. Scanning is not recursive.
    :param excluded: A list of any names of any items to exclude. (These
     are strictly file/directory names, not full paths).
    :param get_dirs: Whether to get directories (True) or files (False).
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
    for path in await asyncio.to_thread(root.iterdir):
        # Make sure it's a file/directory as required
        if get_dirs:
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

        # Yield the path
        yield path, f


class Scanner:
    def __init__(self,
                 scan_all: bool = False,
                 order: bool = False):
        """
        Initialize a Scanner, which iterates through dates, groups, and photos
        in the timelapse project directory.

        :param scan_all: Whether to scan everything. If True, the configurations
         for ignored dates, directories, and photos, are all ignored, as well as
         the sample settings. This is useful for initially locating all the
         config files.
        :param order: Whether to yield files and directories in order. (This
         will make it slower).
        """

        self.scan_all = scan_all
        self.order = order

    async def iter_dates(self) -> Generator[Path, None, None]:
        """
        Iterate over all the directories in the project root that match the date
        format. These are the date directories, which contain groups, which
        contain photos.

        :return: A generator yielding paths to matching date directories.
        """

        cfg = CONFIG.root

        # Get the date format from root/global config
        date_format = cfg.date_format

        # Determine which (if any) dates to exclude and whether to sample
        if self.scan_all:
            excluded_dates = []
            sample, s_random = False, False
        else:
            excluded_dates = CONFIG.root.get_excluded_dates()
            sample, s_random, _ = cfg.sample_details()

        ##################################################

        # Generator that retrieves all the date directories
        generator = _iter(
            cfg.project,
            excluded_dates,
            map_func=lambda n: datetime.strptime(n, date_format)
        )

        # If order doesn't matter, just yield directly from the generator
        if not sample and not self.order:
            async for directory, _ in generator:
                yield directory
            return

        # Otherwise, collect all directories in a list along with their parsed
        # date value
        directories: list[tuple[Path, date]] = []
        async for directory, dt in generator:
            directories.append((directory, dt))

        # Shuffle/sort based on sample mode and whether to order
        if s_random:
            shuffle(directories)
            for directory, _ in directories:
                yield directory
        else:
            # Sort by the datetime to yield in chronological order
            for directory, _ in sorted(directories, key=lambda entry: entry[1]):
                yield directory

    async def iter_groups(self, date_dir: Path) -> Generator[Path, None, None]:
        """
        Iterate over all the directories in a particular date directory. These
        are the group directories, which contain photos.

        :return: A generator yielding paths to matching group directories.
        """

        # Get the Config record specific to this date
        cfg = CONFIG[date_dir.name]

        # Get the group ordering policy from the root/global config
        group_ordering = cfg.group_ordering

        # Determine which (if any) groups to exclude and whether to sample
        if self.scan_all:
            excluded_groups = []
            sample, s_random = False, False
        else:
            excluded_groups = cfg.get_excluded_groups(date_dir.name)
            sample, s_random, _ = CONFIG.root.sample_details()

        ##################################################

        # Get a mapping function based on the group ordering policy. The
        # lambdas accept the group directory names, returning False or raising
        # ValueError to exclude them
        if group_ordering == 'num':
            map_func = lambda n: float(n)
        elif group_ordering == 'abc':
            map_func = lambda n: n.isalpha()
        elif group_ordering == 'natural':
            map_func = None
        else:
            raise RuntimeError(f'Unsupported group ordering "{group_ordering}"')

        # Get a generator that retrieves all the group directories
        generator = _iter(date_dir, excluded_groups, map_func=map_func)

        ##################################################

        # If order doesn't matter, just yield directly from the generator
        if not sample and not self.order:
            async for directory, _ in generator:
                yield directory
            return

        # If it's a randomized sample, shuffle first, and then yield
        if s_random:
            directories = [d async for d, _ in generator]
            shuffle(directories)
            for d in directories:
                yield d
            return

        # Otherwise, get a sort key sort based on the group ordering policy.
        # Each is a lambda that accepts an entry e = (path, mapped_value)
        if group_ordering == 'num':
            sort_key = lambda e: (e[1], e[0])
        elif group_ordering == 'abc':
            sort_key = lambda e: (len(e[0].name), e[0].name.lower())
        else:
            sort_key = lambda e: e[0].name

        # Now sort and yield the directories
        for d, _ in sorted([entry async for entry in generator], key=sort_key):
            yield d

    async def iter_photos(
            self,
            group_dir: Path,
            randomize: bool = False) -> Generator[Path, None, None]:
        """
        Iterate over all the photos in a particular group directory.

        :param group_dir: The group directory containing the photos to retrieve.
        :param randomize: Whether to randomize the order in which the photos
         are returned. This overrides `self.order` if True. Defaults to False.
        :return: A generator yielding paths to matching photo files.
        """

        # Determine which (if any) photos to exclude and whether to sample
        if self.scan_all:
            excluded_photos = []
        else:
            # TODO add config option for excluding individual photos
            excluded_photos = []

        ##################################################

        # Get a generator that retrieves all the photo files (making sure not
        # to include config files)
        generator = _iter(
            group_dir,
            excluded_photos,
            get_dirs=False,
            map_func=lambda n: n != DEFAULT_CONFIG_FILE
        )

        # If order doesn't matter, just yield directly from the generator
        if not randomize and not self.order:
            async for directory, _ in generator:
                yield directory
            return

        # Otherwise, transfer generator to a list
        photos = [d async for d, _ in generator]

        # If randomized, shuffle. Otherwise, sort
        if randomize:
            shuffle(photos)
            for d in photos:
                yield d
        else:
            for d in sorted(photos):
                yield d

    async def iter_all_photos(
            self,
            log_summary: bool = True) -> Generator[Path, None, None]:
        """
        Get a generator that iterates over every photo in the timelapse project.

        :param log_summary: Whether to log summary statistics.
        :return: A generator that yields a path to each photo.
        """

        cfg = CONFIG.root

        # If not scanning all the photos, check whether a sample is active
        if self.scan_all:
            sample, s_size = False, None
        else:
            sample, s_random, s_size = cfg.sample_details()

            # If randomly sampling, defer to the randomized iterator
            if s_random:
                async for p in self.iter_all_photos_random(s_size, log_summary):
                    yield p
                return

        # Initialize counters (only used if the log summary is enabled)
        dates, groups, photos = 0, 0, 0

        ##################################################

        # Iterate over each date
        async for date_dir in self.iter_dates():
            date_photo_counter, dates = 0, dates + 1

            # Iterate over each group
            async for group_dir in self.iter_groups(date_dir):
                group_photo_counter, groups = 0, groups + 1

                # Iterate over each photo
                async for photo in self.iter_photos(group_dir):
                    yield photo
                    photos += 1
                    group_photo_counter += 1

                    # If this reached the sample size, exit
                    if sample and photos >= s_size:
                        if log_summary:
                            msg = (f"Got deterministic sample of {photos} "
                                   f"photo{'' if photos == 1 else 's'} from ")
                            if groups == 1:
                                msg += str(cfg.rel_path(group_dir))
                            elif dates == 1:
                                msg += f"{groups} groups in {date_dir.name}"
                            else:
                                msg += f"{groups} groups in {dates} dates"
                            _log.info(msg)
                        return

                # Log summary stats for this group, if enabled
                if log_summary:
                    _log.debug(
                        f"Group: found {group_photo_counter} "
                        f"photo{'' if group_photo_counter == 1 else 's'} "
                        f"in {cfg.rel_path(group_dir)}"
                    )

            # Log summary stats for this date, if enabled
            if not sample and log_summary:
                _log.debug(
                    f"Date: found {date_photo_counter} "
                    f"photo{'' if date_photo_counter == 1 else 's'} "
                    f"in {cfg.rel_path(date_dir)}"
                )

        # Overall summary stats
        if log_summary:
            _log.info(
                f"Found a total of {dates} date{'' if dates == 1 else 's'} "
                f"containing {groups} group{'' if groups == 1 else 's'} "
                f"and {photos} photo{'' if photos == 1 else 's'}"
            )

    async def iter_all_photos_random(
            self,
            sample_size: int | None,
            log_summary: bool = True) -> Generator[Path, None, None]:
        """
        Iterate over all photos in the timelapse project directory in a random
        order, up to the stated sample size.

        In the interest of not using excessive memory or time, this isn't
        totally random. It roughly performs a stratified sampling over each
        date, from which it exhausts each group one at a time in a random
        order.

        The fewer dates there are, the less random this will seem, especially if
        those dates contain groups with many photos each. (However, there are
        some edge cases. For example, with one date containing one group, or
        one date whose n groups each contain one photo, this is a perfectly
        pseudo-random selection).

        :param sample_size: The desired number of photos. If this is None or
         there aren't enough photos to reach this sample size, all photos are
         returned.
        :param log_summary: Whether to log some summary messages.
        :return: A generator that yields paths to random photos.
        """

        # Count the number of photos yielded to stop at the sample size
        counter = 0

        # Get all the date directories. Make sure there's at least one
        date_dir_queue = [d async for d in self.iter_dates()]

        # This list stores a tuple for each date directory. Each tuple contains
        # two elements:
        # 1. A list of all photos that haven't yet been yielded from one of the
        #    groups.
        # 2. A list of group directories that haven't yet been indexed.
        # When the photo list is exhausted, it's replaced with a list of photos
        # from the next group directory.
        group_dirs: list[tuple[list[Path], list[Path]]] = []

        # This index is the active position in group_dirs
        g = 0

        # Continue yielding photos until reaching the sample size
        while sample_size is None or counter < sample_size:
            # If there are more date directories we haven't indexed yet, we can
            # open them here. However, if there are already enough groups
            # available to select just 1 photo from each remaining groups, skip
            # this step. This will run at least once the first time the main
            # loop runs, as group_dirs is initially empty
            while len(date_dir_queue) > 0 and \
                    (sample_size is None or
                     len(group_dirs) < sample_size - counter):
                group_dirs.append((
                    [],
                    [g async for g in self.iter_groups(date_dir_queue.pop())]
                ))

            # If the group index reached the end of the list of groups, go back
            # to the first one
            if g >= len(group_dirs):
                if g == 0:
                    # We still need more photos, but there aren't any groups
                    # left to scan. First log a message (if enabled), then exit
                    if log_summary:
                        if counter == 0 and sample_size is None:
                            _log.warning("Couldn't find any photos at all.")
                        elif counter == 0:
                            _log.warning(
                                "Couldn't find any photos at all. "
                                f"Unable to sample {sample_size} random "
                                f"photo{'' if sample_size == 1 else 's'}"
                            )
                        elif sample_size is None:
                            _log.info(
                                f"Randomly sampled {counter} "
                                f"photo{'' if sample_size == 1 else 's'}"
                            )
                        else:
                            _log.warning(
                                f"Randomly sampled {counter} "
                                f"photo{'' if sample_size == 1 else 's'}. "
                                f"Unable to meet desired sample size "
                                f"{sample_size} "
                                f"photo{'' if sample_size == 1 else 's'}"
                            )
                    return
                else:
                    g = 0

            # Get a photo from the next group
            photos, groups = group_dirs[g]
            if len(photos) == 0:
                if len(groups) == 0:
                    # There are no more groups to scan for this date. All the
                    # photos have been yielded. Remove it from the list, and
                    # continue (so g will point to the next entry)
                    del group_dirs[g]
                else:
                    # Load all the photos from the next group
                    photos = [p async for p in
                              self.iter_photos(groups.pop(), randomize=True)]
                    group_dirs[g] = (photos, groups)
                continue

            # Yield one photo from the active group, and then continue on to
            # the next group
            yield photos.pop()
            counter += 1
            g += 1

        # Done
        if log_summary:
            _log.info(f"Randomly sampled {counter} "
                      f"photo{'' if sample_size == 1 else 's'}")

        del group_dirs
