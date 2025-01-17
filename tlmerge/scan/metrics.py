from __future__ import annotations

import logging
import math
from pathlib import Path
from threading import Lock

from progress_table import ProgressTable
from progress_table.v1.progress_table import TableProgressBar

from tlmerge.db.photos import MAX_DATE_LENGTH

_log = logging.getLogger(__name__)


def update_estimate(*, prior: float,
                    n_total: int,
                    n_remaining: int,
                    observed: float) -> float:
    """
    Given some an average value estimating something, update that estimate with
    a new observation.

    At first (when there are few observations), this applies nearly equal weight
    to the prior and observed values. However, as more observations are applied
    (i.e. as n_remaining decreases), more weight is applied to the prior value.
    By the last observation, almost 100% weight goes to the prior.

    The precise weight applied to the prior value follows this sigmoid-ish
    curve: `y = 0.5 / (1 + e^(-9 * (x - 0.5))) + 0.5`

    Where `x` is the percentage of recorded observations used to compute that
    prior: `(n_total - n_remaining - 1) / n_total`, and `-9` is an arbitrary
    constant to get that "sigmoid feel".

    When there's exactly 1 record observation (i.e. `n_total - n_remaining` is
    `1`), the prior value is ignored entirely (since it was based on 0
    observations). In this case, the observed value is returned unmodified.

    Note that `n_total - n_remaining` must be 1 the first time this is called
    for a metric.

    :param prior: The prior value.
    :param n_total: The total number of observations.
    :param n_remaining: The number of remaining observations.
    :param observed: The new observed value.
    :return: The updated estimate.
    """

    n_elapsed = n_total - n_remaining

    # If n_elapsed is 1, ignore prior, as it was based on 0 observations
    if n_elapsed == 1:
        return observed

    # Adjust prior weight with sigmoid curve based on percent elapsed
    prior_weight = 0.5 + 0.5 / (
            1 + math.exp(-9 * ((n_elapsed - 1) / n_total - 0.5))
    )

    return prior * prior_weight + observed * (1 - prior_weight)


class ScanMetrics:
    """
    This classed is used to keep track of metrics while scanning (total
    number of dates, groups, and photos) and continuously update progress bar
    with the estimated total number of photos to compute an ETA.

    Each metrics object can only be used for one scan.

    When estimating the total number of photos, this makes the assumption that
    each date will have roughly the same number of photos, and all the groups
    within a given date will also have roughly the same number of photos.
    However, this does not assume that groups in different dates will have the
    same number of photos. For example, one date might have 5 groups, each
    with 200 photos, while another date has 10 groups with 100 photos each.
    """

    def __init__(self,
                 table: ProgressTable,
                 pbar: TableProgressBar, *,
                 externally_managed_pbar: bool = False,
                 initial_avg_photos_per_date: float = 500):
        """
        Initialize a ScanMetrics instance for a new scan.

        :param table: The progress table to update.
        :param pbar: A progress bar in the table. The total value is
         continuously updated with the estimated total number of photos.
        :param initial_avg_photos_per_date: This is an initial estimate for
         the number of photos within each date directory. It's used to compute
         the initial estimate. This is somewhat arbitrary, as there's likely
         no good way to know how many photos there are. This is entirely
         superseded after scanning the first group of the first date. Defaults
         to 500.
        :param externally_managed_pbar: Whether the progress bar in this table
         is managed externally. If True, it is neither incremented when a
         photo is scanned nor decremented when a photo is marked invalid. This
         also disables calling close() on the progress table when scanning
         ends. Defaults to False.
        :raises ValueError: If the table is missing any of the required
         columns: "Date", "Groups", "Photos", and "Other Files".
        """

        for col in ('Date', 'Groups', 'Photos', 'Other Files'):
            if col not in table.column_names:
                raise ValueError(f'Progress table missing "{col}" column')

        # Overall counters
        self._total_files: int = 0  # Total file count
        self._total_dates: int = -1  # Not set
        self._total_groups: int = 0
        self._dates_remaining: int = -1  # Not set

        # This is a counter for the number of files that are invalid (i.e. not
        # parseable photos). The lock restricts access to this counter, as it
        # may be read and written by separate threads. Better safe than sorry:
        # https://stackoverflow.com/questions/2291069/
        self._invalid_files: int = 0
        self._invalid_counter_lock: Lock = Lock()

        # Sub-counters
        self._photos_in_group: int = 0
        self._photos_in_date: int = 0  # (Doesn't include current group)
        self._groups_in_date: int = 0
        self._groups_remaining: int = 0

        # Estimates and averages
        self._estimate: int = 0  # Estimated total photos
        self._avg_per_date: float = initial_avg_photos_per_date
        self._avg_per_group: float = 0  # Only for current date
        self._est_total_groups: float = 0  # Estimated total number of groups
        self._est_group_ratio: float = 1  # Estimated ratio of groups processed

        # Progress table
        self._table: ProgressTable = table
        self._pbar: TableProgressBar = pbar
        self._externally_managed_pbar: bool = externally_managed_pbar

        # Map of dates to their row index in the progress table
        self._table_index: dict[str, int] = {}

        # Track whether this is a fixed-size sample, meaning no estimation is
        # necessary for the progress bar
        self._fixed_sample: bool = False

    @classmethod
    def def_progress_table(cls, *,
                           pbar_label: str = 'Scanning...',
                           sample_size: int = -1) -> \
            tuple[ProgressTable, TableProgressBar]:
        """
        Create a new ProgressTable and associated progress bar designed to
        work with ScanMetrics. The table has the required columns for
        ScanMetrics compatibility and an info bar at the bottom.

        :param pbar_label: The text description for the progress bar. Defaults
         to 'Scanning...'.
        :param sample_size: Set the sample size if conducting a sample. If
         this is a positive integer, the progress bar shows exact progress,
         and the total is set to the sample size. Use any negative number 
         to indicate no sample. Defaults to -1.
        :return: A new progress table and associated progress bar.
        :raises ValueError: If the sample size is 0.
        """

        if sample_size == 0:
            raise ValueError("Invalid sample size: must be a negative "
                             "(no sample) or a positive integer, not 0")

        # Create the table
        table = ProgressTable(
            default_header_color='bold',
            pbar_show_eta=True,
            pbar_show_progress=False,
            pbar_show_throughput=False,
            pbar_style='dots'
        )

        # Add minimum required columns for scan metrics
        table.add_column('Date', width=MAX_DATE_LENGTH, alignment='right')
        table.add_column('Groups', width=6, aggregate='sum')
        table.add_column('Photos', width=6, aggregate='sum')
        table.add_column('Other Files', width=11, aggregate='sum')

        # Add a progress bar
        pbar: TableProgressBar = table.pbar(
            max(0, sample_size),
            position=1,
            description=pbar_label,
            color='bright lightyellow_ex',
            color_empty='bold dim yellow',
            show_progress=sample_size > 0
        )

        # Return with new ScanMetrics
        return table, pbar

    @property
    def total_photos(self) -> int:
        """
        Get an active counter of the total number of photos scanned so far.
        This is the number of scanned files minus the number that were
        invalid (i.e. not parseable photos).

        :return: The number of photos scanned so far.
        :raises RuntimeError: If accessed before starting the metrics.
        """

        if self._total_dates == -1:
            raise RuntimeError('Cannot access total_photos before '
                               'starting metrics.')

        with self._invalid_counter_lock:
            return self._total_files - self._invalid_files

    @property
    def total_files(self) -> int:
        """
        Get an active counter of the total number of files so far.

        :return: The total number of files.
        :raises RuntimeError: If accessed before starting the metrics.
        """

        if self._total_dates == -1:
            raise RuntimeError('Cannot access total_photos before '
                               'starting metrics.')

        return self._total_files

    @property
    def total_groups(self) -> int:
        """
        Get the total number of groups. Note that this is likely inaccurate
        while the scanner is running: it is not the total number of groups in
        the project, nor is it exactly the number processed so far. Rather,
        it's the number of groups in the dates seen so far.

        :return: The number of groups (so far).
        :raises RuntimeError: If accessed before starting the metrics.
        """

        if self._total_dates == -1:
            raise RuntimeError('Cannot access total_groups before '
                               'starting metrics.')

        return self._total_groups

    @property
    def total_dates(self) -> int:
        """
        Get the total number of dates.

        :return: The number of dates.
        :raises RuntimeError: If accessed before starting the metrics.
        """

        if self._total_dates == -1:
            raise RuntimeError('Cannot access total_dates before '
                               'starting metrics.')

        return self._total_dates

    @property
    def total_estimate(self) -> int:
        return self._estimate

    @property
    def avg_per_date(self) -> float:
        return self._avg_per_date

    @property
    def remaining_photos(self) -> int:
        """
        Get the number of remaining photos to scan. This is really only useful
        with a fixed-size sample.

        :return: The estimate minus the current number of photos.
        """

        return self._estimate - self.total_photos

    @property
    def scanned_dates(self) -> int:
        """
        Get the number of dates scanned so far.

        :return: The total number of dates minus the number remaining.
        """

        return self._total_dates - self._dates_remaining

    @property
    def scanned_groups(self) -> int:
        """
        Get the total number of groups scanned so far.

        :return: The total number of groups minus the number remaining in the
         current date.
        """

        return self._total_groups - self._groups_remaining

    @property
    def table(self) -> ProgressTable:
        """
        Get the progress table for tracking metrics.

        :return: The progress table.
        """

        return self._table

    @property
    def pbar(self) -> TableProgressBar:
        """
        Get the progress bar associated with the table.

        :return: The progress bar.
        """

        return self._pbar

    def get_row(self, date_str: str) -> int:
        """
        Get the row number in the progress table for the specified date.

        :param date_str: The string used in the date column.
        :return: The row number.
        :raises KeyError: If the given date is not yet in the progress table.
        """

        return self._table_index[date_str]

    def invalid_photo_file(self, *,
                           row_num: int | None = None,
                           date_str: str | None = None,
                           photo: Path | None = None):
        """
        Indicate that the given photo file is in fact not a valid photo. This
        decreases the total photo count and increases the "Other File" count.

        The photo can be identified by one of three parameters: the row number
        in the progress table, the name of the parent date directory, or the
        path to the photo file itself. Only one is necessary, and they are
        checked in that order. Conflicts are not detected.

        :param row_num: The row number in the progress table for the date
         containing the invalid photo. Defaults to None.
        :param date_str: The name of the date directory containing the invalid
         photo. Defaults to None.
        :param photo: The path to the invalid photo. Defaults to None.
        :return: None
        """

        # Increase invalid photo counter
        with self._invalid_counter_lock:
            self._invalid_files += 1

        # Get row number in progress table
        if row_num is None:
            row_num = self._table_index[photo.parent.parent.name
            if date_str is None else date_str]

        # Update progress table
        self._table.update('Photos', -1, row=row_num)
        self._table.update('Other Files', 1, row=row_num)

        # Decrement the progress bar unless it's managed externally
        if not self._externally_managed_pbar:
            self._pbar.update(-1)

    def _start(self, *,
               dates: int,
               sample_size: int = -1) -> None:
        """
        This is called by the scanner when it begins calculating metrics.
        This initializes the estimated total photo count in the progress bar.
        It's also necessary at the beginning to know the total number of
        dates in the project.

        If a sample size is given, that indicated a fixed-size scanning process,
        in which case no progress bar estimation is necessary.

        :param dates: The total number of dates.
        :param sample_size: The sample size to use as the total in the progress
         bar. Use any negative value if not taking a sample.
        :return: None
        :raises RuntimeError: If the metrics were already started.
        :raises ValueError: If the given sample size is 0.
        """

        if self._total_dates != -1:
            raise RuntimeError('Metrics already started')

        self._total_dates = self._dates_remaining = dates

        if sample_size < 0:
            self._set_estimated_photo_count(int(self._avg_per_date * dates))
        elif sample_size == 0:
            raise ValueError("Invalid sample size: must be a negative "
                             "(no sample) or a positive integer, not 0")
        else:
            self._fixed_sample = True
            self._set_estimated_photo_count(sample_size)

    def _start_date(self,
                    date_str: str,
                    groups: int = -1,
                    next_row: bool = False) -> int:
        """
        This is called by the scanner every time it starts scanning a new date.
        For calculating the estimate, it's necessary to know how many groups
        are in this date in advance.

        :param date_str: The date directory name.
        :param groups: The number of groups in this date. This can be omitted
         (i.e. set to a negative number) if and only if using a fixed-size
         sample. Defaults to -1.
        :param next_row: Move to the next row in the progress table if this is
         not the first date. Defaults to False.
        :return: The row number of this date in the progress table.
        :raises ValueError: If `groups` is 0 when not using a fixed-size sample.
        """

        _log.debug(f'Scanning date "{date_str}"...')

        if groups < 0 and not self._fixed_sample:
            raise ValueError(
                f"Must provide a group count for a new date when "
                f"not using a fixed-size sample, but got {groups}"
            )

        # Go to next row if enabled, and this isn't the first date
        if next_row and self._dates_remaining < self._total_dates:
            self._table.next_row()

        self._table['Date'] = date_str
        self._table_index[date_str] = row = self._table.num_rows() - 1

        self._photos_in_date = 0
        self._dates_remaining -= 1

        if groups >= 0:
            self._groups_in_date = self._groups_remaining = groups
            self._total_groups += groups
        else:
            self._groups_in_date = -1  # Indicate that the count is unknown
            self._groups_remaining = 0

        # Update averages and estimates unless only if not using a fixed sample
        if not self._fixed_sample:
            self._avg_per_group = self._avg_per_date / groups

            # Update the estimated number of groups
            self._est_total_groups = (
                    self._total_groups / (
                    self._total_dates - self._dates_remaining) *
                    self._total_dates
            )

        # Return row number in progress table
        return row

    def _start_group(self, group_str: str) -> None:
        """
        This is called by the scanner every time it starts scanning a new group.

        If a group count was not given in `_start_date()`, the total group count
        is incremented by 1 now.

        :return: None
        """

        _log.debug(f'Scanning group "{group_str}"...')
        self._table['Groups'] = 1

        self._photos_in_group = 0

        # Decrease groups remaining counter, unless it was never set
        if self._groups_remaining > 0:
            self._groups_remaining -= 1

        # Increment group count if not given earlier
        if self._groups_in_date < 0:
            self._total_groups += 1

        # Update the ratio of total groups processed unless using a fixed sample
        if not self._fixed_sample:
            self._est_group_ratio = (
                    (self._total_groups - self._groups_remaining) /
                    self._est_total_groups
            )

    def _next_photo(self, invalid: bool = False,
                    row: int | None = None) -> bool:
        """
        This is called by the scanner every time a photo is scanned. It
        increments the appropriate counters.

        :param invalid: Whether this photo file turned out to be invalid (i.e.
         it's not parseable with RawPy/LibRaw). This saves a separate call to
         `invalid_photo()`. Defaults to False.
        :param row: The row number in the table to update. This can only be
         used when conducting a fixed sample. Defaults to None.
        :return: True if (a) using a fixed-size sample, and (b) the sample
         size was reached with this photo (meaning it's time to stop
         scanning); otherwise False.
        :raises ValueError: If a row number is given when not using a fixed
         sample.
        """

        self._total_files += 1

        if row is not None and not self._fixed_sample:
            raise ValueError(f"Can't specify row number unless using a "
                             f"fixed-size sample, but got {row}")

        # If it's invalid, increment the invalid counter, and then exit:
        # no need to update the progress bar in this case
        if invalid:
            with self._invalid_counter_lock:
                self._invalid_files += 1

            if row is None:
                self._table['Other Files'] = 1
            else:
                self._table.update('Other Files', 1, row=row)

            return False

        # Increment the Photos counter in the progress table
        if row is None:
            self._table['Photos'] = 1
        else:
            self._table.update('Photos', 1, row=row)

        # Increment the progress bar if managed internally
        if not self._externally_managed_pbar:
            self._pbar.update()

        # In a fixes-size sample, exit here: what follows is simply checking
        # the estimate of the total number of photos, which is already known
        # when taking a sample
        total = self.total_photos
        if self._fixed_sample:
            if total == self._estimate:
                return True
            return False

        # If the count now exceeds the progress bar total, recalculate the
        # estimate assuming there are 10 more photos in this group
        if total > self._estimate:
            self._recalculate_and_update_estimate(ghost_inc=10,
                                                  finished_group=False)

        # If the progress bar has reached a higher percentage than the percent
        # of groups processed out of the expected number, increase average
        # photos per group by 25%
        if total / self._estimate > self._est_group_ratio:
            self._avg_per_group *= 1.25
            self._recalculate_and_update_estimate(finished_group=False)

        return False

    def _end_group(self) -> None:
        """
        This is called by the scanner every time it finishes scanning a group.

        :return: None
        """

        self._photos_in_date += self._photos_in_group

        # If using a fixed sample, no need to update averages/estimates
        if self._fixed_sample:
            return

        self._avg_per_group = update_estimate(
            prior=self._avg_per_group,
            n_total=self._groups_in_date,
            n_remaining=self._groups_remaining,
            observed=self._photos_in_group
        )

        # If this was the first group that just finished, replace the initial
        # fixed estimate with the data from this group
        if self._dates_remaining == self._total_dates and \
                self._groups_remaining + 1 == self._groups_in_date:
            self._avg_per_group = self._photos_in_group
            self._avg_per_date = self._avg_per_group * self._groups_in_date
            self._set_estimated_photo_count(
                self._avg_per_date * self._total_dates)
        else:
            # Otherwise refine estimates to update the progress bar(s)
            self._recalculate_and_update_estimate()

    def _end_date(self) -> None:
        """
        This is called by the scanner every time it finishes scanning a date.

        :return: None
        """

        # Update the photos-per-date estimate, unless using a fixed sample
        if not self._fixed_sample:
            self._avg_per_date = update_estimate(
                prior=self._avg_per_date,
                n_total=self._total_dates,
                n_remaining=self._dates_remaining,
                observed=self._photos_in_date
            )

        # Next date in the table
        self._table.next_row()

    def _end(self) -> None:
        """
        Finished scanning. Unless using a fixed sample size, update the
        final value for the progress bar total to the total number of files,
        so it shows 100% complete.

        If progress bar updates are enabled (meaning they aren't being managed
        externally), this also closes the progress table.

        :return: None
        """

        # Set final estimate, unless already set for a fixed sample
        if not self._fixed_sample:
            self._set_estimated_photo_count(self._total_files)

        if self._externally_managed_pbar:
            # If the progress bar is managed externally, start showing
            # progress (if not already), as we now know the final total now
            self._pbar.show_progress = True
        else:
            # If managed internally, everything is done. Close the progress
            # table now
            self._table.close()

    def _set_estimated_photo_count(self, total: int) -> None:
        """
        Set the estimated total number of photos. This also updates the
        progress bar.

        :param total: The new estimate.
        :return: None
        """

        self._estimate = total
        self._pbar.set_total(total)

    def _recalculate_and_update_estimate(self, *,
                                         ghost_inc: int = 0,
                                         finished_group: bool = True) -> None:
        """
        Recalculate the estimated total number of photos, and update the
        estimate and progress bar accordingly.

        :param ghost_inc: Additional photos to add to the estimate. This is
         useful as a temporary measure within a group if the estimate is
         proving to be too low. Defaults to 0.
        :param finished_group: Whether a group just finished (True) or we're
         currently scanning a group (False). This is normally called in the
         former case. Defaults to True.
        :return: None
        """

        # Get the (possibly adjusted) average photos per group
        if ghost_inc > 0:
            refined_avg_per_group = update_estimate(
                prior=self._avg_per_group,
                n_total=self._groups_in_date,
                n_remaining=self._groups_remaining,
                observed=self._photos_in_group + ghost_inc
            )
        else:
            refined_avg_per_group = self._avg_per_group

        # Use this to get the number of photos expected from the rest of the
        # groups in this date
        remaining_in_this_date = refined_avg_per_group * self._groups_remaining

        # If we're in the middle of a group, add the number of photos we expect
        # to still find in the active group (unless that'd be negative because
        # we're already over the estimated group average)
        if not finished_group:
            remaining_in_this_date += max(refined_avg_per_group -
                                          self._photos_in_group, 0)

        # Adjust the average-photo-per-date calculation based on the current
        # progress in the current date
        refined_avg_per_date = update_estimate(
            prior=self._avg_per_date,
            n_total=self._total_dates,
            n_remaining=self._dates_remaining,
            observed=self._photos_in_date + remaining_in_this_date
        )

        # Compute and set the final estimate
        self._set_estimated_photo_count(int(
            self._total_files + ghost_inc +
            refined_avg_per_date * self._dates_remaining +
            remaining_in_this_date
        ))

    def log_summary(self,
                    sample: bool,
                    random: bool = False, *,
                    finished: bool = True) -> None:
        """
        Log a message with total summary statistics.

        :param sample: Whether this was only a sample. This is assumed to be
         True if `random` is True.
        :param random: Whether the sample was randomized. Defaults to False.
        :param finished: Whether all processing on the photos yielded by the
         scanner has finished. This affects the invalid_file counter and
         thus the total photo count: if not finished, it's possible that some
         more files may be marked invalid. Defaults to True.
        :return: None
        """

        # Log a warning message if there weren't any photos at all
        if self.total_photos == 0:
            if self._fixed_sample:
                # If using a sample, add that
                text = (
                    f"Unable to {'randomly ' if random else ''}sample "
                    f"{self._estimate} "
                    f"photo{'' if self._estimate == 1 else 's'}: "
                    f"couldn't find any photos."
                )
            else:
                text = "Couldn't find any photos."

            # Add date and group counts
            if self._total_dates == 0:
                text += ' Found 0 date directories.'
            else:
                if self._total_groups == 0:
                    dt_empty, gp_empty = ' empty', ''
                else:
                    dt_empty, gp_empty = '', ' empty'

                text += (
                    f" Found {self._total_dates}{dt_empty} date director"
                    f"{'y' if self._total_dates == 1 else 'ies'} "
                    f"with {self._total_groups}{gp_empty}"
                    f"group{'' if self._total_groups == 1 else 's'}"
                )
            _log.warning(text)
            return

        # Build a string with the number of photos/files. Use the file count
        # if not finished, as the final photo count is unknown
        if finished:
            total = self.total_photos
            photos_str = f"{total:,} photo{'' if total == 1 else 's'}"
        else:
            total = self._total_files
            photos_str = f"{total:,} file{'' if total == 1 else 's'}"

        # Build a string with the number of groups and dates
        group_date_str = (
            f"from {self.scanned_groups} "
            f"group{'' if self.scanned_groups == 1 else 's'} in "
            f"{self.scanned_dates} "
            f"date{'' if self.scanned_dates == 1 else 's'}"
        )

        # Log a different warning message if using a fixed-sample, and the
        # sample size wasn't met
        if self._fixed_sample and total < self._estimate:
            # noinspection SpellCheckingInspection
            _log.warning(
                f"{'Randomly s' if random else 'S'}ampled {photos_str} "
                f"{group_date_str}. Unable to meet desired sample size of "
                f"{self._estimate} photo{'' if self._estimate == 1 else 's'}"
            )
            return

        # Log an info message with basic summary statistics
        if random:
            text = 'Got randomized sample'
        elif sample:
            text = 'Got deterministic sample'
        else:
            text = 'Found a total'

        # Log the regular info message
        _log.info(f"{text} of {photos_str} {group_date_str}")

    def debug_info(self) -> str:
        """
        Get a string with information about the scan metrics for debugging
        purposes. This includes the values of important counters.

        This is intended for use during a fatal error to log some information
        to the console. It avoids acquiring any locks (even where they would
        normally be used) to ensure it does not deadlock.

        :return: A string with debug info.
        """

        # Shorten strings, and lock in values (in case they change ig)
        e = self._estimate
        f = self._total_files
        p = f - self._invalid_files
        g = self._total_groups
        sg = g - self._groups_remaining
        d = self._total_dates
        sd = d - self._dates_remaining

        # If the number of photos and files scanned so far are the same, just
        # list that as the "photo" count. On the other hand, if they're
        # different, it means that some of the scanned files were not photos
        # recognized by rawpy. In that case, separately list the counts of
        # files and photos.
        if f == p:
            photo_str = f"photo{'' if e == 1 else 's'}"
        else:
            photo_str = (f"file{'' if e == 1 else 's'} "
                         f"({p} photo{'' if p == 1 else 's'})")

        # Assemble the debug string. The final format looks like:
        # "78/~268 files (75 photos) from 7/8 groups in 4/7 dates"
        # Or a shorter form for a fixed-size sample and no invalid photos:
        # "12/100 files from 1/1 group in 1/3 dates"
        return (
            f"{f}/{'~' if e - f != 0 and not self._fixed_sample else ''}{e} "
            f"{photo_str} from {sg}/{g} group{'' if g == 1 else 's'} "
            f"in {sd}/{d} date{'' if d == 1 else 's'}"
        )
