import logging
from pathlib import Path

from progress_table import ProgressTable
from progress_table.v1.progress_table import TableProgressBar

from tlmerge.scan import ScanMetrics

_log = logging.getLogger(__name__)


class PreprocessingMetrics(ScanMetrics):
    def __init__(self,
                 table: ProgressTable,
                 pbar: TableProgressBar,
                 initial_avg_photos_per_date: float | None = None,
                 **kwargs) -> None:
        """
        Initialize a `PreprocessingMetrics` instance, a wrapper around
        `ScanMetrics` that records additional preprocessing-specific metrics.

        :param table: The progress table to update.
        :param pbar: A progress bar in the table.
        :param initial_avg_photos_per_date: See `ScanMetrics.__init__()`. If
         this is None, the default `ScanMetrics` value is used. Defaults
         to None.
        :param kwargs: Additional arguments to pass to `ScanMetrics`.
        :return: None
        """

        if initial_avg_photos_per_date is not None:
            kwargs['initial_avg_photos_per_date'] = initial_avg_photos_per_date

        super().__init__(table, pbar, externally_managed_pbar=True, **kwargs)

        # Counters
        self._preprocessed: int = 0
        self._new_photos: int = 0
        self._updated_photos: int = 0
        self._errors: int = 0

    @classmethod
    def def_progress_table(cls, *, sample_size: int = -1) -> \
            tuple[ProgressTable, TableProgressBar]:
        """
        Create a new ProgressTable and associated progress bar designed to
        work with `PreprocessingMetrics`. The table includes additinal columns
        beyond those required for `ScanMetrics`.

        :param sample_size: Set the sample size if conducting a sample. If
         this is a positive integer, the progress bar shows exact progress,
         and the total is set to the sample size. Use any negative number 
         to indicate no sample. Defaults to -1.
        :return: A new progress table and associated progress bar.
        :raises ValueError: If the sample size is 0.
        """

        table, pbar = super().def_progress_table(
            pbar_label='Preprocessing...',
            sample_size=sample_size
        )

        table.add_column('Errors', width=6,
                         color='lightred_ex', aggregate='sum')
        table.add_column('[DB] New', width=8,
                         color='lightblue_ex', aggregate='sum')
        table.add_column('[DB] Updated', width=12,
                         color='lightblue_ex', aggregate='sum')

        return table, pbar

    def preprocessed_photo(self,
                           date_str: str,
                           is_new: bool = False,
                           is_updated: bool = False) -> None:
        """
        Increment the appropriate counter for a new photo after it has been
        preprocessed. Also increment the progress bar.

        :param date_str: The name of the date directory containing the photo.
        :param is_new: Whether the photo is new, meaning it's not already in
         the database. Defaults to False.
        :param is_updated: Whether the photo was already in the database and
         was updated in some way. Defaults to False.
        :return: None
        :raises ValueError: If `is_new` and `is_updated` are both True.
        """

        row = self.get_row(date_str)

        # Update total counter
        self._preprocessed += 1

        # Update counter
        if is_new:
            if is_updated:
                raise ValueError("Photo can't be both new and updated")
            self._new_photos += 1
            self.table.update('[DB] New', 1, row=row)
        elif is_updated:
            self._updated_photos += 1
            self.table.update('[DB] Updated', 1, row=row)

        # Increment the progress bar
        self.pbar.update()

    def log_error(self, error: Exception, rel_path: str) -> None:
        """
        Log a message indicating that the given photo failed to process, and
        increment the errors counter in the progress table.

        :param error: The exception raised while processing the photo.
        :param rel_path: The relative path to the photo file that triggered
         the exception.
        :return: None
        """

        # Log error message
        _log.error(f'{error.__class__.__name__} processing "{rel_path}": '
                   f' {error}')

        # Increment errors counter
        self._errors += 1
        self.table.update(
            'Errors',
            1,
            row=self.get_row(Path(rel_path).parent.parent.name)
        )

    def log_preprocessing_summary(self) -> None:
        """
        Log summary statistics about the preprocessing stage (not the scanning
        stage).

        :return: None
        """

        n = self._new_photos
        u = self._updated_photos
        e = self._errors
        p = self.total_photos
        c = n + u

        if c == 0:
            _log.info(f"No changed detected: {e} error{'' if e == 1 else 's'} "
                      f"while preprocessing {p} photo{'' if p == 1 else 's'}")
        else:
            _log.info(f"Saved {c} change{'' if c == 1 else 's'} to database: "
                      f"{n} new photo{'' if n == 1 else 's'} and "
                      f"{u} updated record{'' if u == 1 else 's'} with "
                      f"{e} error{'' if e == 1 else 's'}; scanned a total of "
                      f"{p} photo{'' if p == 1 else 's'}")

    def debug_info(self) -> str:
        """
        Get a string with information about the scan and preprocessing metrics
        for debugging purposes. This includes the values of important counters.

        This is intended for use during a fatal error to log some information
        to the console. It avoids acquiring any locks (even where they would
        normally be used) to ensure it does not deadlock.

        :return: A string with debug info.
        """

        e = self._errors

        return (
            f"scanned {super().debug_info()}: {self._preprocessed} "
            f"preprocessed with {self._new_photos} new, "
            f"{self._updated_photos} to update, "
            f"and {e} error{'' if e == 1 else 's'}"
        )
