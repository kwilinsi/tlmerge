from collections import deque
from collections.abc import Generator
import logging
from pathlib import Path
from queue import Queue
from threading import Event, Thread

from tlmerge.conf import ConfigManager, buffer_console_log
from .metrics import ScanMetrics
from . import scan_impl as impl

_log = logging.getLogger(__name__)


def iter_all_dates(config: ConfigManager) -> Generator[Path, None, None]:
    """
    Iterate over all dates, ignoring excluded dates in the configuration.
    The order is not guaranteed.

    :param config: The `tlmerge` configuration.
    :return: A generator yielding the path to every date directory.
    """

    yield from impl.iter_dates(
        config.root.project(),
        config.root.date_format(),
        set()
    )


def iter_all_groups(date_dir: Path,
                    config: ConfigManager) -> Generator[Path, None, None]:
    """
    Iterate over all groups in the specified date directory, ignoring excluded
    groups in the configuration. The order is not guaranteed.

    :param date_dir: The date directory containing the groups.
    :param config: The `tlmerge` configuration.
    :return: A generator yielding the path to every group directory in this
     date.
    """

    yield from impl.iter_groups(date_dir, config=config, scan_all=True)


def iter_photos(metrics: ScanMetrics,
                config: ConfigManager,
                order: bool = False,
                validate: bool = False,
                log_finished: bool | None = None) -> Generator[
    Path, None, None]:
    """
    Iterate over the photos in the project. If a sample is enabled in
    configuration, this only iterates up to the sample size.

    :param metrics: Scanning metrics for tracking progress and summary stats.
    :param config: The `tlmerge` configuration.
    :param order: Whether to yield the photos strictly in order. If conducting
     a deterministic sample via the configuration, this is done implicitly.
     This is ignored if conducting a randomized sample. Defaults to False.
    :param log_finished: Whether to log a message via scan metrics with 
     summary statistics when finished. If True, a message is logged assuming
     that scanning has entirely finished. If False, a message is logged
     assuming that the yielded photos are still processing. If None, nothing
     is logged. Defaults to None.
    :param validate: Whether to validate each file to ensure that it can be
     read by RawPy/LibRaw. Defaults to False.
    :return: A generator yielding the path to every photo in the project (or,
     if using a sample, only a subset of the photos).
    """

    root = config.root
    sample, s_random, s_size = root.sample_details()

    if s_random:
        yield from impl.iter_photos_random(
            metrics=metrics,
            config=config,
            project_root=root.project(),
            date_format=root.date_format(),
            excluded_dates=root.exclude_dates(),
            sample_size=s_size,
            validate=validate
        )
    else:
        yield from impl.iter_photos(
            metrics=metrics,
            config=config,
            project_root=root.project(),
            date_format=root.date_format(),
            excluded_dates=root.exclude_dates(),
            order=order or sample,
            # Yield in order if it's a deterministic sample
            validate=validate,
            sample=s_size
        )

    if log_finished is True:
        metrics.log_summary(sample, s_random, finished=True)
    elif log_finished is False:
        metrics.log_summary(sample, s_random, finished=False)


def enqueue_thread(output: Queue[Path | None] | Queue[Path],
                   metrics: ScanMetrics,
                   config: ConfigManager,
                   name: str = 'scanner',
                   daemon: bool = True,
                   start: bool = True,
                   cancel_event: Event | None = None,
                   none_terminated: bool = True,
                   log_summary: bool = True) -> Thread:
    """
    Scan for all photos on a separate thread, adding them to the given queue.

    :param output: The queue in which to put the photo paths.
    :param metrics: Scanning metrics for tracking progress and summary stats.
    :param config: The `tlmerge` configuration.
    :param name: The thread name. Defaults to 'scanner'.
    :param daemon: Whether to make the thread a daemon. Defaults to True.
    :param start: Whether to start the thread immediately. Defaults to True.
    :param cancel_event: This event is checked every time a new photo is
     added to the queue. If it's set, the thread exits. If no event is
     given, the thread cannot be cancelled gracefully. Defaults to None.
    :param none_terminated: Whether to add None to the queue at the end to
     signal that the scanner is done. Defaults to True.
    :param log_summary: Whether to log scanning summary statistics after
     exhausting the scanner. Defaults to True.
    :return: The Thread doing the scanning. Join it to wait for the
     scan operating to finish.
    """

    def scan() -> None:
        for photo in iter_photos(
                metrics,
                config,
                log_finished=False if log_summary else None
        ):
            # If cancel signal sent, exit this thread
            if cancel_event is not None and cancel_event.is_set():
                return

            # Add this photo the queue
            output.put(photo)

        # Signal done by adding None if enabled
        if none_terminated:
            output.put(None)

    # Create the scanner thread
    thread = Thread(target=scan, name=name, daemon=daemon)

    if start:
        thread.start()

    return thread


def run_scanner(config: ConfigManager) -> None:
    """
    Scan all the files in the timelapse project directory to log summary
    statistics on the number of photos and ensuring everything in the
    scan module is working properly.

    :param config: The `tlmerge` configuration.
    :return: None
    """

    sample, s_random, s_size = config.root.sample_details()

    _log.info(f'Scanning timelapse project "{config.root.project()}" '
              '(this may take some time)')

    table, pbar = ScanMetrics.def_progress_table(sample_size=s_size)
    metrics = ScanMetrics(table, pbar)

    # If sampling, log a message with the target sample size
    if sample:
        _log.info(f"Sampling {s_size}{' random' if s_random else ''} "
                  f"photo{'' if s_size == 1 else 's'}…")

    # Create a generator to iterate over the photos
    generator = iter_photos(
        metrics,
        config,
        order=True,
        validate=True,
        log_finished=True
    )

    with buffer_console_log():
        try:
            # If sampling 10 or fewer photos, log each of them
            if 1 <= s_size <= 10:
                for photo in generator:
                    _log.info(f'Found photo "{photo}"')
            else:
                # Otherwise, quickly exhaust the generator for its side effects.
                # https://stackoverflow.com/a/50938015/10034073
                deque(generator, maxlen=0)
        finally:
            table.close()
