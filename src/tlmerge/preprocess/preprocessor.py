import logging
from pathlib import Path
from queue import Empty, Queue
from threading import Event, local

import imageio.v3 as iio
import numpy as np
import rawpy
# noinspection PyUnresolvedReferences
from rawpy import (LibRawError, LibRawFileUnsupportedError, LibRawIOError,
                   RawPy, ThumbFormat)
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from tlmerge.conf import buffer_console_log, ConfigManager, RootConfig
from tlmerge.db import DB, Photo
from tlmerge.scan import enqueue_thread
from tlmerge.utils import WorkerPool, WorkerPoolExceptionGroup
from .exif import ExifWorker
from .metadata import PhotoMetadata
from .metrics import PreprocessingMetrics

_log = logging.getLogger(__name__)


class Preprocessor:
    """
    The preprocessing stage of tlmerge is where all the photo files are indexed
    in the database, along with lots of helpful metadata.
    """

    # This max queue size caps the size of all three preprocessing queues. This
    # avoids a possible memory issue if (a) there are tens of thousands of
    # photos, and (b) a bottleneck somewhere (such as with the database worker)
    # leads to some queues filling up quickly
    QUEUE_MAX_SIZE: int = 100

    def __init__(self, config: ConfigManager) -> None:
        """
        Initialize the preprocessor.

        :param config: The `tlmerge` configuration.
        :return: None
        """

        # Get the root config
        self.config: ConfigManager = config
        self.root_cfg: RootConfig = config.root

        # Local storage for each worker thread; used to access the exif worker
        self._thread_data = local()

        # Cancel event to signal scanning and database workers to stop in the
        # event of an error in the photo preprocessing pool
        self.cancel_event: Event = Event()

        # The scanning queue with incoming paths and the worker thread that
        # populates it
        self._scanning_queue: Queue[Path | None] = Queue(
            maxsize=self.QUEUE_MAX_SIZE
        )

        # Define the results queue that receives metadata for each photo and
        # the worker pool that obtains that metadata
        self._metadata_queue: Queue[PhotoMetadata] = Queue(
            maxsize=self.QUEUE_MAX_SIZE
        )
        self._photo_worker_pool = WorkerPool(
            max_workers=self._determine_pool_worker_count(),
            name_prefix='prp-wkr-',
            results=self._metadata_queue,
            on_close_hook=self._close_exif_worker,
            error_handler=self._handle_metadata_error,
            task_queue_size=self.QUEUE_MAX_SIZE
        )

        # This is a counter for tracking the number of consecutive times
        # _apply_metadata() has timed out for the purposes of logging warnings
        self._consecutive_metadata_queue_timeouts: int = 0

        # Summary statistics
        self._metrics: PreprocessingMetrics | None = None

        # This dict stores database photo records from recently scanned photos
        # that are waiting for metadata to load. Keys are the relative paths
        # to the file from within the project dir
        self._enqueued_photos: dict[str, Photo] = {}

    @property
    def _exif_worker(self) -> ExifWorker:
        """
        Get the ExifWorker for the calling thread. If there is no such worker,
        create one first.

        :return: The ExifWorker for this thread.
        """

        if not hasattr(self._thread_data, 'exif'):
            self._thread_data.exif = ExifWorker()
        return self._thread_data.exif

    def _close_exif_worker(self) -> None:
        """
        Close the ExifWorker associated with the calling thread (if one exists),
        and remove it from the thread data storage.

        :return: None
        """

        if hasattr(self._thread_data, 'exif'):
            self._thread_data.exif.close()
            del self._thread_data.exif

    def _determine_pool_worker_count(self) -> int:
        """
        Determine the number of workers to use in the photo worker pool (not
        counting the database worker).

        If the user runs tlmerge with 5 workers, 1 is reserved for the
        scanner, and this method returns 4.

        This logs a message indicating the total number of workers (i.e. one
        more than the number returned by this method).

        It's possible that the total worker count will be different from what
        the user selected. If the user chose 1 worker, this will use 2 (one
        for scanning). If the user chose `w` workers while specifying a
        sample of `s` photos, and `s < w - 1`, then this reduces the worker
        count: the extra workers won't have any photos to process and are
        thus unnecessary.

        If the worker count is changed, an explanation is included in the log
        message.

        :return: The number of workers to use in the photo preprocessing pool.
        """

        sample, s_random, s_size = self.root_cfg.sample_details()
        cfg_workers = self.root_cfg.workers()

        if cfg_workers < 2:
            # Need at least 2 to account for the scanner thread
            workers = 2
        elif 2 <= s_size + 1 < cfg_workers:
            # Don't need more than the sample size + 1
            workers = s_size + 1
        else:
            workers = cfg_workers

        # Build and print the log message explaining what happened

        paren = ''
        if sample:
            photos = (f"a{' random' if s_random else ''} sample of "
                      f"{s_size} photo{'' if s_size == 1 else 's'}")
            if workers < cfg_workers:
                # The number of workers was decreased to sample size + 1
                extra = cfg_workers - workers
                paren = (f" (one reserved for file scanner; "
                         f"{extra} extra worker{'' if extra == 1 else 's'} "
                         f"not used)")
        elif cfg_workers < 2:
            # No sample; worker count increased to 2
            photos = 'photos'
            paren = (' (minimum 2 workers required for file scanning '
                     'and metadata extraction)')
        else:
            # No sample; worker count didn't change
            photos = 'all photos'

        # Assemble the parts, and log the message
        _log.info(f"Preprocessing {photos} with {workers} workers{paren}")

        # Return the number of workers strictly for the photo processing pool
        return workers - 1

    def run(self) -> None:
        """
        Run the complete preprocessing step, loading metadata for each photo
        and saving it to the database.

        :return: None
        """

        proj_name = self.root_cfg.project()
        _log.info(f'Scanning "{proj_name}" (this may take a while)')

        try:
            # Open database session, and run preprocessing
            with DB.session() as session:
                self._preprocess_all_photos(session)

            # Log an error message if it failed
            if self.cancel_event.is_set():
                _log.critical(
                    f'Failed to preprocess photos in {proj_name}: '
                    'execution canceled abruptly due to error(s)'
                )
        except WorkerPoolExceptionGroup as exc_pool:
            self.cancel_event.set()
            # Log the error(s)
            _log.critical(
                f"Failed to preprocess photos in {proj_name}: "
                f"got {exc_pool.summary()}",
                exc_info=True
            )
        except BaseException as e:
            self.cancel_event.set()
            # Re-raise a fatal error (MemoryError or strictly BaseException)
            if isinstance(e, MemoryError) or not isinstance(e, Exception):
                raise

            # Unexpected other exception
            _log.critical(
                f'Failed to preprocess photos in {proj_name}: '
                f'got unexpected {e.__class__.__name__}',
                exc_info=True
            )

    def _preprocess_all_photos(self, session: Session) -> None:
        """
        This function, executed on the main thread by `self.run()`, coordinates
        the movement of photos through the queues and updates the records in
        the database.

        :param session: An already-open open database session.
        :return: None
        :raises BaseException: If the preprocessing step fails in any way.
        """

        # Initialize the metrics, progress table, and progress bar
        table, pbar = PreprocessingMetrics.def_progress_table(
            sample_size=self.root_cfg.sample_size()
        )
        self._metrics = PreprocessingMetrics(table, pbar)

        # Buffer log messages to not interfere with the progress table
        buffer = buffer_console_log()
        try:
            buffer.start()

            # Start the scanner
            enqueue_thread(
                output=self._scanning_queue,
                metrics=self._metrics,
                config=self.config,
                name='prp-scn-wkr',
                cancel_event=self.cancel_event
            )

            # Start the preprocessing worker pool
            self._photo_worker_pool.start()

            # FIRST LOOP: alternate between checking scanner for new photo
            # files and checking worker pool for new metadata, repeating until
            # scanner is exhausted
            while self._enqueue_next_file(session):
                # Get next metadata from the preprocessing worker pool
                self._apply_metadata(session)

            _log.debug('Finished scanning for photos. Now preprocessing '
                       'any remaining photos in queue')

            # Close the worker pool: no more tasks to add
            self._photo_worker_pool.close()

            # SECOND LOOP: process all remaining metadata records
            while self._apply_metadata(session):
                pass

            _log.debug('Finished preprocessing. Performing cleanup...')
        except BaseException as e:
            _log.warning('Preprocessing stopped with '
                         f'{e.__class__.__name__}. Debug info:')

            # Log preprocessing debug info
            _log.warning(self._metrics.debug_info())

            # Log worker pool debug info
            n = self._photo_worker_pool.error_count
            w = self._photo_worker_pool.worker_count
            _log.warning(f"Worker pool: {self._photo_worker_pool.state} "
                         f"with {w} active worker{'' if w == 1 else 's'} "
                         f"and {n} error{'' if n == 1 else 's'}")

            # Reraise
            raise
        finally:
            _log.debug('Closing log progress table and releasing buffer...')

            # Close the progress table
            table.close()

            # Release the log buffer
            buffer.release()

        # Commit db changes
        _log.debug('Committing db changes...')
        session.commit()

        # Log results
        self._metrics.log_preprocessing_summary()

    def _enqueue_next_file(
            self,
            session: Session) -> bool:
        """
        Get the next file from the scanner queue. Send it to the preprocessing
        worker pool to get metadata, and load the corresponding Photo record
        from the database.

        If the queue is empty, do nothing.

        :param session: The current database session.
        :return: False if and only if the scanner finished, and all incoming
         photo files have been submitted for preprocessing.
        """

        # Get the next file from the scanner. If there aren't any more files,
        # exit here
        try:
            file: Path | None = self._scanning_queue.get_nowait()
        except Empty:
            # Check again on next iteration, as the scanner may add another
            # photo file by then
            return True

        # If the file is None, that's the signal that the scanner finished
        if file is None:
            return False

        # This is an identifier string for the file
        rel_path: str = str(self.root_cfg.rel_path(file))
        _log.debug(f'Checking db for "{rel_path}"...')

        # Load the photo from the database
        try:
            date, group, file_name = file.parts[-3:]
            db_photo = session.get(Photo, (date, group, file_name))

            # If the photo isn't in the database yet, make a new record
            if db_photo is None:
                _log.debug('Creating new db record for '
                           f'"{self.root_cfg.rel_path(file)}"...')
                db_photo = Photo(
                    date=date,
                    group=group,
                    file_name=file_name
                )

            # Save this db photo record until its metadata finishes loading
            self._enqueued_photos[rel_path] = db_photo
        except SQLAlchemyError as e:
            _log.error(f'Error accessing database record for '
                       f'"{self.root_cfg.rel_path(file)}": {e}')
            raise

        # Send the photo to the preprocessing worker pool to load its metadata
        _log.debug(f'Sending "{rel_path}" preprocessing task to worker...')
        self._photo_worker_pool.add(self._load_metadata, rel_path, file)

        return True

    def _apply_metadata(self,
                        session: Session,
                        timeout: float = 0.1) -> bool:
        """
        Get the next PhotoMetadata record from the preprocessing workers. Apply
        any changes to the corresponding database record, and flush those
        changes to the database (without committing yet).

        If the metadata queue is empty, do nothing.

        :param session: The current database session.
        :param timeout: The number of seconds to block while waiting to get
         a value from the queue. Defaults to 0.1.
        :return: False if and only if (a) the worker pool finished and (b)
         there are no more metadata records to process.
        """

        # Get the next metadata record from the worker pool
        try:
            metadata: PhotoMetadata = self._metadata_queue.get(timeout=timeout)
        except Empty:
            # We're done when the worker pool is finished (meaning the workers
            # are done getting metadata from photos) *AND* the queue is totally
            # empty, meaning this main thread has finished saving changes to
            # the database. It's probably important that the empty() check
            # comes second in this boolean after is_finished(). Re-checking
            # empty() here after getting an Empty exception might mitigate a
            # race condition where the last metadata record was just added
            # after the get() call timed out.
            if self._photo_worker_pool.is_finished() and \
                    self._metadata_queue.empty():
                # If done, return False (the "done" signal)
                return False

            # Update the counter of consecutive timeouts
            self._consecutive_metadata_queue_timeouts = \
                t = self._consecutive_metadata_queue_timeouts + 1

            # If we've hit 10 seconds of consecutive timeout, log a warning
            # (making sure to only do this once). Do the same at 30 seconds,
            # and log additional debug info at 1, 2, 3, and 4 minutes. After 5
            # minutes, raise an error.
            if 0 <= t * timeout - 10 < timeout or 0 <= t * timeout - 30 < timeout:
                _log.warning(
                    f'Preprocessor main thread stalled {t * timeout:.1f} '
                    f'seconds ({t} iterations) while waiting for the next '
                    'photo from the metadata queue'
                )
            elif 0 <= t * timeout - 300 < timeout:
                # Exit after 5 minutes of timeouts. Include a list of the remaining
                # enqueued photos in the error message
                photos = list(self._enqueued_photos.keys())
                p = len(photos)
                if p == 0:
                    p_str = '0 enqueued photos remain'
                else:
                    photos = photos[:10]
                    p_str = (
                        f"{p} enqueued photo{'' if p == 1 else 's'} remain: "
                        f"{', '.join(photos)}{', ...' if p > 10 else ''}"
                    )

                raise RuntimeError(
                    "Forcibly terminating after preprocessor main thread "
                    f"stalled for {t * timeout:.1f} seconds while waiting on "
                    f"the metadata queue; {p_str}"
                )
            elif t * timeout >= 60 and (t * timeout) % 60 < timeout:
                q = self._metadata_queue.qsize()
                if q == 0:
                    q = 'empty'
                else:
                    q = f"contains ~{q} record{'' if q == 1 else 's'}"

                _log.warning(
                    "Preprocessor main thread remains stalled after "
                    f"{t * timeout:.1f} seconds. Worker pool "
                    f"{self._photo_worker_pool.progress_str()}; queue {q}"
                )

            # Continue without "done" signal
            return True

        # Got a value. Reset the timeout counter to 0
        self._consecutive_metadata_queue_timeouts = 0

        # Find the photo associated with this metadata
        path_str = metadata.path_str()
        db_photo = self._enqueued_photos.pop(path_str)

        ##################################################
        # Apply the metadata, and flush changes to DB (but don't commit yet)

        _log.debug(f'Applying metadata to db record for {path_str}')

        try:
            # Apply the metadata for the photo
            metadata.apply_photo_metadata(db_photo)

            # Check whether this record is new or already in the database
            if inspect(db_photo).transient:
                # For a new record, get a Lens and Camera based on the
                # metadata. If there is already a matching Lens/Camera in the
                # db, use that. If not, make new records
                if (camera_id := metadata.get_camera_id(session)) is not None:
                    db_photo.camera_id = camera_id
                else:
                    db_photo.camera = metadata.create_camera()
                    _log.debug('Creating new camera record for '
                               f'{metadata.camera_str()}')

                # Same for lens
                if (lens_id := metadata.get_lens_id(session)) is not None:
                    db_photo.lens_id = lens_id
                else:
                    db_photo.lens = metadata.create_lens()
                    _log.debug('Creating new lens record for '
                               f'{metadata.lens_str()}')

                # Add the new Photo record to the session
                session.add(db_photo)

                # Update metrics
                self._metrics.preprocessed_photo(metadata.date, is_new=True)
            else:
                # For an existing record, if the Lens or Camera data changed,
                # replace them with new records. That way other photos linking
                # to the original Camera/Lens aren't inadvertently changed too
                if not metadata.matches_camera(db_photo.camera):
                    db_photo.camera = metadata.create_camera()
                if not metadata.matches_lens(db_photo.lens):
                    db_photo.lens = metadata.create_lens()

                # Update metrics
                self._metrics.preprocessed_photo(
                    metadata.date,
                    is_updated=session.is_modified(db_photo)
                )

            # Flush changes to save them (but don't commit yet)
            _log.debug(f'Flushing db changes (without committing)...')
            session.flush()
        except SQLAlchemyError as e:
            _log.error('Error creating/updating database record for '
                       f'"{metadata.path_str()}": {e}')
            raise

        return True

    def _load_metadata(self, file: Path) -> PhotoMetadata:
        """
        Load all the relevant metadata for a photo to create/update its
        database record. This includes data from both PyExifTool and RawPy.

        If the image is not valid (that is, it can't be processed by RawPy),
        this logs a warning and returns the relative file path.

        :param file: The path to the photo file.
        :return: All the relevant, available metadata.
        :raises LibRawError: If something goes wrong with RawPy/LibRaw.
        """

        path_str = self.root_cfg.rel_path(file)
        _log.debug(f'Loading metadata for "{path_str}"...')

        # Create the metadata data object for storing all the values
        metadata = PhotoMetadata(*file.parts[-3:])

        # Open the photo in RawPy (i.e. LibRaw) to get more info. Do this first
        # to make sure it's a valid raw file
        _log.debug(f'"{path_str}" in RawPy...')
        with rawpy.imread(str(file)) as rpy_photo:
            _apply_libraw_metadata(rpy_photo, metadata)

        # Extract and record the EXIF data
        _log.debug(f'Extracting EXIF from "{path_str}"...')
        self._exif_worker.extract(file, self.config).record_metadata(metadata)

        # Return the complete metadata object
        return metadata

    def _handle_metadata_error(self, error: Exception, rel_path: str) -> bool:
        """
        Handle an exception raised by `_load_metadata()` when run in the
        worker pool. If it's not a problem, return True to ignore it.
        Otherwise, log the error, and return False.

        :param error: The exception.
        :param rel_path: This is the identifier string given by the worker
         pool for identifying the task. It should be the relative path to
         the photo that failed.
        :return: True if and only if the exception is handled successfully.
        """

        # For invalid files that can't be read by RawPy/LibRaw, just update
        # the metrics
        if isinstance(error, LibRawFileUnsupportedError) or \
                isinstance(error, LibRawIOError):
            self._metrics.invalid_photo_file(
                date_str=Path(rel_path).parts[0]
            )

            # Delete the metadata record for this photo to avoid a memory leak
            if self._enqueued_photos.pop(rel_path, None) is None:
                _log.warning(f"Unexpected: couldn't find enqueued db photo "
                             f"record matching \"{rel_path}\" to delete it")

            # Successfully handled error
            return True

        # This exception can't be handled. Log it, and return False
        self._metrics.log_error(error, rel_path)
        return False


def _apply_libraw_metadata(rpy_photo: RawPy,
                           metadata: PhotoMetadata) -> None:
    """
    Given a photo opened by rawpy (i.e. libraw), apply information from it to
    the database record.

    :param rpy_photo: The photo opened in RawPy.
    :param metadata: The photo metadata object.
    :return: None
    """

    # Get the image crop area size (i.e. not the raw size)
    sizes = rpy_photo.sizes
    metadata.width = sizes.width
    metadata.height = sizes.height

    # Extract the thumbnail to get its size
    try:
        thumb = rpy_photo.extract_thumb()
        if thumb.format == ThumbFormat.JPEG:
            thumb = iio.imread(thumb.data)
        elif thumb.format == ThumbFormat.BITMAP:
            thumb = thumb.data
        else:
            raise ValueError(f'Unknown thumbnail format "{thumb.format}"')
        metadata.thumb_width, metadata.thumb_height = thumb.shape[:2]
    except LibRawError:
        # Thumbnail size params are optional. If it's not found, that's fine
        pass

    # Set white balance at the time of capture
    try:
        r, g1, b, g2 = rpy_photo.camera_whitebalance
        metadata.capture_wb_red = r
        metadata.capture_wb_green1 = g1
        metadata.capture_wb_blue = b
        metadata.capture_wb_green2 = g2
    except LibRawError:
        pass

    # Set camera daylight white balance, if available
    day_r, day_g1, day_b, day_g2 = 1, 1, 1, 1  # Used later
    try:
        day_r, day_g1, day_b, day_g2 = rpy_photo.daylight_whitebalance
        metadata.camera_daylight_wb_red = day_r
        metadata.camera_daylight_wb_green1 = day_g1
        metadata.camera_daylight_wb_blue = day_b
        metadata.camera_daylight_wb_green2 = day_g2
    except LibRawError:
        pass

    # Set photo black levels (i.e. darkness)
    r, g1, b, g2 = rpy_photo.black_level_per_channel
    metadata.black_level_red = r
    metadata.black_level_green1 = g1
    metadata.black_level_blue = b
    metadata.black_level_green2 = g2

    # Set camera white level (i.e. saturation)
    camera_white_level = rpy_photo.camera_white_level_per_channel
    if camera_white_level is not None:
        r, g1, b, g2 = camera_white_level
    elif (white_level := rpy_photo.white_level) is not None:
        # Fall back to white_level only if camera level isn't found. Otherwise,
        # avoid using this value. See the comment here:
        # https://github.com/letmaik/rawpy/pull/122#issuecomment-692038349
        r, g1, b, g2 = (white_level,) * 4
    metadata.white_level_red = r
    metadata.white_level_green1 = g1
    metadata.white_level_blue = b
    metadata.white_level_green2 = g2

    # Estimate the average red, green, and blue values by processing a half
    # size color image (i.e. no interpolation) with no white balance
    # adjustments and no auto exposure but with the default gamma curve.
    # This can be used later to calculate grey-world white balance.
    image = rpy_photo.postprocess(
        half_size=True, user_wb=[1, 1, 1, 1], no_auto_bright=True
    )
    red_channel = image[:, :, 0].ravel()
    green_channel = image[:, :, 1].ravel()
    blue_channel = image[:, :, 2].ravel()
    metadata.avg_red = float(np.mean(red_channel))
    metadata.avg_green = float(np.mean(green_channel))
    metadata.avg_blue = float(np.mean(blue_channel))

    # Use the same image to estimate the brightness percentiles. Correct each
    # RGB value by the default daylight white balance multipliers. (Use the
    # daylight multipliers instead of image-specific values to keep things
    # constant between images from the same camera).
    brightness = ((red_channel * day_r +
                   green_channel * (day_g1 + day_g2) / 2 +
                   blue_channel * day_b) // 3).astype(np.uint8)

    metadata.brightness_min = int(np.min(brightness))
    metadata.brightness_max = int(np.max(brightness))
    metadata.brightness_mean = float(np.mean(brightness))
    metadata.brightness_stdev = float(np.std(brightness))
    metadata.brightness_iqr = float(
        np.percentile(brightness, 75) - np.percentile(brightness, 25)
    )

    percentiles = np.percentile(brightness, np.arange(10, 100, 10))
    metadata.brightness_p10 = float(percentiles[0])
    metadata.brightness_p20 = float(percentiles[1])
    metadata.brightness_p30 = float(percentiles[2])
    metadata.brightness_p40 = float(percentiles[3])
    metadata.brightness_median = float(percentiles[4])
    metadata.brightness_p60 = float(percentiles[5])
    metadata.brightness_p70 = float(percentiles[6])
    metadata.brightness_p80 = float(percentiles[7])
    metadata.brightness_p90 = float(percentiles[8])
