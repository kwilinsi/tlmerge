import logging
from pathlib import Path
from queue import Empty, Queue
from threading import Event, local, Thread

import imageio.v3 as iio
import numpy as np
import rawpy
# noinspection PyUnresolvedReferences
from rawpy import LibRawError, RawPy, ThumbFormat
from PIL import Image

from tlmerge.conf import CONFIG
from tlmerge.db import DB, Camera, Lens, Photo
from tlmerge.scan import Scanner
from .worker_pool import WorkerPool, WorkerPoolExceptionGroup
from .exif import ExifWorker

_log = logging.getLogger(__name__)


class Preprocessor:
    def __init__(self):
        # Determine the number of workers to use
        self.cfg = CONFIG.root
        sample, s_random, s_size = self.cfg.sample_details()
        workers = self.cfg.workers - 1

        if workers < 1:
            # Need at least 2 workers to account for the db writer
            _log.debug('Using minimum 2 workers in preprocessor '
                       '(one for database)')
            workers = 1
        elif 1 <= s_size < workers:
            # If the number of workers is greater than the number of photos
            # we're loading (due to a sample), then reduce the worker count
            # to avoid unused workers
            workers = s_size

        # Local storage for each worker thread; used to access the exif worker
        self._thread_data = local()

        # Log info
        msg = f'Running preprocessor with {workers + 1} workers (1 for db)'
        if sample:
            msg += (f": {'randomly ' if s_random else ''} sampling "
                    f"{s_size} photo{'' if s_size else 's'}")
        _log.info(msg)

        # Database queue, a worker that processes it, and an event to signal
        # that worker to stop
        self.database_queue: Queue[Photo] = Queue()
        self.db_worker: Thread = Thread(
            target=self._run_db_worker,
            name='prp-db-wkr',
            daemon=True
        )
        self.db_end_event: Event = Event()

        # The primary worker pool that processes each photo
        self.photo_worker_pool = WorkerPool(
            max_workers=workers,
            name_prefix='prp-wkr-',
            results=self.database_queue,
            on_close_hook=self._close_exif_worker
        )

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

    def run(self) -> None:
        """
        Run the preprocessing step. This loads all the photos based on the
        program configuration, scans their metadata (and some other info about
        brightness), and saves that information to the database.

        :return: None
        """

        _log.info(f'Preprocessing photos in "{self.cfg.project}" '
                  '(this may take some time)')

        # Start the database worker
        self.db_worker.start()

        # Assign photos to a worker pool, and wait for them to finish processing
        try:
            with self.photo_worker_pool as pool:
                for photo in Scanner().iter_all_photos():
                    pool.add(
                        self.preprocess_photo,
                        str(self.cfg.rel_path(photo)),
                        photo
                    )
        except WorkerPoolExceptionGroup as exc_pool:
            # Log the error(s)
            _log.critical(
                f"Failed to preprocess photos in {self.cfg.project}: "
                f"got {exc_pool.summary()}",
                exc_info=True
            )
        except BaseException as e:
            # Re-raise a fatal error (MemoryError or strictly BaseException)
            if isinstance(e, MemoryError) or not isinstance(e, Exception):
                self.db_end_event.set()
                raise

            # Unexpected other exception
            _log.critical(
                f'Failed to preprocess photos in {self.cfg.project}: '
                f'got unexpected {e.__class__.__name__}',
                exc_info=True
            )
        finally:
            # Signal database thread to stop
            self.db_end_event.set()

        # Wait for DB worker to finish
        self.db_worker.join()

    def _run_db_worker(self) -> None:
        """
        Run this in its on thread as the database worker. It continuously
        queries the database queue until it gets None, at which point it exits.

        :return: None
        """

        while not self.db_end_event.is_set():
            try:
                photo = self.database_queue.get(timeout=0.25)

                # Save to database
                _log.info(f'Saving photo to database: '
                          f'\nPhoto: {photo.__dict__}'
                          f'\nCamera: {photo.camera.__dict__}'
                          f'\nLens: {photo.lens.__dict__}')
            except Empty:
                pass

    def preprocess_photo(self, file: Path) -> Photo:
        """
        Preprocess a single photo.

        :param file: The path to the photo file.
        :return: A new database Photo record.
        """

        _log.info(f'Preprocessing {self.cfg.rel_path(file)}')

        # Create the database record
        photo = Photo()
        photo.camera = Camera()
        photo.lens = Lens()

        # Extract and apply the EXIF data
        self._exif_worker.extract(file).apply_metadata(photo)

        # Open the photo in RawPy (i.e. LibRaw) to get more info
        with rawpy.imread(str(file)) as rpy_photo:
            _apply_libraw_metadata(rpy_photo, photo)

        # Return the finished db Photo record
        return photo


def _apply_libraw_metadata(rpy_photo: RawPy, db_photo: Photo) -> None:
    """
    Given a photo opened by rawpy (i.e. libraw), apply information from it to
    the database record.

    :param rpy_photo: The photo opened in RawPy.
    :param db_photo: The database Photo record.
    :return: None
    """

    # Get the image crop area size (i.e. not the raw size)
    sizes = rpy_photo.sizes
    db_photo.width = sizes.width
    db_photo.height = sizes.height

    # Extract the thumbnail to get its size
    try:
        thumb = rpy_photo.extract_thumb()
        if thumb.format == ThumbFormat.JPEG:
            thumb = iio.imread(thumb.data)
        elif thumb.format == ThumbFormat.BITMAP:
            thumb = thumb.data
        else:
            raise ValueError(f'Unknown thumbnail format "{thumb.format}"')
        db_photo.thumb_width, db_photo.thumb_height = thumb.shape[:2]
    except LibRawError:
        # Thumbnail size params are optional. If it's not found, that's fine
        pass

    # Set camera white balance if available
    cam_wb_r, cam_wb_g1, cam_wb_b, cam_wb_g2 = 1, 1, 1, 1  # Used later
    try:
        cam_wb_r, cam_wb_g1, cam_wb_b, cam_wb_g2 = rpy_photo.camera_whitebalance
        camera: Camera = db_photo.camera
        camera.wb_red = cam_wb_r
        camera.wb_green1 = cam_wb_g1
        camera.wb_blue = cam_wb_b
        camera.wb_green2 = cam_wb_g2
    except LibRawError:
        pass

    # Set photo daylight white balance, if available
    try:
        r, g1, b, g2 = rpy_photo.daylight_whitebalance
        db_photo.daylight_wb_red = r
        db_photo.daylight_wb_green1 = g1
        db_photo.daylight_wb_blue = b
        db_photo.daylight_wb_green2 = g2
    except LibRawError:
        pass

    # Set photo black levels (i.e. darkness)
    r, g1, b, g2 = rpy_photo.black_level_per_channel
    db_photo.black_level_red = r
    db_photo.black_level_green1 = g1
    db_photo.black_level_blue = b
    db_photo.black_level_green2 = g2

    # Set camera white level (i.e. saturation)
    camera_wb = rpy_photo.camera_white_level_per_channel
    if camera_wb is not None:
        r, g1, b, g2 = camera_wb
    elif (white_level := rpy_photo.white_level) is not None:
        # Fall back to white_level only if camera level isn't found. Otherwise,
        # avoid using this value. See the comment here:
        # https://github.com/letmaik/rawpy/pull/122#issuecomment-692038349
        r, g1, b, g2 = (white_level,) * 4
    db_photo.white_level_red = r
    db_photo.white_level_green1 = g1
    db_photo.white_level_blue = b
    db_photo.white_level_green2 = g2

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
    db_photo.avg_red = np.mean(red_channel)
    db_photo.avg_green = np.mean(green_channel)
    db_photo.avg_blue = np.mean(blue_channel)

    # Use the same image to estimate the brightness percentiles. Correct each
    # RGB value by the default camera white balance multipliers. (Use the
    # camera multipliers instead of image-specific values to keep things
    # constant between images from the same camera).
    brightness = ((red_channel * cam_wb_r +
                   green_channel * (cam_wb_g1 + cam_wb_g2) / 2 +
                   blue_channel * cam_wb_b) // 3).astype(np.uint8)

    db_photo.brightness_min = np.min(brightness)
    db_photo.brightness_max = np.max(brightness)
    db_photo.brightness_mean = np.mean(brightness)
    db_photo.brightness_stdev = np.std(brightness)
    db_photo.brightness_iqr = np.percentile(brightness, 75) - \
                              np.percentile(brightness, 25)

    percentiles = np.percentile(brightness, np.arange(10, 100, 10))
    db_photo.brightness_p10 = percentiles[0]
    db_photo.brightness_p20 = percentiles[1]
    db_photo.brightness_p30 = percentiles[2]
    db_photo.brightness_p40 = percentiles[3]
    db_photo.brightness_median = percentiles[4]
    db_photo.brightness_p60 = percentiles[5]
    db_photo.brightness_p70 = percentiles[6]
    db_photo.brightness_p80 = percentiles[7]
