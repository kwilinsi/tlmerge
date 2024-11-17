import asyncio
from asyncio import Queue as AsyncQueue, Task
import logging
from pathlib import Path
from queue import SimpleQueue

import imageio.v3 as iio
import numpy as np
import rawpy
from rawpy import LibRawError, RawPy, ThumbFormat
from PIL import Image

from tlmerge.conf import CONFIG
from tlmerge.db import DB, Camera, Lens, Photo
from tlmerge.scan import Scanner
from .async_worker_pool import AsyncWorkerPool, AsyncPoolExceptionGroup
from .exif import ExifWorker, PhotoExifRecord

_log = logging.getLogger(__name__)


class Preprocessor:
    def __init__(self):
        # Determine the number of workers to use. If the number of workers is
        # greater than the number of photos we're loading (due to a sample),
        # then reduce the worker count to avoid unused workers
        self.cfg = CONFIG.root
        workers = self.cfg.workers
        sample, s_random, s_size = self.cfg.sample_details()
        if 0 <= s_size < workers:
            workers = s_size

        # Log info
        msg = f'Running preprocessor with {workers} workers'
        if sample:
            msg += (f": {'randomly ' if s_random else ''} sampling "
                    f"{s_size} photo{'' if s_size else 's'}")
        _log.info(msg)

        # Database queue and worker that processes it
        self.database_queue: AsyncQueue[Photo | None] = AsyncQueue()
        self.db_worker: Task | None = None

        # EXIF queue and threaded workers that process it
        self.exif_queue: SimpleQueue[PhotoExifRecord | None] = SimpleQueue()
        loop = asyncio.get_running_loop()
        self.exif_workers: list[ExifWorker] = [
            ExifWorker(self.exif_queue, loop, i)
            for i in range(1, workers + 1)
        ]

        # The primary async worker pool that processes each photo
        self.photo_worker_pool = AsyncWorkerPool(
            workers=workers,
            max_errors=self.cfg.max_processing_errors,
            results=self.database_queue
        )

    async def run(self) -> None:
        """
        Run the preprocessing step. This loads all the photos based on the
        program configuration, scans their metadata (and some other info about
        brightness), and saves that information to the database.

        :return: None
        """

        cfg = CONFIG.root
        project = cfg.project
        _log.info(f'Preprocessing photos in "{project}" '
                  '(this may take some time)')

        # Start the database worker
        self.db_worker = asyncio.create_task(
            self._run_db_worker(), name='db_worker'
        )

        # Start the exif workers
        for worker in self.exif_workers:
            worker.start()

        # Run the async worker pool that processes all the photos. If it fails,
        # exit here
        if not await self._load_photos():
            return

        # Signal the database and EXIF workers to close
        await self.database_queue.put(None)
        for _ in range(len(self.exif_workers)):
            self.exif_queue.put(None)

        # Wait for DB worker to finish
        await self.db_worker

        # Wait for EXIF workers to close, ironically by spawning another thread
        # that joins with the worker threads to wait on them
        def wait_for_exif_workers():
            for w in self.exif_workers:
                w.join()

        await asyncio.to_thread(wait_for_exif_workers)

    async def _load_photos(self) -> bool:
        try:
            # Assign photos to a worker pool
            async with self.photo_worker_pool as pool:
                async for p in Scanner().iter_all_photos():
                    pool.add(self.preprocess_photo(p))

            # Success
            return True
        except AsyncPoolExceptionGroup as exc_pool:
            # Log the error(s)
            n = len(exc_pool.exceptions)
            if n == 1:
                err = exc_pool.exceptions[0].__class__.__name__
            else:
                err = f"{n} error{'s' if n > 1 else ''}"

            _log.critical(
                f"Failed to preprocess photos in {self.cfg.project}: "
                f"got {err}",
                exc_info=True
            )
            self._cancelled()
            return False
        except Exception as e:
            # Unexpected other exception
            _log.critical(
                f'Failed to preprocess photos in {self.cfg.project}: '
                f'got unexpected {e.__class__.__name__}',
                exc_info=True
            )
            self._cancelled()
            return False
        except BaseException:
            self._cancelled()
            # Re-raise a fatal error (like KeyboardInterrupt or SystemExit)
            raise

    async def _run_db_worker(self) -> None:
        while True:
            photo = await self.database_queue.get()
            if photo is None:
                return

            # Save to database
            _log.info(f'Saving photo to database: '
                      f'\nPhoto: {photo.__dict__}'
                      f'\nCamera: {photo.camera.__dict__}'
                      f'\nLens: {photo.lens.__dict__}')

    def _cancelled(self) -> None:
        """
        Something went wrong, and we need to cancel. Perform some cleanup by
        cancelling worker tasks and threads spawned by this Preprocessor.

        :return: None
        """

        # Cancel the database worker
        self.db_worker.cancel()

        # Signal EXIF workers to stop
        while not self.exif_queue.empty():
            self.exif_queue.get_nowait()
        for _ in range(len(self.exif_workers)):
            self.exif_queue.put(None)

    async def preprocess_photo(self, file: Path) -> Photo:
        """
        Preprocess a single photo.

        :param file: The path to the photo file.
        :return: A new database Photo record.
        """

        _log.info(f'Preprocessing {CONFIG.root.rel_path(file)}')

        # Set the task name to the file name. Useful for logging. The structure
        # (photo/group/date) is reversed since the file name is more important than
        # the group and date
        asyncio.current_task().set_name(
            Path(file.stem) / file.parent.name / file.parent.parent.name
        )

        # Send the photo to the exif processor
        exif_record = PhotoExifRecord(file)
        self.exif_queue.put_nowait(exif_record)

        # Create the database record
        photo = Photo()
        photo.camera = Camera()
        photo.lens = Lens()

        # Open the photo in rawpy (i.e. libraw) to get some info
        def run_rawpy():
            with rawpy.imread(str(file)) as rpy_photo:
                _apply_libraw_metadata(rpy_photo, photo)

        await asyncio.to_thread(run_rawpy)

        # do stuff
        ...

        # Wait for the metadata from the EXIF worker, and then add it
        await exif_record.event.wait()
        exif_record.apply_metadata(photo)

        # Return the finished db Photo record
        return photo


# noinspection DuplicatedCode
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
