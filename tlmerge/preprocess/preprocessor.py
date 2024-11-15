import asyncio
from asyncio import Queue as AsyncQueue, Task
import logging
from pathlib import Path
from queue import SimpleQueue

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

        camera = Camera()
        photo.camera = camera
        lens = Lens()
        photo.lens = lens

        # do stuff
        ...

        # Wait for the metadata from the EXIF worker, and then add it
        await exif_record.event.wait()
        exif_record.apply_metadata(photo)

        # Return the finished db Photo record
        return photo


async def _apply_exif_info(file: Path, photo: Photo) -> Photo:
    """
    Extract the EXIF data from a photo, applying the values to the database
    Photo record.

    :param file: The path to the photo file.
    :param photo: The database Photo record.
    :return: The same Photo record (for chaining).
    """

    img: Image = Image.open(file)
    exif: Image.Exif = img.getexif()

    if exif is None:
        raise ValueError(
            "Failed to extract EXIF data from "
            f"{Path(file.parent.parent.name) / file.parent.name / file.name}")
    else:
        _log.info(f'Found {len(exif)} EXIF entries for {file}')

    return photo
