import asyncio
from asyncio import Queue
import logging
from pathlib import Path

from PIL import Image

from tlmerge.conf import CONFIG
from tlmerge.db import DB, Camera, Lens, Photo
from tlmerge.scan import iterate_all_photos
from .async_worker_pool import AsyncWorkerPool, AsyncPoolExceptionGroup

_log = logging.getLogger(__name__)


async def preprocess(project: Path) -> None:
    """
    Scan through all the photos in the timelapse, collecting their metadata and
    adding them to the database.

    :param project: The path to the project directory.
    :return: None
    """

    _log.info(f'Preprocessing photos in "{project}" (this may take some time)')
    cfg = CONFIG.root

    # Initialize the results queue that's populated with photos to send to db
    # and a task worker to process those photos
    photo_queue: Queue[Photo | None] = Queue()
    db_writer = asyncio.create_task(save_to_db(photo_queue), name='db_writer')

    # Run the async worker pool that processes all the photos
    try:
        async with AsyncWorkerPool(workers=cfg.workers,
                                   max_errors=cfg.max_processing_errors,
                                   results=photo_queue) as pool:
            async for p in iterate_all_photos(project):
                pool.add(preprocess_photo(p))
    except AsyncPoolExceptionGroup as exc_pool:
        # Log the error(s)
        n = len(exc_pool.exceptions)
        if n == 1:
            err = exc_pool.exceptions[0].__class__.__name__
        else:
            err = f"{n} error{'s' if n > 1 else ''}"

        _log.critical(
            f"Failed to preprocess photos in {project}: got {err}",
            exc_info=True
        )

        # Cancel the database writer
        db_writer.cancel()
        return
    except Exception as e:
        # Unexpected other exception
        _log.critical(f'Failed to preprocess photos in {project}: '
                      f'got unexpected {e.__class__.__name__}', exc_info=True)

        # Cancel the database writer
        db_writer.cancel()
        return
    except BaseException:
        # Re-raise a fatal error (like KeyboardInterrupt or SystemExit)
        db_writer.cancel()
        raise

    # Close photo queue, and wait for results to finish saving to the database
    await photo_queue.put(None)
    await db_writer


async def save_to_db(queue: Queue[Photo | None]) -> None:
    while True:
        photo = await queue.get()
        if photo is None:
            return

        # Save to database
        print(f'Saving {photo} to database')


async def preprocess_photo(file: Path) -> Photo:
    """
    Preprocess a single photo.

    :param file: The path to the photo file.
    :return: A new database Photo record.
    """

    _log.info(f'Preprocessing {file.parent.name}/{file.stem}')

    # Set the task name to the file name. Useful for logging. The structure
    # (photo/group/date) is reversed since the file name is more important than
    # the group and date
    asyncio.current_task().set_name(
        Path(file.stem) / file.parent.name / file.parent.parent.name
    )

    photo = Photo()
    await _apply_exif_info(file, photo)
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
