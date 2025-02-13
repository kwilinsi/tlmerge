import logging
from pathlib import Path

import imageio.v3 as iio
from PIL import Image
import rawpy
# noinspection PyUnresolvedReferences
from rawpy import (LibRawError, LibRawNoThumbnailError,
                   LibRawUnsupportedThumbnailError, RawPy, ThumbFormat)

from tlmerge import scan
from tlmerge.conf import ConfigManager, GroupConfig
from tlmerge.utils import postprocess, WorkerPool

_log = logging.getLogger(__name__)


def generate_thumbnails(config: ConfigManager,
                        queue_max_size: int = 100) -> None:
    """
    Generate preview thumbnails for all timelapse images.

    :param config: The `tlmerge` configuration.
    :param queue_max_size: The maximum size of the queue that facilitates
     thumbnail generation via a worker pool. Defaults to 100.
    :return: None
    """

    # Get root config
    root = config.root

    sample, s_random, s_size = root.sample_details()
    _log.info(
        f"Generating thumbnails for {'random ' if s_random else ''}"
        f"{f'sample of {s_size}' if sample else 'all included'} "
        f"photo{'' if s_size == 1 else 's'}…"
    )

    n_workers = root.workers()
    if n_workers > s_size:
        _log.debug(f"Using {s_size} worker{'' if s_size == 1 else 's'} (extra "
                   f"{s_size - n_workers} not needed as the sample size is "
                   f"only {s_size})")
        n_workers = s_size
    else:
        _log.debug(f"Using {n_workers} worker{'' if n_workers == 1 else 's'}")

    worker_pool = WorkerPool(
        max_workers=n_workers,
        name_prefix='thm-wkr-',
        task_queue_size=queue_max_size
    )

    worker_pool.start()

    try:
        # Send all thumbnail tasks to worker pool
        photos = _enqueue_thumbnail_tasks(config, worker_pool)
    except Exception:
        # If there's an error enqueuing, log it
        _log.error("Fatal error while generating thumbnails", exc_info=True)
        return
    finally:
        # Clean up the worker pool, waiting for all threads to finish
        try:
            worker_pool.close()
            worker_pool.join()
        except BaseException:
            # If workers failed, log the errors here. Note that this is
            # separate from the logging line above, as it's possible to log two
            # error messages if both enqueuing and cleanup fail
            _log.error(
                "Fatal error while generating thumbnails",
                exc_info=True
            )
            return

    # Success
    _log.info(f"Finished generating thumbnails for {photos} "
              f"photo{'' if photos == 1 else 's'}")


def _enqueue_thumbnail_tasks(config: ConfigManager,
                             worker_pool: WorkerPool) -> int:
    """
    Iterate through all the photos found in the database, adding thumbnail
    generation tasks for each photo the given worker pool.

    :param config: The `tlmerge` configuration.
    :param worker_pool: The pool that actually generates thumbnails.
    :return: The number of enqueued tasks, which is the number of generated
     thumbnails.
    """

    project_dir: Path = config.root.project()

    # Create a cache of destination directories for each group
    thumb_paths: dict[tuple[str, str], tuple[GroupConfig, Path]] = {}

    counter = 0

    for dt, grp, file in scan.iter_photo_records_from_db(config):
        # Construct the path to the photo file
        photo_path: Path = project_dir / dt / grp / file
        rel_photo_path = str(Path(dt) / grp / file)

        # Check whether this group config and associated path to the thumbnail
        # directory are already cached
        group_config, dest = thumb_paths.get((dt, grp), (None, None))

        # If not cached, load the config, find the proper path, and then cache
        if dest is None:
            group_config = config.get(dt, grp)
            dest = group_config.get_full_thumbnail_path(project_dir, dt)

            # Validate this path by making sure it's not a file
            if dest.is_file():
                raise RuntimeError(
                    f"Cannot create thumbnails for '{Path(dt) / grp}': "
                    f"the destination directory '{dest}' is a file."
                )

            # If the destination directory doesn't already exist, create it
            dest.mkdir(parents=True, exist_ok=True)

            # Add this path to the cache
            thumb_paths[(dt, grp)] = (group_config, dest)

        # Add this thumbnail as a task to the worker pool
        worker_pool.add(
            save_thumbnail,
            rel_photo_path,
            photo_path,
            dest / (photo_path.stem + '.jpg'),
            group_config
        )
        counter += 1

    return counter


def save_thumbnail(source: Path,
                   destination: Path,
                   config: GroupConfig) -> None:
    """
    Load the source image, extract a thumbnail preview, and save it to the
    destination path.

    :param source: The path to the source photo.
    :param destination: The destination path for the thumbnail.
    :param config: The configuration for the particular group containing the
     source photo.
    :return: None
    """

    # Determine whether to use the embedded thumbnail
    use_embedded_thumb = config.use_embedded_thumbnail()

    # Open the photo in RawPy (i.e. LibRaw) to get the thumbnail
    with rawpy.imread(str(source)) as rpy_photo:
        thumb = None

        # Attempt to get the embedded thumbnail if requested. If that fails,
        # we'll postprocess the full raw image
        if use_embedded_thumb:
            try:
                embed = rpy_photo.extract_thumb()
                if embed.format == ThumbFormat.JPEG:
                    thumb = Image.fromarray(iio.imread(embed.data))
                elif embed.format == ThumbFormat.BITMAP:
                    thumb = Image.fromarray(embed.data)
                else:
                    _log.debug(
                        f'Embedded thumbnail {embed} is invalid for {source}'
                    )
            except (LibRawNoThumbnailError, LibRawUnsupportedThumbnailError):
                _log.debug(f'No embedded thumbnail available for {source}')

        if thumb is None:
            thumb: Image = Image.fromarray(postprocess(rpy_photo, config))

    # If down-sampling the thumbnail, do that
    resize_factor = config.thumbnail_resize_factor()
    if resize_factor < 1:
        thumb = thumb.resize(
            (int(thumb.width * resize_factor),
             int(thumb.height * resize_factor)),
            Image.Resampling.LANCZOS
        )

    # Save the thumbnail with the specified quality
    thumb.save(destination, format='JPEG', quality=config.thumbnail_quality())
