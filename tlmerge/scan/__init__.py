from pathlib import Path
import logging

from tlmerge.conf import CONFIG
from .scanner import Scanner

_log = logging.getLogger(__name__)


async def scan(project: Path) -> None:
    """
    Scan all the files in the timelapse directory to log summary statistics
    on the number of photos.

    :param project: The path to the project directory.
    :return: None
    """

    _log.info(f'Scanning timelapse project "{project}" '
              '(this may take some time)')

    # Iterator that traverses the photos. Ordered so log messages look better
    iterator = Scanner(order=True).iter_all_photos()

    # If sampling 50 or fewer photos, log each of them
    sample, _, sample_size = CONFIG.root.sample_details()
    if sample and sample_size <= 50:
        async for photo in iterator:
            _log.info(f'Found {photo}')
    else:
        async for _ in iterator:
            pass
