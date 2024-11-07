import logging
from pathlib import Path

from .conf import CONFIG
from .scan import iterate_all_photos

_log = logging.getLogger(__name__)


async def scan(project: Path) -> None:
    """
    Scan all the files in the timelapse directory, and build a database.

    :param project: Path to the project directory.
    :return: None
    """

    cfg = CONFIG.root
    _log.info(f'Scanning timelapse project "{project}" '
              '(this may take some time)')

    # Scan through the photos
    for _ in iterate_all_photos(project, cfg.date_format,
                                cfg.group_ordering, order=True):
        # The generator logs summary stats; no need to do anything here
        pass
