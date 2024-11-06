import logging
from pathlib import Path

from .conf import CONFIG

_log = logging.getLogger(__name__)


async def scan(project: Path) -> None:
    """
    Scan all the files in the timelapse directory, and build a database.

    :param project: Path to the project directory.
    :return: None
    """

    cfg = CONFIG.root
    _log.info(f'Scanning {project} (db={cfg.database})')
