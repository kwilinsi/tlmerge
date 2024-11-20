from collections import deque
from pathlib import Path
import logging

from tlmerge.conf import CONFIG
from .scanner import Scanner

_log = logging.getLogger(__name__)


def scan(project: Path) -> None:
    """
    Scan all the files in the timelapse directory to log summary statistics
    on the number of photos.

    :param project: The path to the project directory.
    :return: None
    """

    _log.info(f'Scanning timelapse project "{project}" '
              '(this may take some time)')

    # Iterator that traverses the photos. Ordered so log messages look better
    generator = Scanner(order=True).iter_all_photos()

    # If sampling 50 or fewer photos, log each of them
    sample, s_random, s_size = CONFIG.root.sample_details()
    if sample and s_size <= 50:
        _log.info(f"Sampling {s_size}{' random' if s_random else ''} "
                  f"photo{'' if s_size == 1 else 's'}…")
        for photo in generator:
            _log.info(f'Found photo {photo}')
    else:
        # Quickly exhaust the generator for its logging side effects and
        # to make sure there aren't any scanning errors
        # https://stackoverflow.com/a/50938015/10034073
        deque(generator, maxlen=0)
