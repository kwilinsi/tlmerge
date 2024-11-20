import logging
from pathlib import Path
from typing import Literal

from .scan import scan
from .preprocess import Preprocessor

_log = logging.getLogger(__name__)


def run(mode: Literal['scan', 'preprocess'],
        project: Path) -> None:
    # Run the appropriate function based on the mode
    if mode == 'scan':
        scan(project)
    elif mode == 'preprocess':
        Preprocessor().run()
    else:
        raise ValueError(f"Invalid execution mode '{mode}'")
