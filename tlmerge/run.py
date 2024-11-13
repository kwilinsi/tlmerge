import asyncio
import logging
from pathlib import Path
from typing import Literal

from .scan import scan
from .preprocess import preprocess

_log = logging.getLogger(__name__)


async def run(mode: Literal['scan', 'preprocess'],
              project: Path) -> None:
    # Set the asyncio task name to the mode; useful for logging
    asyncio.current_task().set_name(mode.capitalize())

    # Run the appropriate function based on the mode
    if mode == 'scan':
        await scan(project)
    elif mode == 'preprocess':
        await preprocess(project)
    else:
        raise ValueError(f"Invalid execution mode '{mode}'")
