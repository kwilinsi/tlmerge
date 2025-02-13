from typing import Literal

from .conf import ConfigManager
from .scan import run_scanner
from .preprocess import Preprocessor
from .thumb import generate_thumbnails


def run(mode: Literal['scan', 'preprocess'],
        config: ConfigManager) -> None:
    """
    Run the appropriate function based on the mode

    :param mode: The user-selected mode.
    :param config: The `tlmerge` configuration.
    :return: None
    """

    if mode == 'scan':
        run_scanner(config)
    elif mode == 'preprocess':
        Preprocessor(config).run()
    elif mode == 'thumb':
        generate_thumbnails(config)
    else:
        raise ValueError(f"Invalid execution mode '{mode}'")
