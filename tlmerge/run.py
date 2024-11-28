from typing import Literal

from .scan import run_scanner
from .preprocess import Preprocessor


def run(mode: Literal['scan', 'preprocess']) -> None:
    """
    Run the appropriate function based on the mode

    :param mode: The user-selected mode.
    :return: None
    """

    if mode == 'scan':
        run_scanner()
    elif mode == 'preprocess':
        Preprocessor().run()
    else:
        raise ValueError(f"Invalid execution mode '{mode}'")
