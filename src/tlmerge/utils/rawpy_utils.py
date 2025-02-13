import numpy as np
# noinspection PyUnresolvedReferences
from rawpy import RawPy

from tlmerge.conf import BaseConfig


def postprocess(photo: RawPy, config: BaseConfig) -> np.ndarray:
    """
    Postprocess the given RawPy photo based on the given configuration.
    :param photo: The photo to postprocess.
    :param config: The particular configuration record to control the
     postprocessing behavior.
    :return: The postprocessed photo, an `ndarray` of shape (h, w, c).
    """

    wb = config.white_balance()
    cam_wb = auto_wb = False
    user_wb = None
    if wb == 'camera':
        cam_wb = True
    elif wb == 'auto':
        auto_wb = True
    elif isinstance(wb, tuple):
        user_wb = wb

    return photo.postprocess(
        median_filter_passes=config.median_filter(),
        use_camera_wb=cam_wb,
        use_auto_wb=auto_wb,
        user_wb=user_wb,
        chromatic_aberration=config.chromatic_aberration(),
    )
