from dataclasses import dataclass
from datetime import datetime

from tlmerge.db import Camera, Lens, Photo

_PHOTO_ATTRIBUTES = (
    'time_taken',
    'file_size',
    'iso',
    'shutter_speed',
    'aperture',
    'focal_length',
    'auto_focus',
    'focus_distance',
    'field_of_view',
    'raw_width',
    'raw_height',
    'width',
    'height',
    'thumb_width',
    'thumb_height',
    'daylight_wb_red',
    'daylight_wb_green1',
    'daylight_wb_blue',
    'daylight_wb_green2',
    'avg_red',
    'avg_green',
    'avg_blue',
    'black_level_red',
    'black_level_green1',
    'black_level_blue',
    'black_level_green2',
    'white_level_red',
    'white_level_green1',
    'white_level_blue',
    'white_level_green2',
    'brightness_min',
    'brightness_p10',
    'brightness_p20',
    'brightness_p30',
    'brightness_p40',
    'brightness_median',
    'brightness_p60',
    'brightness_p70',
    'brightness_p80',
    'brightness_p90',
    'brightness_max',
    'brightness_mean',
    'brightness_stdev',
    'brightness_iqr',
    'exposure_difference',
)

_CAMERA_ATTRIBUTES = (
    'make',
    'model',
    'wb_red',
    'wb_green1',
    'wb_blue',
    'wb_green2'
)

_LENS_ATTRIBUTES = (
    'make',
    'model',
    'spec',
    'min_focal_length',
    'max_focal_length',
    'lens_f_stops',
    'max_aperture_min_focal',
    'max_aperture_max_focal',
    'effective_max_aperture'
)


@dataclass(init=False)
class PhotoMetadata:
    def __init__(self, date: str, group: str, file_name: str):
        """
        Initialize a PhotoMetadata object by specifying the photo to which it
        refers.

        :param date: The name of the date directory.
        :param group: The name of the group subdirectory within the date.
        :param file_name: The name of the photo file within the group.
        """

        self.date = date
        self.group = group
        self.file_name = file_name

    # File location
    date: str
    group: str
    file_name: str

    # Photo metadata
    time_taken: datetime
    file_size: int
    iso: int | None
    shutter_speed: str | None
    aperture: float | None
    focal_length: float | None
    auto_focus: bool | None
    focus_distance: float
    field_of_view: float

    # Photo size
    raw_width: int
    raw_height: int
    width: int
    height: int
    thumb_width: int | None
    thumb_height: int | None

    # White balance
    daylight_wb_red: float | None
    daylight_wb_green1: float | None
    daylight_wb_blue: float | None
    daylight_wb_green2: float | None
    avg_red: float
    avg_green: float
    avg_blue: float

    # Black and white levels (i.e. darkness and saturation)
    black_level_red: float
    black_level_green1: float
    black_level_blue: float
    black_level_green2: float
    white_level_red: float
    white_level_green1: float
    white_level_blue: float
    white_level_green2: float

    # Overall brightness
    brightness_min: int
    brightness_p10: float
    brightness_p20: float
    brightness_p30: float
    brightness_p40: float
    brightness_median: float
    brightness_p60: float
    brightness_p70: float
    brightness_p80: float
    brightness_p90: float
    brightness_max: int
    brightness_mean: float
    brightness_stdev: float
    brightness_iqr: float
    exposure_difference: float | None

    # Camera data
    camera_make: str
    camera_model: str
    camera_wb_red: float | None
    camera_wb_green1: float | None
    camera_wb_blue: float | None
    camera_wb_green2: float | None

    # Lens data
    lens_make: str | None
    lens_model: str | None
    lens_spec: str | None
    lens_min_focal_length: float
    lens_max_focal_length: float
    lens_lens_f_stops: float
    lens_max_aperture_min_focal: float
    lens_max_aperture_max_focal: float
    lens_effective_max_aperture: float

    def apply_to_db_photo(self, photo: Photo) -> None:
        """
        Apply this metadata to the given database Photo record.

        :param photo: The photo record to modify.
        :return: None
        """

        # Set photo attributes
        for attr in _PHOTO_ATTRIBUTES:
            setattr(photo, attr, getattr(self, attr))

        # Set camera attributes
        camera: Camera = photo.camera
        for attr in _CAMERA_ATTRIBUTES:
            setattr(camera, attr, getattr(self, 'camera_' + attr))

        # Set the lens attributes
        lens: Lens = photo.lens
        for attr in _LENS_ATTRIBUTES:
            setattr(lens, attr, getattr(self, 'lens_' + attr))
