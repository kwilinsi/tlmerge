from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

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
    'capture_wb_red',
    'capture_wb_green1',
    'capture_wb_blue',
    'capture_wb_green2',
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
    'daylight_wb_red',
    'daylight_wb_green1',
    'daylight_wb_blue',
    'daylight_wb_green2'
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
    def __init__(self, date: str, group: str, file_name: str) -> None:
        """
        Initialize a PhotoMetadata object by specifying the photo to which it
        refers.

        :param date: The name of the date directory.
        :param group: The name of the group subdirectory within the date.
        :param file_name: The name of the photo file within the group.
        :return: None
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
    capture_wb_red: float | None
    capture_wb_green1: float | None
    capture_wb_blue: float | None
    capture_wb_green2: float | None
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
    camera_daylight_wb_red: float | None
    camera_daylight_wb_green1: float | None
    camera_daylight_wb_blue: float | None
    camera_daylight_wb_green2: float | None

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

    def path_str(self) -> str:
        """
        Combine the `date`, `group`, and `file_name` into a path, and then
        return it as a string. This is useful for identifying the file in
        one string.

        :return: The relative path to the file within the project as a string.
        """

        return str(Path(self.date, self.group, self.file_name))

    def apply_photo_metadata(self, photo: Photo) -> None:
        """
        Apply this metadata to the given database Photo record. This does NOT
        apply metadata for the camera or lens.

        :param photo: The photo record to modify.
        :return: None
        """

        # Set photo attributes
        for attr in _PHOTO_ATTRIBUTES:
            setattr(photo, attr, getattr(self, attr))

    def matches_camera(self, camera: Camera) -> bool:
        """
        Check whether this metadata matches the given database Camera record.

        :param camera: The camera to compare to this metadata.
        :return: True if and only if all the attributes match.
        """

        for attr in _CAMERA_ATTRIBUTES:
            if getattr(camera, attr) != getattr(self, 'camera_' + attr):
                return False
        return True

    def matches_lens(self, lens: Lens) -> bool:
        """
        Check whether this metadata matches the given database Lens record.

        :param lens: The lens to compare to this metadata.
        :return: True if and only if all the attributes match.
        """

        for attr in _LENS_ATTRIBUTES:
            if getattr(lens, attr) != getattr(self, 'lens_' + attr):
                return False
        return True

    def get_camera_id(self, session: Session) -> int | None:
        """
        Get the id of the Camera record in the database matching this metadata.
        If there is no such Camera, return None.

        :param session: A session connected to the database.
        :return: The id of the matching Camera, or None if there's no match.
        """

        return session.scalar(select(Camera.id).where(
            Camera.make == self.camera_make,
            Camera.model == self.camera_model,
            Camera.daylight_wb_red == self.camera_daylight_wb_red,
            Camera.daylight_wb_green1 == self.camera_daylight_wb_green1,
            Camera.daylight_wb_blue == self.camera_daylight_wb_blue,
            Camera.daylight_wb_green2 == self.camera_daylight_wb_green2
        ))

    def get_lens_id(self, session: Session) -> int | None:
        """
        Get the id of the Lens record in the database matching this metadata.
        If there is no such Lens, return None.

        :param session: A session connected to the database.
        :return: The id of the matching Lens, or None if there's no match.
        """

        return session.scalar(select(Lens.id).where(
            Lens.make == self.lens_make,
            Lens.model == self.lens_model,
            Lens.spec == self.lens_spec,
            Lens.min_focal_length == self.lens_min_focal_length,
            Lens.max_focal_length == self.lens_max_focal_length,
            Lens.lens_f_stops == self.lens_lens_f_stops,
            Lens.max_aperture_min_focal == self.lens_max_aperture_min_focal,
            Lens.max_aperture_max_focal == self.lens_max_aperture_max_focal,
            Lens.effective_max_aperture == self.lens_effective_max_aperture
        ))
    
    def camera_str(self) -> str:
        """
        Get a string with the camera make and model.
        
        :return: The camera make and model.
        """

        return f'{self.camera_make} {self.camera_model}'

    def create_camera(self) -> Camera:
        """
        Create a new database Camera record based on this metadata.

        :return: A new Camera record.
        """

        return Camera(
            make=self.camera_make,
            model=self.camera_model,
            daylight_wb_red=self.camera_daylight_wb_red,
            daylight_wb_green1=self.camera_daylight_wb_green1,
            daylight_wb_blue=self.camera_daylight_wb_blue,
            daylight_wb_green2=self.camera_daylight_wb_green2
        )
    
    def lens_str(self) -> str:
        """
        Get a string with the lens make and model.
        
        :return: The lens make and model.
        """

        return f'{self.lens_make} {self.lens_model}'

    def create_lens(self) -> Lens:
        """
        Create a new database Lens record based on this metadata.

        :return: A new Lens record.
        """

        return Lens(
            make=self.lens_make,
            model=self.lens_model,
            spec=self.lens_spec,
            min_focal_length=self.lens_min_focal_length,
            max_focal_length=self.lens_max_focal_length,
            lens_f_stops=self.lens_lens_f_stops,
            max_aperture_min_focal=self.lens_max_aperture_min_focal,
            max_aperture_max_focal=self.lens_max_aperture_max_focal,
            effective_max_aperture=self.lens_effective_max_aperture,
        )
