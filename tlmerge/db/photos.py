from enum import Enum
from datetime import datetime

from sqlalchemy import (Boolean, DateTime, Enum as SQLEnum,
                        Float, ForeignKey, Integer, String)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

# The maximum length of a date directory name
MAX_DATE_LENGTH = 25

# The maximum length of a group name
MAX_GROUP_LENGTH = 25

# The maximum length of a photo file name
MAX_PHOTO_NAME_LENGTH = 25

# The maximum length of the filter pattern as reported by dcraw
MAX_FILTER_PATTERN_LENGTH = 50


class Interpolation(Enum):
    """
    These are the interpolation options supported by dcraw, set via the -q flag.
    """

    BILINEAR = 0
    VNG = 1
    PPG = 2
    AHD = 3


class Photo(Base):
    __tablename__ = 'Photos'

    # Primary key
    id: Mapped[int] = mapped_column(primary_key=True)

    # Location
    date: Mapped[str] = mapped_column(String(MAX_DATE_LENGTH))
    group: Mapped[str] = mapped_column(String(MAX_GROUP_LENGTH))
    file_name: Mapped[str] = mapped_column(String(MAX_PHOTO_NAME_LENGTH))

    # Camera and lens
    camera_id: Mapped[int] = mapped_column(ForeignKey("Cameras.id"))
    camera: Mapped["Camera"] = relationship(  # noqa
        back_populates='photos',
        lazy='joined'
    )
    lens_id: Mapped[int] = mapped_column(ForeignKey("Lenses.id"))
    lens: Mapped["Lens"] = relationship(  # noqa
        back_populates='photos',
        lazy='joined'
    )

    # Photo metadata
    time_taken: Mapped[datetime] = mapped_column(DateTime())
    file_size: Mapped[int] = mapped_column(Integer())  # in kilobytes
    iso: Mapped[int | None] = mapped_column(Integer())
    shutter_speed: Mapped[str | None] = mapped_column(Float())  # a/b
    aperture: Mapped[float | None] = mapped_column(Float())  # f/[#]
    focal_length: Mapped[float | None] = mapped_column(Float())  # mm
    embedded_icc_profile: Mapped[bool | None] = mapped_column(Boolean())
    num_raw_images: Mapped[int | None] = mapped_column(Integer())
    auto_focus: Mapped[bool | None] = mapped_column(Boolean())
    focus_distance: Mapped[float] = mapped_column(Float)  # in meters
    field_of_view: Mapped[float] = mapped_column(Float)  # in degrees

    # Photo size
    width: Mapped[int] = mapped_column(Integer())
    height: Mapped[int] = mapped_column(Integer())
    thumb_width: Mapped[int] = mapped_column(Integer())
    thumb_height: Mapped[int] = mapped_column(Integer())

    # White balance
    daylight_mult_red: Mapped[float | None] = mapped_column(Float())
    daylight_mult_green: Mapped[float | None] = mapped_column(Float())
    daylight_mult_blue: Mapped[float | None] = mapped_column(Float())
    camera_mult_red: Mapped[float] = mapped_column(Float())
    camera_mult_green1: Mapped[float] = mapped_column(Float())
    camera_mult_blue: Mapped[float] = mapped_column(Float())
    camera_mult_green2: Mapped[float] = mapped_column(Float())
    auto_mult_red: Mapped[float] = mapped_column(Float())
    auto_mult_green1: Mapped[float] = mapped_column(Float())
    auto_mult_blue: Mapped[float] = mapped_column(Float())
    auto_mult_green2: Mapped[float] = mapped_column(Float())

    # Darkness and saturation
    darkness: Mapped[float] = mapped_column(Float())
    saturation: Mapped[float] = mapped_column(Float())

    # Overall brightness
    brightness_min: Mapped[int] = mapped_column(Integer())
    brightness_p10: Mapped[float] = mapped_column(Float())
    brightness_p20: Mapped[float] = mapped_column(Float())
    brightness_p30: Mapped[float] = mapped_column(Float())
    brightness_p40: Mapped[float] = mapped_column(Float())
    brightness_median: Mapped[float] = mapped_column(Float())
    brightness_p60: Mapped[float] = mapped_column(Float())
    brightness_p70: Mapped[float] = mapped_column(Float())
    brightness_p80: Mapped[float] = mapped_column(Float())
    brightness_p90: Mapped[float] = mapped_column(Float())
    brightness_max: Mapped[int] = mapped_column(Integer())
    brightness_mean: Mapped[float] = mapped_column(Float())
    brightness_stdev: Mapped[float] = mapped_column(Float())
    exposure_difference: Mapped[float | None] = mapped_column(Float())

    # Misc
    raw_colors: Mapped[int | None] = mapped_column(Integer())
    filter_pattern: Mapped[str | None] = \
        mapped_column(String(MAX_FILTER_PATTERN_LENGTH))
    default_interpolation: Mapped[Interpolation | None] = \
        mapped_column(SQLEnum(Interpolation))
