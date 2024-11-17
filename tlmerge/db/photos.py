from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

# The maximum length of a date directory name
MAX_DATE_LENGTH = 25

# The maximum length of a group name
MAX_GROUP_LENGTH = 25

# The maximum length of a photo file name
MAX_PHOTO_NAME_LENGTH = 25


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
    auto_focus: Mapped[bool | None] = mapped_column(Boolean())
    focus_distance: Mapped[float] = mapped_column(Float)  # in meters
    field_of_view: Mapped[float] = mapped_column(Float)  # in degrees

    # Photo size
    raw_width: Mapped[int] = mapped_column(Integer())
    raw_height: Mapped[int] = mapped_column(Integer())
    width: Mapped[int] = mapped_column(Integer())
    height: Mapped[int] = mapped_column(Integer())
    thumb_width: Mapped[int | None] = mapped_column(Integer())
    thumb_height: Mapped[int | None] = mapped_column(Integer())

    # White balance
    daylight_wb_red: Mapped[float | None] = mapped_column(Float())
    daylight_wb_green1: Mapped[float | None] = mapped_column(Float())
    daylight_wb_blue: Mapped[float | None] = mapped_column(Float())
    daylight_wb_green2: Mapped[float | None] = mapped_column(Float())
    avg_red: Mapped[float] = mapped_column(Float())
    avg_green: Mapped[float] = mapped_column(Float())
    avg_blue: Mapped[float] = mapped_column(Float())

    # Black and white levels (i.e. darkness and saturation in dcraw)
    black_level_red: Mapped[float] = mapped_column(Float())
    black_level_green1: Mapped[float] = mapped_column(Float())
    black_level_blue: Mapped[float] = mapped_column(Float())
    black_level_green2: Mapped[float] = mapped_column(Float())
    white_level_red: Mapped[float] = mapped_column(Float())
    white_level_green1: Mapped[float] = mapped_column(Float())
    white_level_blue: Mapped[float] = mapped_column(Float())
    white_level_green2: Mapped[float] = mapped_column(Float())

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
    brightness_iqr: Mapped[float] = mapped_column(Float())
    exposure_difference: Mapped[float | None] = mapped_column(Float())
