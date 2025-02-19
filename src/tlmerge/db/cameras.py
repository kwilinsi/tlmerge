from sqlalchemy import Float, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

# The maximum length of a camera make
MAX_CAMERA_MAKE_LENGTH = 75

# The maximum length of a camera model
MAX_CAMERA_MODEL_LENGTH = 100


class Camera(Base):
    __tablename__ = 'Cameras'

    # Primary key
    id: Mapped[int] = mapped_column(primary_key=True)

    # Name
    make: Mapped[str] = mapped_column(String(MAX_CAMERA_MAKE_LENGTH))
    model: Mapped[str] = mapped_column(String(MAX_CAMERA_MODEL_LENGTH))

    # The daylight white balance coefficients for this camera model
    daylight_wb_red: Mapped[float | None] = mapped_column(Float())
    daylight_wb_green1: Mapped[float | None] = mapped_column(Float())
    daylight_wb_blue: Mapped[float | None] = mapped_column(Float())
    daylight_wb_green2: Mapped[float | None] = mapped_column(Float())

    # Photo relationship: one-to-many
    photos: Mapped[list['Photo']] = relationship(  # noqa
        back_populates='camera'
    )

    __table_args__ = (
        UniqueConstraint(
            make, model, daylight_wb_red, daylight_wb_green1,
            daylight_wb_blue, daylight_wb_green2,
            name='all_unique'
        ),
    )
