from sqlalchemy import String
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

    # Location
    make: Mapped[str] = mapped_column(String(MAX_CAMERA_MAKE_LENGTH))
    model: Mapped[str] = mapped_column(String(MAX_CAMERA_MODEL_LENGTH))

    # Photo relationship: one-to-many
    photos: Mapped[list["Photo"]] = relationship(  # noqa
        back_populates='camera'
    )
