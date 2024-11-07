from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

# The maximum length of a camera make
MAX_CAMERA_MAKE_LENGTH = 100

# The maximum length of a camera name
MAX_CAMERA_NAME_LENGTH = 100


class Camera(Base):
    __tablename__ = 'Cameras'

    # Primary key
    id: Mapped[int] = mapped_column(primary_key=True)

    # Location
    make: Mapped[str] = mapped_column(String(MAX_CAMERA_MAKE_LENGTH))
    name: Mapped[str] = mapped_column(String(MAX_CAMERA_NAME_LENGTH))

    # Photo relationship: one-to-many
    photos: Mapped[list["Photo"]] = relationship(  # noqa
        back_populates='camera'
    )
