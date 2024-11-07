from sqlalchemy import Float, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

# The maximum length of a lens id
MAX_LENS_ID_LENGTH = 100

# The maximum length of a lens spec
MAX_LENS_SPEC_LENGTH = 100

# The maximum length of a lens type
MAX_LENS_TYPE_LENGTH = 100

# The maximum length of a lens name
MAX_LENS_NAME_LENGTH = 100


class Lens(Base):
    __tablename__ = 'Lenses'

    # Primary key
    id: Mapped[int] = mapped_column(primary_key=True)

    # Location
    id_str: Mapped[str] = mapped_column(String(MAX_LENS_ID_LENGTH))
    spec: Mapped[str] = mapped_column(String(MAX_LENS_SPEC_LENGTH))
    type: Mapped[str] = mapped_column(String(MAX_LENS_TYPE_LENGTH))
    name: Mapped[str] = mapped_column(String(MAX_LENS_NAME_LENGTH))

    # Focal length and focus
    min_focal_length: Mapped[float] = mapped_column(Float)
    max_focal_length: Mapped[float] = mapped_column(Float)
    focus_distance: Mapped[float] = mapped_column(Float)
    field_of_view: Mapped[float] = mapped_column(Float)  # in degrees

    # Aperture
    lens_f_stops: Mapped[float] = mapped_column(Float)
    max_aperture_min_focal: Mapped[float] = mapped_column(Float)
    max_aperture_max_focal: Mapped[float] = mapped_column(Float)
    effective_max_aperture: Mapped[float] = mapped_column(Float)

    # Photo relationship: one-to-many
    photos: Mapped[list["Photo"]] = relationship(  # noqa
        back_populates='lens'
    )
