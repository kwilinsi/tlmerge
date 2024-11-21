from sqlalchemy import CheckConstraint, Float, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

# The maximum length of a lens make
MAX_LENS_MAKE_LENGTH = 75

# The maximum length of a lens model
MAX_LENS_MODEL_LENGTH = 100

# The maximum length of a lens spec
MAX_LENS_SPEC_LENGTH = 100


class Lens(Base):
    __tablename__ = 'Lenses'

    # Primary key
    id: Mapped[int] = mapped_column(primary_key=True)

    # Location
    make: Mapped[str | None] = mapped_column(String(MAX_LENS_MAKE_LENGTH))
    model: Mapped[str | None] = mapped_column(String(MAX_LENS_MODEL_LENGTH))
    spec: Mapped[str | None] = mapped_column(String(MAX_LENS_SPEC_LENGTH))

    # Focal length range
    min_focal_length: Mapped[float | None] = mapped_column(Float())  # mm
    max_focal_length: Mapped[float | None] = mapped_column(Float())  # mm

    # Aperture range
    lens_f_stops: Mapped[float | None] = mapped_column(Float())
    max_aperture_min_focal: Mapped[float | None] = mapped_column(Float())
    max_aperture_max_focal: Mapped[float | None] = mapped_column(Float())
    effective_max_aperture: Mapped[float | None] = mapped_column(Float())

    # Photo relationship: one-to-many
    photos: Mapped[list["Photo"]] = relationship(  # noqa
        back_populates='lens'
    )

    __table_args__ = (
        CheckConstraint(
            'make IS NOT NULL OR model IS NOT NULL OR spec IS NOT NULL'
        ),
        UniqueConstraint(
            make, model, spec, min_focal_length, max_focal_length,
            lens_f_stops, max_aperture_min_focal, max_aperture_max_focal,
            effective_max_aperture,
            name='all_unique'
        ),
    )
