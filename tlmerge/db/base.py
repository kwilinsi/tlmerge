from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase


# Base class for all ORM models
class Base(AsyncAttrs, DeclarativeBase):
    pass
