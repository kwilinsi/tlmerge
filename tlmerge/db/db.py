import logging
from pathlib import Path

from sqlalchemy.ext.asyncio import (AsyncEngine, AsyncSession,
                                    async_sessionmaker, create_async_engine)

from .base import Base

_log = logging.getLogger(__name__)


class DBManager:
    def __init__(self):
        self._engine: AsyncEngine | None = None
        self._session_maker: async_sessionmaker[AsyncSession] | None = None

    @property
    def engine(self):
        if self._engine is None:
            raise ValueError('DB not yet initialized')
        return self._engine

    async def initialize(self, path: Path) -> None:
        """
        Initialize the database.

        :param path: Path to the database file.
        :return: None
        """

        _log.debug('Initializing database engine...')
        self._engine = create_async_engine(f'sqlite+aiosqlite:///{path}')
        self._session_maker = async_sessionmaker(bind=self._engine)

        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        _log.debug(f'Initialized db: "{path}"')

    def session(self):
        if self._session_maker is None:
            raise ValueError('DB not yet initialized')
        return self._session_maker()


DB = DBManager()
