import logging
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from .base import Base

_log = logging.getLogger(__name__)


class DBManager:
    def __init__(self):
        self._engine = None
        self._session_maker = None

    @property
    def engine(self):
        if self._engine is None:
            raise ValueError('DB not yet initialized')
        return self._engine

    def initialize(self, path: Path) -> None:
        """
        Initialize the database.

        :param path: Path to the database file.
        :return: None
        """
        _log.debug('Initializing database engine...')
        self._engine = create_engine(f'sqlite:///{path}')
        self._session_maker = sessionmaker(bind=self._engine)

        with self._engine.begin() as conn:
            Base.metadata.create_all(conn)

        _log.debug(f'Initialized db: "{path}"')

    def session(self) -> Session:
        if self._session_maker is None:
            raise ValueError('DB not yet initialized')
        return self._session_maker()


DB = DBManager()
