import logging
from pathlib import Path

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session

from .base import Base

_log = logging.getLogger(__name__)


class DBManager:
    """
    The DBManager facilitates a connection to the database through SQLAlchemy
    (with `initialize()`) and hands out sessions with a session maker.
    """

    def __init__(self) -> None:
        """
        Initialize the DBManager, creating a new SQLAlchemy engine and session
        maker to initialize later.

        :return: None
        """

        self._engine: Engine | None = None
        self._session_maker: sessionmaker | None = None

    @property
    def engine(self) -> Engine:
        """
        Get the SQLAlchemy engine associated with this DBManager. This is a
        property to enforce read-only access to the engine attribute of the
        manager.

        :return: The SQLAlchemy engine.
        :raises ValueError: If the engine was not initialized.
        """

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
        """
        Get a new database session. Note that this method is thread-safe, but
        the sessions it returns are not (they must only be used by the thread
        that created them).

        :return: A new session.
        :raises ValueError: If the database engine backing the sessions was not
         initialized.
        """

        if self._session_maker is None:
            raise ValueError('DB not yet initialized')
        return self._session_maker()


DB = DBManager()
