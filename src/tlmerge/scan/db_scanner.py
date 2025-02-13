from collections.abc import Generator
from pathlib import Path

from sqlalchemy import func, select

from tlmerge.conf import ConfigManager
from tlmerge.db import DB, Photo


def iter_photo_records_from_db(
        config: ConfigManager,
        order: bool = False) -> Generator[tuple[str, str, str], None, None]:
    """
    Iterate over photos, but instead of traversing the file system as a regular
    scanner, load photos from the database.

    :param config: The `tlmerge` configuration.
    :param order: Whether to yield the photos in order. Defaults to False.
    :return: A generator yielding the date, group, and file name to uniquely
     identify each photo record in the project (or, if using a sample, only a
     subset of the photos).
    """

    sample, s_random, s_size = config.root.sample_details()

    # This is the base SQLAlchemy query to load photo records
    stmt = select(Photo.date, Photo.group, Photo.file_name)

    if s_random:
        # For a randomized sample, we randomize all the records and then limit
        # to the sample size.
        stmt = stmt.order_by(func.random()).limit(s_size)

        # If the output should be ordered, nest this in a subquery, and
        # then order on top of that
        if order:
            stmt = stmt.subquery()
            stmt = select(stmt).order_by(
                stmt.c.date, stmt.c.group, stmt.c.file_name
            )
    else:
        # If the output is ordered, and we don't need a randomized sample, then
        # we can apply order_by on the base statement
        if order:
            stmt = stmt.order_by(Photo.date, Photo.group, Photo.file_name)

        # For a non-randomized sample, just limit the results
        if sample:
            stmt = stmt.limit(s_size)

    # Execute the query, and yield the results
    with DB.session() as session:
        yield from session.execute(stmt)


def iter_photo_paths_from_db(
        config: ConfigManager,
        order: bool = False) -> Generator[Path, None, None]:
    """
    This is a convenience method on `iter_photo_records_from_db()` that,
    rather than yielding tuples with the date, group, and file name of each
    photo, instead yields complete `pathlib` `Paths` to each photo file.

    :param config: The `tlmerge` configuration.
    :param order: Whether to yield the photos in order. Defaults to False.
    :return: A generator yielding the Path to every photo in the project (or,
     if using a sample, only a subset of the photos).
    """

    root = config.root.project()

    for dt, grp, file in iter_photo_records_from_db(config, order):
        yield root / dt / grp / file
