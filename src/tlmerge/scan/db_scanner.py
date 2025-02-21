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

    # Check whether to only scan a sample
    root_cfg = config.root
    sample, s_random, s_size = root_cfg.sample_details()

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

    # Get the excluded dates
    exclude_dates = root_cfg.exclude_dates() - root_cfg.include_dates()

    # Execute the query, and yield the results
    with DB.session() as session:
        for dt, grp, file in session.execute(stmt):
            # Ignore excluded dates
            if dt in exclude_dates:
                continue

            # Ignore exluded groups
            dt_cfg = config[dt]
            if grp in dt_cfg.exclude_groups() and \
                    file not in dt_cfg.include_groups():
                continue

            # Ignore excluded photos
            grp_cfg = config[dt, grp]
            if file in grp_cfg.exclude_photos() and \
                    file not in grp_cfg.include_photos():
                continue

            # Yield this photo file, now that we know it's not excluded
            yield dt, grp, file


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
