import os
from pathlib import Path

import pytest

from tlmerge.conf import (DEFAULT_DATABASE_FILE, DEFAULT_LOG_FILE,
                          ENV_VAR_PREFIX, LogLevel, RootConfig)


def test_root_init(tmp_path: Path, monkeypatch):
    # Try all project types in the constructor for RootConfig: Path, something
    # PathLike, str, and None, using environment variable. Also try some
    # invalid objects

    # Remove environment variable if it exists to ensure we don't fall back
    # on that
    monkeypatch.delenv(f'{ENV_VAR_PREFIX}_PROJECT', raising=False)

    RootConfig(tmp_path)  # test Path
    RootConfig(str(tmp_path))  # test string

    # Create an arbitrary subdirectory so we can test os.DirEntry, which is a
    # builtin PathLike type
    tmp_path.joinpath('subdir').mkdir()
    RootConfig(next(os.scandir(tmp_path)))

    # Test a couple other random (invalid) project paths
    with pytest.raises(ValueError):
        RootConfig(None)  # No path given and nothing in environment variable
    with pytest.raises(ValueError):
        RootConfig('')  # Empty path
    with pytest.raises(ValueError):
        RootConfig('a')  # Invalid path
    with pytest.raises(ValueError):
        RootConfig(1)  # noqa
    with pytest.raises(ValueError):
        RootConfig(tmp_path.joinpath('nonexistent'))  # Nonexistent dir

    file = tmp_path.joinpath('file')
    file.write_text('')
    with pytest.raises(ValueError):
        RootConfig(file)  # Given file instead of directory

    # Test getting project path from environment variable
    monkeypatch.setenv(f'{ENV_VAR_PREFIX}_PROJECT', str(tmp_path))
    RootConfig(None)


def test_root_project(tmp_path: Path):
    # This functions much like testing the init function except that it also
    # checks the project() getter

    c = RootConfig(tmp_path)
    assert c.project() == tmp_path

    sub1 = tmp_path.joinpath('subdir1')
    sub1.mkdir()
    c.set_project(next(os.scandir(tmp_path)))  # os.DirEntry (PathLike)
    assert c.project() == sub1

    sub2 = tmp_path.joinpath('subdir2')
    sub2.mkdir()
    c.set_project(sub2)  # pathlib.Path
    assert c.project() == sub2

    sub3 = tmp_path.joinpath('subdir3')
    sub3.mkdir()
    c.set_project(str(sub3))  # str
    assert c.project() == sub3

    with pytest.raises(ValueError):
        c.set_project(None)  # noqa
    assert c.project() == sub3  # No change

    with pytest.raises(ValueError):
        c.set_project('')  # noqa
    assert c.project() == sub3  # No change

    with pytest.raises(ValueError):
        c.set_project(1)  # noqa
    assert c.project() == sub3  # No change

    with pytest.raises(ValueError):
        c.set_project(tmp_path.joinpath('nonexistent'))
    assert c.project() == sub3  # No change

    file = tmp_path.joinpath('file')
    file.write_text('')
    with pytest.raises(ValueError):
        c.set_project(file)  # File instead of directory
    assert c.project() == sub3  # No change


def test_root_database(tmp_path: Path):
    c = RootConfig(tmp_path)

    db_default = tmp_path / DEFAULT_DATABASE_FILE
    db_other = tmp_path / 'db.sqlite'

    # Should work with nonexistent files
    c.set_database(db_default)
    assert c.database() == db_default
    c.set_database(db_other)
    assert c.database() == db_other

    # Test with strings to nonexistent files
    c.set_database(str(db_default))
    assert c.database() == db_default
    c.set_database(str(db_other))
    assert c.database() == db_other

    # Create files
    db_default.write_text('')
    db_other.write_text('')

    # Should still work with ones that exist
    c.set_database(db_default)
    assert c.database() == db_default
    c.set_database(db_other)
    assert c.database() == db_other

    # Same with strings for files that exist
    c.set_database(str(db_default))
    assert c.database() == db_default
    c.set_database(str(db_other))
    assert c.database() == db_other

    # Replace files with directories
    db_default.unlink()
    db_other.unlink()
    db_default.mkdir()
    db_other.mkdir()

    fallback = tmp_path / 'db file'
    c.set_database(fallback)

    # Should fail now on Paths to directories
    with pytest.raises(ValueError):
        c.set_database(db_default)
    assert c.database() == fallback  # No change
    with pytest.raises(ValueError):
        c.set_database(db_other)
    assert c.database() == fallback  # No change

    # Should also fail with strings
    with pytest.raises(ValueError):
        c.set_database(str(db_default))
    assert c.database() == fallback  # No change
    with pytest.raises(ValueError):
        c.set_database(str(db_other))
    assert c.database() == fallback  # No change

    # Should fail on None, empty string, and invalid data types
    with pytest.raises(ValueError):
        c.set_database(None)  # noqa
    assert c.database() == fallback  # No change
    with pytest.raises(ValueError):
        c.set_database('')  # noqa
    assert c.database() == fallback  # No change
    with pytest.raises(ValueError):
        c.set_database(1)  # noqa
    assert c.database() == fallback  # No change


def test_root_log(tmp_path):
    c = RootConfig(tmp_path)

    file1 = tmp_path / DEFAULT_LOG_FILE.name
    file2 = tmp_path / (DEFAULT_LOG_FILE.name + '2')

    # Should work with a nonexistent file Path and str
    c.set_log(file1)
    assert c.log() == file1
    c.set_log(str(file2))
    assert c.log() == file2

    # Create files
    file1.write_text('')
    file2.write_text('')

    # Should still work with files that exist
    c.set_log(file1)
    assert c.log() == file1
    c.set_log(str(file2))
    assert c.log() == file2

    # Disable with None, empty string, blank string, and words like "off"
    c.set_log(None)
    assert c.log() is None
    c.set_log('')
    assert c.log() is None
    c.set_log('   \t\n ')
    assert c.log() is None
    c.set_log('off')
    assert c.log() is None
    c.set_log('disable')
    assert c.log() is None
    c.set_log('0')
    assert c.log() is None
    c.set_log(False)  # noqa
    assert c.log() is None

    # Replace file with directory
    file1.unlink()
    file1.mkdir()

    # Should fail now on Paths and strings to directories
    with pytest.raises(ValueError):
        c.set_log(file1)
    with pytest.raises(ValueError):
        c.set_log(str(file1))

    # Should fail on invalid data types, like integers
    with pytest.raises(ValueError):
        c.set_log(1)  # noqa

    assert c.log() is None  # No change


def test_root_log_level(tmp_path):
    c = RootConfig(tmp_path)

    # Test LogLevel inputs
    c.set_log_level(LogLevel.VERBOSE)
    assert c.log_level() == LogLevel.VERBOSE
    c.set_log_level(LogLevel.DEFAULT)
    assert c.log_level() == LogLevel.DEFAULT
    c.set_log_level(LogLevel.QUIET)
    assert c.log_level() == LogLevel.QUIET
    c.set_log_level(LogLevel.SILENT)
    assert c.log_level() == LogLevel.SILENT

    # Test None, which should reset to DEFAULT
    c.set_log_level(None)
    assert c.log_level() == LogLevel.DEFAULT

    # Test string inputs
    c.set_log_level('verbose')
    assert c.log_level() == LogLevel.VERBOSE
    c.set_log_level('default')
    assert c.log_level() == LogLevel.DEFAULT
    c.set_log_level('quiet')
    assert c.log_level() == LogLevel.QUIET
    c.set_log_level('silent')
    assert c.log_level() == LogLevel.SILENT

    # You can also specify levels by their enumerated integer value
    c.set_log_level(0)  # noqa
    assert c.log_level() == LogLevel.VERBOSE
    c.set_log_level(1)  # noqa
    assert c.log_level() == LogLevel.DEFAULT
    c.set_log_level(2)  # noqa
    assert c.log_level() == LogLevel.QUIET
    c.set_log_level(3)  # noqa
    assert c.log_level() == LogLevel.SILENT

    # Mixed case strings and leading/trailing whitespace
    c.set_log_level('Verbose')  # noqa
    assert c.log_level() == LogLevel.VERBOSE
    c.set_log_level('DEFAULT')  # noqa
    assert c.log_level() == LogLevel.DEFAULT
    c.set_log_level('\nqUiEt  ')  # noqa
    assert c.log_level() == LogLevel.QUIET

    # Invalid data types, such as out of bounds integers, should raise errors
    with pytest.raises(ValueError):
        c.set_log_level(8)  # noqa

    # Unknown strings also raise errors
    with pytest.raises(ValueError):
        c.set_log_level('warn')  # noqa

    assert c.log_level() == LogLevel.QUIET  # No change


def test_root_verbose(tmp_path):
    c = RootConfig(tmp_path)

    # Test getter with all possible log states
    c.set_log_level(LogLevel.VERBOSE)
    assert c.verbose() is True
    c.set_log_level(LogLevel.DEFAULT)
    assert c.verbose() is False
    c.set_log_level(LogLevel.QUIET)
    assert c.verbose() is False
    c.set_log_level(LogLevel.SILENT)
    assert c.verbose() is False

    # Test enabling verbose
    c.set_verbose(True)
    assert c.verbose() is True
    assert c.log_level() == LogLevel.VERBOSE

    # Disabling when currently verbose should make it DEFAULT
    c.set_verbose(False)
    assert c.verbose() is False
    assert c.log_level() == LogLevel.DEFAULT

    # 1/0 are accepted aliases for True/False
    c.set_verbose(1)  # noqa
    assert c.verbose() is True
    c.set_verbose(0)  # noqa
    assert c.verbose() is False

    # If the log level is something else (e.g. QUIET), setting verbose() to
    # False should have no effect
    c.set_log_level(LogLevel.QUIET)
    assert c.verbose() is False
    c.set_verbose(False)
    assert c.verbose() is False

    # None and invalid data types should raise errors
    with pytest.raises(ValueError):
        c.set_verbose(None)  # noqa
    with pytest.raises(ValueError):
        c.set_verbose(1.2)  # noqa

    assert c.log_level() == LogLevel.QUIET  # No change


def test_root_quiet(tmp_path):
    c = RootConfig(tmp_path)

    # Test getter with all possible log states
    c.set_log_level(LogLevel.VERBOSE)
    assert c.quiet() is False
    c.set_log_level(LogLevel.DEFAULT)
    assert c.quiet() is False
    c.set_log_level(LogLevel.QUIET)
    assert c.quiet() is True
    c.set_log_level(LogLevel.SILENT)
    assert c.quiet() is False

    # Test enabling quiet
    c.set_quiet(True)
    assert c.quiet() is True
    assert c.log_level() == LogLevel.QUIET

    # Disabling when currently quiet should make it DEFAULT
    c.set_quiet(False)
    assert c.quiet() is False
    assert c.log_level() == LogLevel.DEFAULT

    # 1/0 are accepted aliases for True/False
    c.set_quiet(1)  # noqa
    assert c.quiet() is True
    c.set_quiet(0)  # noqa
    assert c.quiet() is False

    # If the log level is something else (e.g. SILENT), setting quiet() to
    # False should have no effect
    c.set_log_level(LogLevel.SILENT)
    assert c.quiet() is False
    c.set_quiet(False)
    assert c.quiet() is False

    # None and invalid data types should raise errors
    with pytest.raises(ValueError):
        c.set_quiet(None)  # noqa
    with pytest.raises(ValueError):
        c.set_quiet(1.2)  # noqa

    assert c.log_level() == LogLevel.SILENT  # No change


def test_root_silent(tmp_path):
    c = RootConfig(tmp_path)

    # Test getter with all possible log states
    c.set_log_level(LogLevel.VERBOSE)
    assert c.silent() is False
    c.set_log_level(LogLevel.DEFAULT)
    assert c.silent() is False
    c.set_log_level(LogLevel.QUIET)
    assert c.silent() is False
    c.set_log_level(LogLevel.SILENT)
    assert c.silent() is True

    # Test enabling silent
    c.set_silent(True)
    assert c.silent() is True
    assert c.log_level() == LogLevel.SILENT

    # Disabling when currently silent should make it DEFAULT
    c.set_silent(False)
    assert c.silent() is False
    assert c.log_level() == LogLevel.DEFAULT

    # 1/0 are accepted aliases for True/False
    c.set_silent(1)  # noqa
    assert c.silent() is True
    c.set_silent(0)  # noqa
    assert c.silent() is False

    # If the log level is something else (e.g. VERBOSE), setting silent() to
    # False should have no effect
    c.set_log_level(LogLevel.VERBOSE)
    assert c.silent() is False
    c.set_silent(False)
    assert c.silent() is False

    # None and invalid data types should raise errors
    with pytest.raises(ValueError):
        c.set_silent(None)  # noqa
    with pytest.raises(ValueError):
        c.set_silent(1.2)  # noqa

    assert c.log_level() == LogLevel.VERBOSE  # No change


def test_root_workers(tmp_path):
    c = RootConfig(tmp_path)

    # Try a few numbers
    c.set_workers(1)
    assert c.workers() == 1
    c.set_workers(50)
    assert c.workers() == 50
    c.set_workers(10000)
    assert c.workers() == 10000

    # Zero and negatives should fail
    with pytest.raises(ValueError):
        c.set_workers(0)
    with pytest.raises(ValueError):
        c.set_workers(-1)
    with pytest.raises(ValueError):
        c.set_workers(-50)

    # Floats, None, and other invalid data types should fail
    with pytest.raises(ValueError):
        c.set_workers(2.5)  # noqa
    with pytest.raises(ValueError):
        c.set_workers(None)  # noqa
    with pytest.raises(ValueError):
        c.set_workers('two')  # noqa

    assert c.workers() == 10000  # No change


def test_root_max_processing_errors(tmp_path):
    c = RootConfig(tmp_path)

    # Try a few numbers
    c.set_max_processing_errors(1)
    assert c.max_processing_errors() == 1
    c.set_max_processing_errors(50)
    assert c.max_processing_errors() == 50
    c.set_max_processing_errors(10000)
    assert c.max_processing_errors() == 10000

    # Zero and negatives should fail
    with pytest.raises(ValueError):
        c.set_max_processing_errors(0)
    with pytest.raises(ValueError):
        c.set_max_processing_errors(-1)
    with pytest.raises(ValueError):
        c.set_max_processing_errors(-50)

    # Floats, None, and other invalid data types should fail
    with pytest.raises(ValueError):
        c.set_max_processing_errors(2.5)  # noqa
    with pytest.raises(ValueError):
        c.set_max_processing_errors(None)  # noqa
    with pytest.raises(ValueError):
        c.set_max_processing_errors('two')  # noqa

    assert c.max_processing_errors() == 10000  # No change


def test_root_sample(tmp_path):
    c = RootConfig(tmp_path)

    # Regular sample
    c.set_sample('12')
    assert c.sample() == '12'
    assert c.sample_details() == (True, False, 12)
    assert c.sample_size() == 12

    # Random sample
    c.set_sample('~1')
    assert c.sample() == '~1'
    assert c.sample_details() == (True, True, 1)
    assert c.sample_size() == 1

    # No sample
    c.set_sample(None)
    assert c.sample() is None
    assert c.sample_details() == (False, False, -1)
    assert c.sample_size() == -1

    # Invalid data types should fail
    with pytest.raises(ValueError):
        c.set_sample(2.5)  # noqa
    with pytest.raises(ValueError):
        c.set_sample(True)  # noqa

    assert c.sample() is None  # No change


def test_root_date_format(tmp_path):
    c = RootConfig(tmp_path)

    # Basic test in expected format
    c.set_date_format('%Y%m%d')
    assert c.date_format() == '%Y%m%d'

    # Test substitution of yyyy/mm/dd format strings
    c.set_date_format('YYYY/MM/DD')
    assert c.date_format() == '%Y/%m/%d'
    c.set_date_format('Md')
    assert c.date_format() == '%m%d'
    c.set_date_format('\\date: %Y-mm-dD')
    assert c.date_format() == 'date: %Y-%m-%d'
    c.set_date_format('m/d/yy')
    assert c.date_format() == '%m/%d/%y'

    # None and other invalid data types should fail
    with pytest.raises(ValueError):
        c.set_date_format(None)  # noqa
    with pytest.raises(ValueError):
        c.set_date_format(True)  # noqa

    assert c.date_format() == '%m/%d/%y'  # No change


def test_root_exclude_dates(tmp_path):
    c = RootConfig(tmp_path)

    # Test setting and adding
    c.set_exclude_dates(('2000-12-31', '2001-01-01'))
    assert c.exclude_dates() == {'2000-12-31', '2001-01-01'}
    c.add_exclude_dates(('2001-04-12',))
    assert c.exclude_dates() == {'2000-12-31', '2001-01-01', '2001-04-12'}

    # Adding a date twice does nothing
    c.add_exclude_dates(['2001-04-12'])
    assert c.exclude_dates() == {'2000-12-31', '2001-01-01', '2001-04-12'}

    # Individual values not in a tuple work too thanks to Pydantic
    c.set_exclude_dates('2000-06-01')  # noqa
    # Setting should replace existing entries
    assert c.exclude_dates() == {'2000-06-01'}

    # None should fail
    with pytest.raises(ValueError):
        c.set_exclude_dates(None)  # noqa

    assert c.exclude_dates() == {'2000-06-01'}  # No change


def test_root_include_dates(tmp_path):
    c = RootConfig(tmp_path)

    # Test setting and adding
    c.set_include_dates(('2000-12-31', '2001-01-01'))
    assert c.include_dates() == {'2000-12-31', '2001-01-01'}
    c.add_include_dates(('2001-04-12',))
    assert c.include_dates() == {'2000-12-31', '2001-01-01', '2001-04-12'}

    # Adding a date twice does nothing
    c.add_include_dates(['2001-04-12'])
    assert c.include_dates() == {'2000-12-31', '2001-01-01', '2001-04-12'}

    # Individual values not in a tuple work too thanks to Pydantic
    c.set_include_dates('2000-06-01')  # noqa
    # Setting should replace existing entries
    assert c.include_dates() == {'2000-06-01'}

    # None and individual dates without an iterable should fail
    with pytest.raises(ValueError):
        c.set_include_dates(None)  # noqa

    assert c.include_dates() == {'2000-06-01'}  # No change
