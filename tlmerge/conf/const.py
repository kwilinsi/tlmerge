from pathlib import Path

from platformdirs import PlatformDirs

DEFAULT_DATABASE_FILE = 'tlmerge.sqlite'
DEFAULT_CONFIG_FILE = 'config.tlmerge'

# If the user doesn't specify where to put the log, this is the log file
# location
DEFAULT_LOG_FILE: Path = (
        PlatformDirs('tlmerge', appauthor=False).user_log_path /
        'tlmerge.log'
)

ENV_VAR_PREFIX = 'TLMERGE'
