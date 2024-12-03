# Overview

It's possible to configure `tlmerge` at four levels, prioritized as follows:

1. Command line arguments
2. Per-group configuration file
3. Per-date configuration file
4. Global configuration file
5. (Defaults)

For example, if you set the white balance in the global configuration file, this
can be overridden for particular dates using the per-date configuration file.
From there, it can be overridden again for a particular group. And to suppress all of these values, you can use a command line argument.

# Config Files

By default, configuration files are `yaml` files called `config.tlmerge`. You can add a config files to the project root, a date directory, or a group directory:

```
my_timelapse_project
  config.tlmerge      # global/project-level
  2024-01-01
    config.tlmerge    # date-level
    a
      config.tlmerge  # group-level
      ...
    ...
  ...
```

Config files follow a YAML structure.

# Options

This outlines all the configuration options available in `tlmerge`. Some options
are available everywhere, while others are only available at particular levels.

## Setup

### `project`

**CLI only**: `-p [PATH]` or `--project [PATH]`

Set the path to the timelapse project directory. If omitted, it defaults to the current working directory.

### `config`

**CLI only**: `-c [FILE]` or `--config [FILE]`

Set the config file name/path. The behavior of this flag is based on the `FILE` value:

- If `FILE` is a file name, it supercedes the default configuration file name. By default, all configuration files are named `config.tlmerge`.
- If `FILE` is a relative path, it's interpreted as the global config file relative to the project root (defined with `-p`).
- If `FILE` is an absolute path, it points to the configuration file This doesn't have to be inside the project. You can use this to share a global configuration file between multiple timelapse projects.

### `database`

- CLI: `-d [PATH]` or `--database [PATH]`
- Config file (global only): `database: <PATH>`

Set the path to the database file. By default, the database file is called `tlmerge.sqlite` and located in the project root directory.

### `make_config`

**CLI only**: `--make_config`

If this flag is present and the global config file doesn't exist, it's created using the default configuration.

## Logging

- CLI `--log <PATH>`
- Config file (global only): `log: <PATH>`

Set the path to the log file. If omitted, the default log file is `tlmerge.log` in a [platform-specific directory](https://platformdirs.readthedocs.io/en/latest/api.html#logs-directory).

The behavior of this configuration is based on the `PATH` value:

- If `PATH` is a file name, it's a file located in the timelapse project directory.
- If `PATH` is a relative path, it's resolved relative to the current working directory.
- If `PATH` is absolute, it points directly to the log file (which need not be in the timelapse project).
- If the `PATH` is omitted entirely, log messages are not written to any file. You can use this along with the `silent` flag to disable all logging.

Note that tlmerge uses a rotating handler for the log file, storing up to 5 backups of 5 MB each.

### `verbose`

- CLI: `-v` or `--verbose`
- Config file (global only): `verbose: True`

This enables verbose logging in the console, which is useful for debugging
purposes.

### `quiet`

- CLI: `-q` or `--quiet`
- Config file (global only): `quiet: True`

Do not log general information messages in the console. Only show warnings and
errors.

### `silent`

- CLI: `-s` or `--silent`
- Config file (global only): `silent: True`

Do not log anything in the console, even error messages. The program will not
produce any output.

Note that in some cases this may still log errors pertaining to the
configuration itself. This could happen if the CLI or configuration file is
malformed such that it encounters an error before reading the `silent` flag.

## Execution

### `workers`

- CLI: `--workers [NUM]`

Set the number of worker threads to use when multithreading in certain modes. This must be at least 1, although some tasks have a higher minimum and will ignore this value if it's too low. By default, tlmerge uses 20 workers.

### Max processing errros

- CLI: `--max_processing_errors [NUM]`

This is the maximum number of errors that are allowed while working with many workers. For example, when preprocessing all the timelapse phoots, this it he maximum number of photos that can fail before the program will halt. This allows recovery from one or two malformed photos without cancelling an entire operation. Set this to 0 to halt as soon as a single task fails.

Note that this only applies to errors encountered while using worker pools with 1 or more workers, not all errors during program execution. By default, this is 5.

### `sample`

- CLI: `--sample [<~>NUM]`

While iterating over the timelapse photos based on the mode, only process this many photo files. This makes it possible to preview everything to ensure it's functioning properly before running on the entire project.

Prefix the sample size with a tilde (~) to randomly sample photos instead of always sampling the first ones. (Randomization is done with a reasonable best-effort approach that employs some stratification without excessive performance compromises).

If you enable a sample without randomization, it will always sample photos strictly in order to ensure repeatability. This is opposed to the default behavior when not using a sample, in which the order that photos are scanned is not guaranteed.

If using the CLI, you can specify -1 to disable a sample enabled in the config file.

## Dates

### Date Format

- CLI: `--date_format [FORMAT]`
- Config file (global only): `date_format: [FORMAT]`

Set the format used for parsing and identifying the date directories. Defaults to `yyyy-mm-dd`. Any directories in the project root that do not conform to this format are not considered dates and are thus ignored.

### Exclude Dates

- CLI: `--exclude_dates [DATE ...]`
- Config file (global only):
```yaml
exclude_dates:
  - [DATE]
  ...
```

Specify zero or more dates to exclude. Groups and photos within these dates are neither scanned nor processed. This can be overwritten for individual dates with `include_dates`.

### Include Dates

- CLI: `--include_dates [DATE ...]`
- Config file (global only):
```yaml
include_dates:
  - [DATE]
  ...
```

Specify zero or more dates to specifically include. Groups and photos within these dates are scanned and processed even if found in the `exclude_dates` list. Note that all dates are included by default, and this is simply for overriding dates that were otherwise excluded.

## Groups

### Group Ordering

- CLI: `--group_ordering [POLICY]`
- Config file (global or date): `group_ordering: [POLICY]`

Set the group ordering policy. This controls the order in which groups are processed (when scanning/processing photos strictly in order). It also affects which directories are considered groups and which (if any) are ignored.

There are three ordering policies available:

- `abc`: This is the default ordering policy. It follows an intuitive spreadsheet-like ordering: `a`, `b`, `c`, ... `y`, `z`, `aa`, `ab`, ..., `az`, `ba`, etc. Directories containing any non-letter characters are ignored. This is case in-sensitive.
- `num`: Sort groups in numerical order. All group directory names are parsed as numbers and sorted. Directories that cannot be parsed are ignored. This *does* support decimals and negative numbers.
- `natural`: Sort groups in their natural, lexiographic order based on their names using defualt Python sorting. No directories are ignored.

### Exclude Groups

- CLI: `--exclude_groups [GROUP ...]`
- Config file (global only):
```yaml
exclude_groups:
  - [GROUP]
  ...
```

Specify zero or more groups to exclude. Photos within these groups are neither scanned nor processed. This can be overwritten for individual groups with `include_groups`.

When using the CLI or global config file, you must fully qualify group names with the associated date. For example, to exclude the group `a` on `2024-01-01` in the CLI, use `--exclude_groups 2024-01-01/a`.

### Include Groups

- CLI: `--include_groups [GROUP ...]`
- Config file (global only):
```yaml
include_groups:
  - [GROUP]
  ...
```

Specify zero or more groups to specifically include. Photos within these groups are scanned and processed even if found in the `exclude_groups` list. Note that all groups are included by default, and this is simply for overriding groups that were otherwise excluded.

When using the CLI or global config file, you must fully qualify group names with the associated date. (e.g. `--include_groups 2024-01-01/a`).

## Camera Settings

### White Balance

- CLI: `--white_balance [RED] [GREEN1] [BLUE] [GREEN2]`
- Config file (all):
```yaml
white_balance:
  red: [RED]
  green_1: [GREEN1]
  blue: [BLUE]
  green_2: [GREEN2]
```

Set the white balance multipliers for developing raw images. By default, no white balance correction is applied, with a 1.0 multiplier for each. See LibRaw documentation for more information about custom white balance.

### Chromatic Aberration

- CLI: `--chromatic_aberration [RED] [BLUE]`
- Config file (all):
```yaml
chromatic_aberration:
  red: [RED]
  blue: [BLUE]
```

Set the chromatic aberration multipliers for developing raw images. By default, no correction is applied, with a 1.0 multiplier for each. See LibRaw documentation for more information.

### Median Filter

- CLI: `--median_filter [PASSES]`
- Config file (all): `median_filter: [PASSES]`

Set the number of passes with a 3x3 median filter when developing raw images. Defaults to 0. See LibRaw documentation for more information.

### Dark Frame

- CLI: `--dark_frame [FILE]`
- Config file (all): `dark_frame: [FILE]`

Provide an optional dark frame to subtract noise from raw images. See LibRaw documentation for more information.

