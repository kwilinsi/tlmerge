# tlmerge

tlmerge is a Python program for developing high-bitrate, visually lossless
timelapses videos from raw images.

> [!IMPORTANT]
> If you are not familiar with the details of raw photo development, you should
> read the documentation [here](docs/raw_processing.md) before using tlmerge.

tlmerge is designed with large timelapses in mind, especially those spanning
multiple days. It supports variable frame rates, changing camera settings, and
dynamic white balance. Images are processed
with [rawpy](https://github.com/letmaik/rawpy)/[LibRaw](https://www.libraw.org/)
and assembled with [ffmpeg](https://ffmpeg.org/).

---

**Note:** tlmerge is currently in active development, and thus not all features
are fully implemented. It works best via the CLI, and only the scanning and
preprocessing stages are completely functional.

# Installation

Currently, you can install tlmerge by cloning the GitHub respository:

```
git clone https://github.com/kwilinsi/tlmerge
cd tlmerge
```

It's recommended that you
[create a virtual environment](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#create-and-use-virtual-environments).
Activate it, and then install the dependencies:

```
pip install -r requirements.txt --upgrade
```

Then run tlmerge using the CLI:

```
python -m tlmerge --help
```

# Project Setup

tlmerge is opinionated about the project structure. Photos are divided first
into dates and then groups. An example structure is shown below (for brevity,
not all photos and groups are shown).

```
my_timelapse_project/
├── 2024-01-01/
│   ├── a/
│   │   ├── 0000.dng
│   │   ├── 0001.dng
│   │   └── ...
│   ├── b
│   ├── c
│   ├── ...
│   ├── z
│   ├── aa
│   └── ab
├── 2024-01-02/
│   └── a
├── 2024-01-03
├── 2024-01-10
└── 2024-01-11
```

## Dates

tlmerge is primarily designed for large-scale timelapses spanning multiple days.
As such, all projects must contain at least one date directory at the top level.
The date format string can be customized, but all dates must use the same
format.

## Groups

Groups allow one level of subdivision below dates. When assembling the
timelapse, each group is converted to a separate video file. Groups are not
combined.

For this reason, groups are particularly useful when changing camera settings or
interval between captures. When you make final touches while editing the final
video, it's easy to tweak the color grade or framerate separately for each
group.

In the example above, groups are named with the `abc` ordering policy: `a`,
`b`, ..., `z`, `aa`, `ab`, etc. It's also possible to use numbers or any text in
natural sort order.

## Configuration

Configuration is supported at all levels for maximum flexibility. You can place
`config.tlmere` files in the project root, inside a date, or inside a group.
Each config file applies only to the files/folders at that level and their
children.

For example, the file `./2024-01-01/config.tlmerge` applies to all the groups
and photos taken on 2024-01-01. Another file at `./2024-01-01/a/config.tlmerge`
overrides that configuration specifically for photos in the group `a`.

It's also possible to override all configuration files through command line
arguments.

You can find more information about tlmerge
configuration [here](docs/config.md).
