from asyncio import AbstractEventLoop, Event
from collections.abc import Callable, Iterable
from datetime import datetime
from pathlib import Path
from queue import SimpleQueue
from threading import Thread

from exiftool import ExifToolHelper
from exiftool.exceptions import ExifToolException

from tlmerge.conf import CONFIG
from tlmerge.db import Camera, Lens, Photo


def parse_date_time(dt_string: str,
                    formats: Iterable[str] = (
                            '%Y:%m:%d %H:%M:%S.%f%z',
                            '%Y:%m:%d %H:%M:%S.%f',
                            '%Y:%m:%d %H:%M:%S%z',
                            '%Y:%m:%d %H:%M:%S',
                            '%Y-%m-%d %H:%M:%S.%f%z',
                            '%Y-%m-%d %H:%M:%S.%f',
                            '%Y-%m-%d %H:%M:%S%z',
                            '%Y-%m-%d %H:%M:%S',
                    )) -> datetime:
    """
    Try the given datetime formats to parse a datetime string.
    :param dt_string: The string to parse.
    :param formats: The list of datetime formats to try. The first one that
     matches is used.
    :return: The parsed datetime object.
    :raises ValueError: If the input string is None, empty, or otherwise
     cannot be parsed.
    """

    if dt_string is None or not dt_string.strip():
        raise ValueError("Datetime string is missing: can't parse it")

    for fmt in formats:
        try:
            return datetime.strptime(dt_string, fmt)
        except ValueError:
            pass

    raise ValueError(f'Unable to parse datetime string "{dt_string}"')


class PhotoExifRecord:
    def __init__(self, path: Path | str) -> None:
        """
        Initialize an EXIF loader record, which helps control the process of
        batch loading EXIF data from many images.

        This includes an asyncio Event, which is fired

        :param path: The path to the image file.
        """

        self.path: str = str(path)
        self.event: Event = Event()
        self.exif_raw: dict[str, any] | None = None
        self.exif_fmt: dict[str, any] | None = None
        self.exception: ExifToolException | None = None

    def get(self,
            *tags: str,
            fmt: bool = True,
            cast: Callable[[any], any] | None = None,
            opt: bool = True) -> any:
        """
        Get the EXIF data associated with the given tag.

        :param tags: The desired tag name. If multiple tags are given, they
         are each tried in succession.
        :param fmt: Whether to use the formatted EXIF data (True) or the raw
         data (False). Defaults to True.
        :param cast: An optional cast function to coerce the data into the
         correct format. Defaults to None.
        :param opt: Whether this value is optional. If True and the tag
         doesn't exist, this returns None. Otherwise, if False and the tag
         doesn't exist, this raises a KeyError. Defaults to True.
        :return: The value associated with the tag, or None if it couldn't be
         found and was optional.
        :raises KeyError: If the tag doesn't exist and is not optional.
        :raises ValueError: If the casting function fails.
        """

        # Use formatted or raw exif data
        exif = self.exif_fmt if fmt else self.exif_raw

        # Try each tag until a match is found
        error = None
        for i, tag in enumerate(tags):
            last_tag: bool = i + 1 == len(tags)

            # Get the tag value
            val = exif.get(tag)

            # If no value found, continue to the next tag
            if val is None:
                if opt:
                    continue
                else:
                    # If it's mandatory and not found, raise an exception
                    if error is None:
                        error = KeyError(
                            f"Mandatory tag {tag} not found in "
                            f"{'formatted' if fmt else 'raw'} EXIF data "
                            f"for {CONFIG.root.rel_path(self.path)}"
                        )
                    if last_tag:
                        raise error

            # If there's no cast function, return the value
            if cast is None:
                return val

            # Try casting
            try:
                return cast(val)
            except Exception:
                if error is None:
                    error = ValueError(
                        f"Tag {tag} had unexpected value \"{val}\" "
                        f"for {CONFIG.root.rel_path(self.path)}; "
                        f"couldn't cast to {cast}"
                    )

        # No matching tags
        if error is not None:
            raise error
        return None

    def apply_metadata(self, photo: Photo) -> None:
        """
        Apply the EXIF data to the given database Photo record.

        :param photo: The database Photo record.
        :return: None
        :raises ExifToolException: If there was an error obtaining the EXIF
         data for this photo.
        :raises KeyError: If the EXIF data for a mandatory (non-null) column in
         the database is missing.
        :raises ValueError: If the EXIF data for a mandatory (non-null) column
         in the database is in an unexpected format/type.
        """

        # If there was an error getting the EXIF data, raise it now
        if self.exception is not None:
            raise self.exception

        # Capture time
        photo.time_taken = self.get(
            'Composite:SubSecDateTimeOriginal',
            'EXIF:DateTimeOriginal',
            opt=False,
            cast=parse_date_time
        )

        # File size (kilobytes)
        photo.file_size = int(
            self.get('File:FileSize', fmt=False, opt=False, cast=int) / 1000
        )

        # Capture metadata
        photo.iso = self.get('EXIF:ISO', cast=int)
        photo.shutter_speed = self.get('Composite:ShutterSpeed', cast=str)
        photo.aperture = self.get('Composite:Aperture', cast=float)
        photo.focal_length = self.get('EXIF:FocalLength',
                                      fmt=False, cast=float)
        photo.auto_focus = bool(self.get('Composite:AutoFocus',
                                         fmt=False, cast=int))
        photo.focus_distance = self.get('MakerNotes:FocusDistance',
                                        fmt=False, cast=float)
        photo.field_of_view = self.get('Composite:FOV', fmt=False,
                                       cast=lambda v: float(v.split()[0]))
        photo.exposure_difference = self.get(
            'MakerNotes:ExposureDifference', fmt=False, cast=float
        )

        # Photo size
        photo.raw_width = self.get('EXIF:ImageWidth', opt=False, cast=int)
        photo.raw_height = self.get('EXIF:ImageHeight',
                                    opt=False, cast=int)

        # Camera info
        camera: Camera = photo.camera
        camera.make = self.get('EXIF:Make')
        camera.model = self.get('EXIF:Model')

        # Lens info
        lens: Lens = photo.lens
        lens.make = self.get('EXIF:LensMake')
        lens.model = self.get('EXIF:LensModel', 'Composite:LensID')
        lens.spec = self.get('Composite:LensSpec')

        # Lens focal length range
        lens.min_focal_length = self.get('MakerNotes:MinFocalLength',
                                         fmt=False, cast=float)
        lens.max_focal_length = self.get('MakerNotes:MaxFocalLength',
                                         fmt=False, cast=float)

        # Lens aperture range
        lens.lens_f_stops = self.get('MakerNotes:LensFStops', cast=float)
        lens.max_aperture_min_focal = self.get(
            'MakerNotes:MaxApertureAtMinFocal', cast=float
        )
        lens.max_aperture_max_focal = self.get(
            'MakerNotes:MaxApertureAtMaxFocal', cast=float
        )
        lens.effective_max_aperture = self.get(
            'MakerNotes:EffectiveMaxAperture', cast=float
        )


class ExifWorker(Thread):
    def __init__(self,
                 queue: SimpleQueue[PhotoExifRecord | None],
                 event_loop: AbstractEventLoop,
                 worker_num: int) -> None:
        """
        Initialize an ExifLoader thread. This continuously loads photos from
        the queue to get their metadata with PyExifTool. It runs until
        receiving None from the queue, at which point it exits.

        By default, ExifTool tries to make its output user-friendly. For
        example, it changes the file size "15908707" to "16 MB" and the Lens
        Spec "18 55 3.5 5.6 142" to "18-55mm f/3.5-5.6 G VR AF-P".

        However, the raw version with numbers is sometimes more useful to a
        computer. Thus, PyExifTool enables the -n flag by default, which
        turns off the formatting.

        However, no one version fits all. Sometimes the formatted approach is
        more useful, such as in the case with the lens information (as you'd
        otherwise need access to the same massive lookup table to interpret
        the lens ID). Other times, as with the file size, the raw version may
        be better: such as if you want the size in kilobytes with more
        precision.

        Therefore, this worker runs two simultaneous instances of ExifTool: one
        with the -n flag and one without it.

        PyExifTool also uses the -G flag by default to specify the tag groups.
        This is retained even in the raw version without -n.

        :param queue: The queue with records to populate with EXIF data.
        :param event_loop: The event loop on which to set the asyncio.Event for
         each record in the queue.
        :param worker_num: The number of this EXIF loader thread. This is just
         used for setting the thread name in log messages.
        """

        super().__init__(name=f'exif_{worker_num}')

        self.exif_raw: ExifToolHelper = ExifToolHelper()  # Default: -G and -n
        self.exif_fmt: ExifToolHelper = ExifToolHelper(common_args=['-G'])
        self.queue: SimpleQueue[PhotoExifRecord | None] = queue
        self.event_loop: AbstractEventLoop = event_loop

    def run(self) -> None:
        """
        Run this worker. First, this opens the PyExifTool helpers (one with raw
        output and the other with user-friendly output). Then it repeatedly
        loads records from the queue to populate with EXIF data.

        When it gets None from the queue, it exits.

        :return: None
        """

        with self.exif_raw:
            with self.exif_fmt:
                while (record := self.queue.get()) is not None:
                    self._load_metadata(record)

    def _load_metadata(self, record: PhotoExifRecord) -> None:
        """
        Extract metadata from the photo. If there's an error, store the
        exception.

        Note that PyExifTool supports batch executions with many photos, but in
        my tests, that didn't really improve performance at all (and it has the
        drawback that it's harder to tell which photo broke in the event of an
        error). There are already improvements by using PyExifTool to keep the
        ExifTool process open this way rather than incurring the program startup
        cost for each photo. It doesn't seem to get better by processing, say,
        50 photos in a batch instead of 1 at a time.

        :param record: The photo to load.
        :return: None
        """

        try:
            record.exif_raw = self.exif_raw.get_metadata(record.path)[0]
            record.exif_fmt = self.exif_fmt.get_metadata(record.path)[0]
        except ExifToolException as e:
            record.exception = e

        # Set the event to notify the preprocessing worker that submitted this
        # photo record
        self.event_loop.call_soon_threadsafe(record.event.set)  # noqa
