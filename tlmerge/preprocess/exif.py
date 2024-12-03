from collections.abc import Callable, Iterable
from datetime import datetime
from pathlib import Path

from exiftool import ExifToolHelper

from tlmerge.conf import CONFIG
from .metadata import PhotoMetadata


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
    :raise ValueError: If the input string is None, empty, or otherwise
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


class ExifData:
    def __init__(self,
                 exif_raw: dict[str, any],
                 exif_fmt: dict[str, any]) -> None:
        """
        Initialize an EXIF data record with metadata from a photo.

        :param exif_raw: The raw EXIF data from ExifTool.
        :param exif_fmt: The EXIF data automatically formatted by ExifTool.
        """

        self.exif_raw: dict[str, any] = exif_raw
        self.exif_fmt: dict[str, any] = exif_fmt

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
        :raise KeyError: If the tag doesn't exist and is not optional.
        :raise ValueError: If the casting function fails.
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
                            f"{'formatted' if fmt else 'raw'} EXIF data"
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
                    # Try to get the file path/name to make the error message
                    # more helpful
                    try:
                        name = CONFIG.root.rel_path(exif['SourceFile'])
                    except Exception:
                        name = exif.get('File:FileName', '[Unknown Photo]')

                    error = ValueError(
                        f"Tag {tag} had unexpected value \"{val}\" "
                        f"for {name}; couldn't cast to {cast}"
                    )

        # No matching tags
        if error is not None:
            raise error
        return None

    def record_metadata(self, photo: PhotoMetadata) -> None:
        """
        Record the relevant extracted EXIF data in the given PhotoMetadata
        object.

        :param photo: The photo metadata object.
        :return: None
        :raise KeyError: If the EXIF data for a mandatory (non-null) column in
         the database is missing.
        :raise ValueError: If the EXIF data for a mandatory (non-null) column
         in the database is in an unexpected format/type.
        """

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
        photo.camera_make = self.get('EXIF:Make', opt=False)
        photo.camera_model = self.get('EXIF:Model', opt=False)

        # Lens info
        photo.lens_make = self.get('EXIF:LensMake')
        photo.lens_model = self.get('EXIF:LensModel', 'Composite:LensID')
        photo.lens_spec = self.get('Composite:LensSpec')

        # Lens focal length range
        photo.lens_min_focal_length = self.get('MakerNotes:MinFocalLength',
                                               fmt=False, cast=float)
        photo.lens_max_focal_length = self.get('MakerNotes:MaxFocalLength',
                                               fmt=False, cast=float)

        # Lens aperture range
        photo.lens_lens_f_stops = self.get(
            'MakerNotes:LensFStops', cast=float
        )
        photo.lens_max_aperture_min_focal = self.get(
            'MakerNotes:MaxApertureAtMinFocal', cast=float
        )
        photo.lens_max_aperture_max_focal = self.get(
            'MakerNotes:MaxApertureAtMaxFocal', cast=float
        )
        photo.lens_effective_max_aperture = self.get(
            'MakerNotes:EffectiveMaxAperture', cast=float
        )


class ExifWorker:
    def __init__(self) -> None:
        """
        Initialize an ExifLoader. This opens a connection to ExifTool through
        PyExifTool until explicitly closed (which is mandatory).

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
        """

        self.exif_raw: ExifToolHelper = ExifToolHelper()  # Default: -G and -n
        self.exif_fmt: ExifToolHelper = ExifToolHelper(common_args=['-G'])

    def close(self) -> None:
        """
        Close the connections to ExifTool. This does nothing if no connection
        is currently open.

        :return: None
        """

        self.exif_raw.terminate()
        self.exif_fmt.terminate()

    def extract(self, file: Path | str) -> ExifData:
        """
        Extract EXIF metadata from the given photo file.

        Note that PyExifTool supports batch executions with many photos, but in
        my tests, that didn't really improve performance at all (and it has the
        drawback that it's harder to tell which photo broke in the event of an
        error). There are already improvements by using PyExifTool to keep the
        ExifTool process open this way rather than incurring the program startup
        cost for each photo. It doesn't seem to get better by processing, say,
        50 photos in a batch instead of 1 at a time.

        :param file: The path to the photo file.
        :return: None
        :raise ExifToolException: If there is an error from PyExifTool while
         loading the EXIF data.
        """

        # Start ExifTool in case not yet running
        self.exif_raw.run()
        self.exif_fmt.run()

        return ExifData(
            exif_raw=self.exif_raw.get_metadata(str(file))[0],
            exif_fmt=self.exif_fmt.get_metadata(str(file))[0]
        )
