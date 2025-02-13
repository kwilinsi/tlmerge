from __future__ import annotations

from enum import Enum
from typing import Annotated, Literal, TypeAlias

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

WhiteBalanceType: TypeAlias = tuple[float, float, float, float] | \
                              Literal['auto', 'camera', 'default']


class WhiteBalanceModel(BaseModel):
    model_config = ConfigDict(extra='forbid')

    # Note that green_2 shares the green_1 aliases, meaning that if you omit
    # green_2, it'll just use the same values as green_1
    red: Annotated[float, Field(ge=0, validation_alias=AliasChoices(
        'r', 'red'))]
    green_1: Annotated[float, Field(ge=0, validation_alias=AliasChoices(
        'g1', 'g', 'green_1', 'green1', 'green'))]
    blue: Annotated[float, Field(ge=0, validation_alias=AliasChoices(
        'b', 'blue'))]
    green_2: Annotated[float, Field(ge=0, validation_alias=AliasChoices(
        'g2', 'green_2', 'green2', 'green', 'g',
        'green_1', 'green1', 'g1'))]

    @classmethod
    def to_tuple(
            cls,
            data: WhiteBalanceType | WhiteBalanceModel) -> WhiteBalanceType:
        """
        If the given white balance data is a WhiteBalanceModel, convert it to a
        tuple of four floats in RGBG order. Otherwise, return it unmodified.

        :return: The white balance data.
        """

        if isinstance(data, WhiteBalanceModel):
            return data.red, data.green_1, data.blue, data.green_2

        return data


class ChromaticAberrationModel(BaseModel):
    model_config = ConfigDict(extra='forbid')
    red: Annotated[float, Field(ge=0, validation_alias=AliasChoices(
        'r', 'red'))]
    blue: Annotated[float, Field(ge=0, validation_alias=AliasChoices(
        'b', 'blue'))]

    @classmethod
    def to_tuple(cls,
                 data: tuple[float, float] | ChromaticAberrationModel) -> \
            tuple[float, float]:
        """
        If the given chromatic aberration data is a ChromaticAberrationModel,
        convert it to a tuple of two floats, red and blue, in that order.
        Otherwise, return it unmodified.

        :return: The chromatic aberration data.
        """

        if isinstance(data, ChromaticAberrationModel):
            return data.red, data.blue

        return data


class ThumbLocation(Enum):
    """
    These are the possible settings for where thumbnails can be stored.
    For each option except CUSTOM, thumbnails are stored in some folder (by
    default "thumb") at the specified location.

    Consider the raw picture "2000-01-01/a/0001.dng". A thumbnail could be
    stored in any of these locations:

    - ROOT: `<PROJECT_ROOT>/thumb/2000-01-01/a/0001.jpg`
    - DATE: `<PROJECT_ROOT>/2000-01-01/thumb/a/0001.jpg`
    - GROUP: `<PROJECT_ROOT>/2000-01-01/a/thumb/0001.jpg`
    - CUSTOM: `<SOME_CUSTOM_PATH>/2000-01-01/a/0001.jpg`
    """

    ROOT = 0
    DATE = 1
    GROUP = 2
    CUSTOM = 3


class FlipRotate(Enum):
    """
    These photo flips and rotations correspond to RawPy's `"user_flip"`
    parameter, which can flip and rotate images when postprocessing them.
    """

    # If the photo has rotation/flip data, use it. Otherwise, this usually
    # means no rotation or flipping
    DEFAULT = None

    # Specifically don't do any rotation or flipping. Ignore any rotation data
    # baked into the raw photo by the camera
    NONE = 0

    # Simple horizontal mirror
    HORIZONTAL_FLIP = 1

    # Simple vertical mirror
    VERTICAL_FLIP = 2

    # 180 degree rotation
    HALF_ROTATION = 3

    # This could also be ROTATE_CCW_VERTICAL_FLIP. My selection is arbitrary
    ROTATE_CW_HORIZONTAL_FLIP = 4

    # 270 degree rotation
    ROTATE_CCW = 5

    # 90 degree rotation
    ROTATE_CW = 6

    # This could also be ROTATE_CW_VERTICAL_FLIP. My selection is arbitrary
    ROTATE_CCW_HORIZONTAL_FLIP = 7
