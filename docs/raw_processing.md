# Processing a Raw Image

tlmerge is designed for photographers who are familiar with how raw images work.

Often, people think of "shooting in raw" as an alternative to `.jpg` images that
preserves more detail, especially in the highlights and shadows. As opposed to
`.jpg` images, shooting in raw means more possibilities and higher image quality
when you edit your photos in Lightroom.

This is true, but there's more to it. If you use software like Lightroom to
process your raw photos, and you're not familiar with the details of raw images
processing, you'll want to read over this document before using tlmerge.

You might think that converting a `.cr3` file (a raw format used by Canon) into
a `.jpg` is as simple as converting a `.jpg` to a `.png`. However, this is not
the case. There are few interpretive decisions you have to make when converting
a raw image.

Additionally, note that RAW image here doesn't mean a `.raw` file. Rather, it
collectively refers to many image formats from each camera manufacturer that
store raw data from the camera sensor. To quote the
`dcraw` [documentation](https://www.dechifro.org/dcraw/):

> "raw" is an English word, not an acronym or file format. "raw photo" \[is\]
> the same adjective that you would use for "crude oil" or "raw materials".

# The Camera Sensor

The first step in understanding raw images is understanding how the camera
sensor works. As you can probably guess, each "pixel" in an image corresponds
to one very tiny photodetector on the camera sensor.

However, this photodetector on the sensor doesn't see in color. It measures
the amount of light it receives but not whether that light is red, green, or
blue. But each pixel in a `.jpg` image has RGB color: three separate numbers for
the amount of red, green, and blue.

So how does a camera sensor see in color? Most consumer cameras have a very
tiny color filter in front of each pixel, called a Bayer filter. The red filters
block everything except red light, the green filters only allow green light to
get through, and the blue filters only allow blue light. Each photodetector
still measures all incoming light waves, but they're restricted to only see
certain colors of light. The filters are arranged in a grid like this:

![RGBG bayer filter pattern](images/bayer_pattern.png)

The exact color arrangement varies between colors, but it's very common to have
twice as many green pixels as blue or red. This mimics the human eye, which
is more sensitive green light. As a result, only 1/4 of the pixels in your
camera are measuring red light, and
the same for blue.

If we convert the raw file directly to a normal raster format like `.tiff`, it
looks very odd. The following example shows the same image converted with a
standard raw processing method and converted directly to a `.tiff` without
processing.<sup>1</sup>

![A picture of a beach with side-by-side processed and directly converted
images. A zoomed-in circle shows individual pixels in a bayer pattern on a
high-contrast person in the distance.](images/raw_bayer_comparison.png)

<sup>1</sup>Technically, there's some processing going on here to scale the
pixel brightness and directly assign a color to each pixel. See the code below
for details.

<details>
  <summary>(Expand for processing code)</summary>

**(Left) Plain TIFF processing**

```
dcraw -T -w -v beach.nef
```

**(Right) Mosaiced showing Bayer filter**

Here we read the raw values from each pixel and assign them to an RGB value.
For example, a red pixel reading 183 becomes the RGB value `(56, 0, 0)` or
`#B70000`. Also, while RGB values typically range from 0-255 per channel, the
raw values can be much larger (up to 65535 for a 16-bit image). We normalize
them to a 0-255 range so that the brightest pixel in this image is 255.

```python
import imageio.v3 as iio
import numpy as np
import rawpy

with rawpy.imread('beach.nef') as img:  # open raw Nikon image
    rows, columns = img.raw_image.shape
    mosaiced = np.zeros((rows, columns, 3), dtype=np.uint8)
    max_val = np.max(img.raw_image)  # max pixel value for scaling

    for r in range(rows):
        for c in range(columns):
            val = int(img.raw_image[r, c] / max_val * 255)  # scale to 0-255
            color = img.raw_colors[r, c]  # get pixel's color filter
            mosaiced[r, c, 1 if color == 3 else color] = val  # set color val

    iio.imwrite('beach_mosaic.tiff', mosaiced)
```

</details>

There are three things in particular to notice about the raw image:

### It's green

The extra green photodetectors make the whole image appear very green.
Whatever processing method we use must account for this by increasing the
other colors relative to green.

### The pixels are weird

Looking at the individual pixels in the closeup shows us that each pixel is
strictly some shade of red, green or blue. There aren't any other colors, such
as beige in the sand. This is because each photodetector measures just one of
the three colors (RGB) used in computers.

In the zoomed out version, there pixels start to blend together, so there's
actually some other colors. However, it's very hard to see, since the image is
so green.

### It's dark

As a whole, the image is rather dark. There are a couple of reasons for this.
For one, no gamma correction was applied to the raw image to brighten it on a
log scale (more on this later). Additionally, computer monitors display color
with three LEDs for each pixel: one red, one green, and one blue. In order to
get the brightest output (solid white), all three LEDs must be maxed. However,
this image only uses one color for each pixel. That means that for every pixel
on the screen, two LEDs aren't on at all, and the image looks dark.

---

In order to process a raw image so that it looks like the version on the left,
we'll need to fix all these problems. The rest of this document outlines that
process. Additionally, there are some other things we can do in the processing
stage to make the image look even better. We'll cover some of those as well.

There are also some other things to know about how the sensor detects light,
which we'll cover later.

# Demosaicing

As [explained previously](#the-camera-sensor), each pixel (or photodetector)
in the camera sensor only detects one color: red, green, or blue. Once we
develop the image, we want each pixel to have full color data.

To do this, we can make inferences based on nearby pixels. Say you're looking
at a particular red pixel in an image. The photodetector there gave measured 30
for the amount of red light, but it didn't measure green or blue. However,
there are four green pixels adjacent to it, which measure 38, 65, 91, and 61.
If we average these values, we get 63.75. Thus, we can estimate that if the
red pixel were able to measure green light, it would have recorded ~64.

![The bayer pattern from an image demonstrating how to interpolate from nearby
values](images/interpolating_bayer.png)

Alternatively, we could try interpolating the mosaiced pixels by combining
them in groups of four. If we average together groups of four pixels, we'll
get a full RGB value.

This works well, but it has a pretty big downside of decreasing the overall
resolution of the image. Since every 2x2 grid gets merged into one pixel, the
image resolution is cut in half. It looks good, but we lose some detail. Here's
an image with half-size interpolated colors on the left and the original
sensor data on the right:

![A picture of a seagull where the left side is interpolated at half-resolution
and the right side is original sensor data. A zoomed-in circle shows individual
pixels on a high-contrast area around the bird's
foot.](images/demosaic_interpolation.png)

<details>
  <summary>(Expand for processing command)</summary>

The following `dcraw` command was used to generate the interpolated image on
the left:

```
dcraw -T -r 1 1 1 1 -g 1 1 -W -v seagull.nef
```

It uses default pixel values with no white balance adjustment (`-r 1 1 1 1`),
linear values without a gamma curve (`-g 1 1`), and no auto brightness
adjustment (`-W`). It saves as a `.tiff` file (`-T`) and prints some processing
information (`-v`).

The image on the right is processed with the same script used earlier.

</details>

This process of combining the color values from nearby pixels to remove the
bayer pattern is called [demosaicing](https://en.wikipedia.org/wiki/Demosaicing)
or debayering.

The first example above, averaging nearby pixels to estimate color values, is
a pretty naive estimate. It works okay on solid gradients, such as the sky, but
it's not very good for edges with sharp luminance and/or color changes.

There's no perfect solution to demosaic an image. At the end of the day, there's
color information that the sensor didn't give us, and it's impossible to
perfectly derive that information. Thus, demosaicing is an **underdetermined
problem**.

However, there are many demosaicing algorithms available. Some prioritize
computational speed, while others try to produce clean images in particularly
tricky situations (like high contrast edges and fine details). Based on the
photos you're developing, it may be worth trying out a few algorithms to see
which one works best for you.

The `dcraw` tool provides four algorithms in addition to the half-size averaging
approach shown above:

- Bilinear interpolation. This is high-speed but low-quality. Select it with the
  `-q 0` CLI flag.
- Variable Number of Gradients (VNG) interpolation. Select with `-q 1`.
- Patterned Pixel Grouping (PPG) interpolation. Select with `-q 2`.
- Adaptive Homogeneity-Directed (AHD) interpolation. Select with `-q 3`.

It also includes the `-f` flag to interpolate RGBG as four colors. This treats
the two green values in each 2x2 grid as separate colors, which is specific
to certain camera models.

tlmerge uses [rawpy](https://pypi.org/project/rawpy/) to develop images, which
is an interface for [LibRaw](https://www.libraw.org) (itself a fork of `dcraw`).

LibRaw provides support for additional demosaicing algorithms.

- AAHD
- AFD
- AHD
- AMAZE
- DCB
- DHT
- LINEAR
- LMMSE
- MODIFIED_AHD
- PPG
- VCD
- VCD_MODIFIED_AHD
- VNG

Note that some of these algorithms are included in GPL2– and GPL3-licensed
demosaic packs. These are not compatible with the MIT license used by rawpy
and tlmerge, and thus they are not included by default.

### Brightness

You may have noticed that the interpolated image of the seagull above is
considerably brighter than the raw image. Combining the luminance of all three
colors within each pixel makes the overall image considerably brighter.
