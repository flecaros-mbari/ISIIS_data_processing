"""
Shared ROI-cropping helper used by create_folder.py and get_data_train.py.

Both scripts crop a region of interest out of a source image and save it
into a class-named folder, but historically computed the pixel bounding
box from two different bbox conventions (corner fractions vs.
width/height fractions) and cropped via two different mechanisms (a
torch/torchvision tensor slice vs. a plain PIL crop). The bbox
conversion stays with each caller since the conventions genuinely
differ; only the "clamp to image bounds and crop" step is shared here.
"""


def crop_roi(image, left, top, right, bottom):
    """
    Crop a region from an already-open PIL Image, clamped to image bounds.

    Parameters
    ----------
    image : PIL.Image.Image
        An open image.
    left, top, right, bottom : float
        Pixel coordinates of the crop box (before clamping).

    Returns
    -------
    PIL.Image.Image or None
        The cropped image, or None if the box is degenerate/invalid
        after clamping to the image bounds.
    """
    img_width, img_height = image.size

    left = max(0, int(left))
    top = max(0, int(top))
    right = min(img_width, int(right))
    bottom = min(img_height, int(bottom))

    if right <= left or bottom <= top:
        return None

    return image.crop((left, top, right, bottom))
