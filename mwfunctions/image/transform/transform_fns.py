import cv2
import numpy as np
from typing import Tuple, Optional

def optimal_resize(
        image: np.array,
        shape: Tuple[int, int],
        keep_aspect_ratio: bool = True,
        pad: Optional[str] = 'constant',
        upscale_interp: str = 'inter_linear',
        downscale_interp: str = 'inter_area',
        return_ratio: bool = False):
    """
    This function takes a picture and decides if it needs
    upscaling or downscaling and then uses a corresponding method.

    Visually the best:
        downscale: inter_area
        upscale: INTER_LANCZOS4

    Corresponding to this blog
    https://medium.com/infosimples/how-to-deal-with-image-resizing-in-deep-learning-e5177fad7d89
    it is better to downscale to the smallest image
    and **bilinear** gives better performance (keep in mind that may be just accidential)

    inter_liner == bilinear

    :param image:
    :param shape:
    :param keep_aspect_ratio_and_pad:  We will pad missing pixels with zeros
    :param replicate_border:
    :param upscale_interp: 'inter_linear', 'inter_cubic' else INTER_LANCZOS4
    :param downscale_interp: 'inter_linear' else INTER_AREA
    :return:
    """
    assert len(image.shape) == 3, 'Only up to rgb is allowed for now'
    assert len(shape) == 2, "Give a shape tuple"

    def area(shape):
        return shape[0] * shape[1]

    # CARE! cv2.resize dsize not working as intended, you must
    # swap the dsize
    # shape given in (y, x)
    if keep_aspect_ratio:
        i_max = np.argmax(image.shape[:2])
        ratio = float(shape[i_max]) / float(image.shape[i_max])
        new_size = tuple([int(x * ratio) for x in image.shape[:2]][::-1])
    else:
        new_size = tuple(shape[::-1])
    # new_size given in (x, y)

    # Downsize if the image is bigger than the wanted shape
    if area(image.shape) > area(shape):
        interp = cv2.INTER_LINEAR if downscale_interp == 'inter_linear' else cv2.INTER_AREA
        image = cv2.resize(image, new_size, interpolation=interp)
    # Upsize
    else:
        if upscale_interp == 'inter_linear':
            image = cv2.resize(image, new_size, interpolation=cv2.INTER_LINEAR)
        elif upscale_interp == 'inter_cubic':
            # https://theailearner.com/2018/11/15/image-interpolation-using-opencv-python/
            # says intercubic would be the best...
            # TODO: Test this
            image = cv2.resize(image, None, fx=10, fy=10,
                               interpolation=cv2.INTER_CUBIC)
        else:
            # I found this to be visually looking the best, but bilinear
            # performed better then this
            image = cv2.resize(
                image, new_size, interpolation=cv2.INTER_LANCZOS4)

    if keep_aspect_ratio and pad:
        # shape in (y, x), new_size in (x, y)
        delta_w = shape[1] - new_size[0]
        delta_h = shape[0] - new_size[1]
        top, bottom = delta_h // 2, delta_h - (delta_h // 2)
        left, right = delta_w // 2, delta_w - (delta_w // 2)
        if pad == 'replicate_border':
            image = cv2.copyMakeBorder(
                image, top, bottom, left, right, cv2.BORDER_REPLICATE)
        elif pad == 'constant':
            image = cv2.copyMakeBorder(
                image,
                top,
                bottom,
                left,
                right,
                cv2.BORDER_CONSTANT,
                value=[0, 0, 0])

    return image if not return_ratio else (image, ratio)
