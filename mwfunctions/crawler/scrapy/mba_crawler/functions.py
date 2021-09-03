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


from collections import Counter, namedtuple
import matplotlib.colors as mcolors
import scipy.spatial
import matplotlib._color_data as mcd
import numpy as np
from typing import Optional, List
#from mvfunctions.profiling import log_time
from easydict import EasyDict as edict

# Without pil (is faster)
class CSS4Counter(Counter):
    # Class dependent, gets called once (when accessing the module?!)
    CSS_COLORS = mcd.CSS4_COLORS
    RGB2NAME_DICT = {mcolors.to_rgb(color): name for name, color in CSS_COLORS.items()}
    RGB2NUM_DICT = {rgb: i for i, rgb in enumerate(RGB2NAME_DICT.keys())}
    PALETTE = np.array(list(RGB2NAME_DICT.keys()))
    # COMMON = namedtuple("Common", ["pixel_count", "percentage", "rgb", "hex", "name"])


    def __init__(self, img, maxsize=None):
        """This is not fast
        maxsize should be an int declaring the largest side of an image
        """

        # TODO: Assert we have rgb image
        if len(img.shape) != 3:
            raise ValueError(f"Can only count rgb images, yours have {img.shape} shape")

        # Why old shape?
        self.old_shape = img.shape
        if maxsize:
            self.maxsize = (maxsize, maxsize) if maxsize else maxsize
            if self.maxsize and img.shape[0] > self.maxsize[0] and img.shape[1] > self.maxsize[1]:
                img = optimal_resize(img, (maxsize, maxsize), keep_aspect_ratio=True, pad=None)
            self.resize_shape = img.shape
        # Flatten the first two dimensions, keep the channels
        img = img.reshape((-1, 3))/255
        closest_idx = scipy.spatial.distance.cdist(img, self.PALETTE).argmin(1)
        super(CSS4Counter, self).__init__(closest_idx)
        self.mapped = self.PALETTE[closest_idx]
        # mapped_tuples = [tuple(m) for m in self.mapped]
        # super(CSS4Counter, self).__init__(mapped_tuples)
        self.n_values = sum(self.values())


    # def most_common(self, n: Optional[int] = None) -> List:
    #     # with log_time("CSS4Counter CountingMC", is_on=True):
    #     _most_commons = super(CSS4Counter, self).most_common(n)
    #     most_commons = []
    #     for mc in _most_commons:
    #         closest_idx, count = mc[0], mc[1]
    #         rgb = self.PALETTE[closest_idx]
    #         perc = count / self.n_values
    #         hex = str(mcolors.to_hex(rgb))
    #         name = self.RGB2NAME_DICT[rgb]
    #         most_commons.append(self.COMMON(count, perc, rgb, hex, name))
    #     return most_commons

    def most_common(self, n: Optional[int] = None) -> dict:
        _most_commons = super(CSS4Counter, self).most_common(n)
        most_commons = edict()
        for place, mc in enumerate(_most_commons):
            closest_idx, count = mc[0], mc[1]
            rgb = tuple(self.PALETTE[closest_idx])
            perc = count / self.n_values
            hex = str(mcolors.to_hex(rgb))
            name = self.RGB2NAME_DICT[rgb]
            # Make a dict with {"0": colorstuff, "1": ...}
            most_commons[str(place)] = edict({"pixel_count": count, "percentage": perc, "rgb": rgb, "hex": hex, "name": name})
            # most_commons.append(self.COMMON(count, perc, rgb, hex, name))
        return most_commons

    def get_css4img(self):
        return optimal_resize(self.mapped.reshape(self.resize_shape), self.old_shape[:2], keep_aspect_ratio=True)

    # def sample_count(self):
    #     np.random.normal(image_mid, scale, size)


import pprint
import pyexiv2
from pyexiv2.metadata import ImageMetadata
import piexif
import uuid
from pathlib import Path
import json 
import os 
from io import BytesIO
from PIL import Image

# Example of reading meta data from image in filepath
def print_metadata(filename):
    metadata = pyexiv2.ImageMetadata(filename)
    metadata.read()
    userdata=json.loads(metadata['Exif.Photo.UserComment'].value)
    pprint.pprint(userdata)


def add_metadata(image, meta_dict, path_to_data_dir="", filename=None, delete_file=True):
    """Add meta data to an given image. 
    Process: uuid is generated for a unique filename. Image will be stored locally, because ImageMetadata object need to receive filepath.

    TODO: Might work without save image on local storage.  

    :param image: Pillow Image which where meta information should be stored in
    :type image: PIL.Image
    :param meta_dict: Dict with meta information. E.g. meta_dict={"color":["red","green"], "hex":["#ff0000", "#00ff00"]}
    :type meta_dict: [type]
    :param path_to_data_dir: Optional path of image_file. Local file will be removed within function anyway, therefore this paramter is not impportant.
    :type path_to_data_dir: str
    :return: [description]
    :rtype: [type]
    """
    
    Path(path_to_data_dir).mkdir(parents=True, exist_ok=True)

    # download file to write new meta data to file
    if not filename:
        media_guid = uuid.uuid4().hex
        filename = path_to_data_dir + media_guid + '.jpg'
    image.save(filename)

    # add metadata 
    meta = ImageMetadata(filename)       
    meta.read()
    meta['Exif.Photo.UserComment']=json.dumps(meta_dict)
    meta.write()

    # transform back to PIL Image
    byteio = BytesIO(meta.buffer)
    image = Image.open(byteio)
    
    # delete file
    if delete_file:
        os.remove(filename)

    # return image with meta information
    return image
