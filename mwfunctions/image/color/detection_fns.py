from collections import Counter, namedtuple
import matplotlib.colors as mcolors
import scipy.spatial
import matplotlib._color_data as mcd
import numpy as np
from typing import Optional, List
#from mvfunctions.profiling import log_time
from easydict import EasyDict as edict

from mwfunctions.image.transform import optimal_resize

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
