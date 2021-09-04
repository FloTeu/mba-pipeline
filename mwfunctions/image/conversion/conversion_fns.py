
import io

import numpy as np
from PIL import Image


def load_image_bytes(img_data):
    image = Image.open(io.BytesIO(img_data))
    image = np.array(image)
    return image


def rgba2rbg(image):
    png = Image.fromarray(image)
    png.load()  # required for png.split()

    background = Image.new("RGB", png.size, (255, 255, 255))
    background.paste(png, mask=png.split()[3])  # 3 is the alpha channel
    return np.array(background)


def grayscale2rgb(image):
    img = Image.fromarray(image)
    img.convert("RGB")
    return np.array(img)
