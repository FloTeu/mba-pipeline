
import io
import base64

import numpy as np
from PIL import Image


def load_image_bytes(img_data):
    image = Image.open(io.BytesIO(img_data))
    image = np.array(image)
    return image

def pil2bytes(img):
    import io
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='PNG')
    img_byte_arr = img_byte_arr.getvalue()
    return img_byte_arr

def pil2b64_str(img):
    img_bytes = pil2bytes(img)
    base64_bytes = base64.b64encode(img_bytes)
    base64_string = base64_bytes.decode('utf-8')
    return base64_string

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

def b64_str2bytes(b64_str):
    # str to base64 bytes
    b64_bytes = b64_str.encode('utf-8')
    # base64bytes to normal bytes
    img_bytes = base64.b64decode(b64_bytes)
    return img_bytes

def bytes2np(img_data):
    image = Image.open(io.BytesIO(img_data))
    image = np.array(image)
    return image

def bytes2pil(img_bytes):
    image = Image.open(io.BytesIO(img_bytes))
    return image

def bytes2b64_str(img_bytes):
    # bytes to base64bytes
    base64_bytes = base64.b64encode(img_bytes)
    # base64bytes to str
    base64_string = base64_bytes.decode('utf-8')
    return base64_string

def np2b64_str(img_np):
    """ Care, this assumes u8 data"""
    img_pil = Image.fromarray(np.uint8(img_np))
    img_b64_str = pil2b64_str(img_pil)
    return img_b64_str

def np2pil(img_np):
    return Image.fromarray(img_np)

def pil2np(img_pil):
    return np.array(img_pil)

def b64_str2np(b64_str):
    img_bytes = b64_str2bytes(b64_str)
    img = bytes2np(img_bytes)
    return img