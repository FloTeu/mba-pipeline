import pprint
#from pyexiv2.metadata import ImageMetadata
import piexif
import piexif.helper
import uuid
from pathlib import Path
import json
import os
from io import BytesIO
from PIL import Image
import tempfile
from mwfunctions.image.conversion import pil2np


# Example of reading meta data from image in filepath
def print_metadata(filename):
    import pyexiv2
    metadata = pyexiv2.ImageMetadata(filename)
    metadata.read()
    userdata = json.loads(metadata['Exif.Photo.UserComment'].value)
    pprint.pprint(userdata)

def add_metadata(tpath, meta_dict):
    # add meta data
    # load existing exif data from image
    exif_dict = piexif.load(tpath)
    # insert custom data in usercomment field
    exif_dict["Exif"][piexif.ExifIFD.UserComment] = piexif.helper.UserComment.dump(
        json.dumps(meta_dict),
        encoding="unicode"
    )
    # insert mutated data (serialised into JSON) into image
    piexif.insert(
        piexif.dump(exif_dict),
        tpath
    )

def tmp_pil_add_metadata(image_pil, meta_dict, img_format=None):
    """ Works without store image on local sorage (memory temp dir is used)
    """
    img_format = img_format if img_format else "jpg"
    tfile, tpath = tempfile.mkstemp(f".{img_format}")
    # store image file in temp path
    image_pil.save(tpath)#, image_pil.format)
    add_metadata(tpath, meta_dict)
    return Image.open(tpath)


def pil_add_metadata(image, meta_dict, path_to_data_dir="", filename=None, delete_file=True):
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
    add_metadata(filename, meta_dict)
    image = Image.open(filename)

    # meta = ImageMetadata(filename)
    # meta.read()
    # meta['Exif.Photo.UserComment'] = json.dumps(meta_dict)
    # meta.write()
    # # transform back to PIL Image
    # byteio = BytesIO(meta.buffer)
    # image = Image.open(byteio)

    # delete file
    if delete_file:
        os.remove(filename)

    # return image with meta information
    return image
