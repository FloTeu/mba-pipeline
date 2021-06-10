import os
import cv2
import numpy as np
from google.cloud import storage
from tempfile import NamedTemporaryFile
import tempfile
import functions
from google.cloud import bigquery
import piexif
import piexif.helper
import json
from PIL import Image
from matplotlib import cm
from functions import grayscale2rgb, load_image_bytes, rgba2rbg
from datetime import datetime
import io

def get_most_common_colors(img_bytes_io, n=10):
    # bytes to numpy.
    # Hint: numpy colors are changed from the original image like red becoming blue. Dont know exact background
    if type(img_bytes_io) == io.BytesIO:
        img = np.array(Image.open(img_bytes_io))
    else:
        img = load_image_bytes(img_bytes_io)
    try:
        counter = functions.CSS4Counter(img)
        return counter.most_common(n)
    except Exception as e:
        print(str(e))
        return []

def most_common_to_property(most_commons):
    '''Transforms most_common output of CSS4Counter to firestore property
    '''
    most_common_dict_list = []
    for most_common in most_commons:
        most_common_dict = {}
        color = []
        pixel_count = []
        hex = []
        percentage = []
        rgb = []
        for top_n in list(most_common.keys()):
            most_common_top_n = most_common[top_n]
            color.append(most_common_top_n.name)
            pixel_count.append(most_common_top_n.pixel_count)
            hex.append(most_common_top_n.hex)
            percentage.append(float('%.4f' % most_common_top_n.percentage)) 
            rgb.append(",".join([str(int(255 * rgb)) for rgb in list(most_common_top_n.rgb)]))
        most_common_dict.update({"color": color, "pixel_count": pixel_count, "hex": hex, "percentage": percentage, "rgb": rgb})
        most_common_dict_list.append(most_common_dict)

    return most_common_dict_list 

def crop_image(image, scale):
    """ MBA image designs have alwas 4500 * 5400 demension for shirt designs
        Therfore, we want to keep dimension scale for cropping

    """
    image_h, image_w = image.shape[0], image.shape[1]
    scale_l, scale_h = (1-scale)/2,scale + (1-scale)/2
    w_to_h = 4500/5400
    h_1 = int(image_h*scale_l)
    h_2 = int(image_h*scale_h)
    image_w_center = int(image_w / 2)
    w_px = (h_2-h_1) * w_to_h
    w_1 = image_w_center - int(w_px / 2)
    w_2 = image_w_center + int(w_px / 2)

    return image[h_1:h_2, w_1:w_2]

def load_data(bucket, file_path):
    source_bucket = storage.Client().get_bucket(bucket)
    source_blob = source_bucket.get_blob(file_path)
    return np.array(
        cv2.imdecode(
            np.asarray(bytearray(source_blob.download_as_string()), dtype=np.uint8), 0
        ))


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Blob {} downloaded to {}.".format(
            source_blob_name, destination_file_name
        )
    )


def get_cropped_image_bytes_io(image, scale):
    crop_img = crop_image(image, scale)
    #crop_img_pil = Image.fromarray(np.uint8(cm.gist_earth(crop_img)*255))
    #crop_img_pil = Image.fromarray(cm.gist_earth(bytearray(source_blob.download_as_string()), bytes=True))
    crop_img_pil = Image.fromarray(np.uint8(crop_img)).convert('RGB')
    # crop_img_pil = Image.fromarray(crop_img)
    # crop_img_pil.save("test.jpg")
    # crop_img_pil_np = np.array(crop_img_pil)
    # cv2.imwrite("test.jpg", crop_img_pil_np)
    crop_img_bytes = io.BytesIO()
    crop_img_pil.save(crop_img_bytes, format='PNG')
    return crop_img_bytes


def crop_image2public(event, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This generic function logs relevant data when a file is changed.

    Args:
        event (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    # init storage references
    bucket = event['bucket']
    file_path = event['name']
    file_dir_path, file_name = "/".join(file_path.split("/")[0:-1]), file_path.split("/")[-1]
    dest_bucket_name = bucket + "_public"
    dest_file_path = f"{file_dir_path}/cropped/{file_name}"

    # init data fpr bigquery
    shop = file_dir_path.split("/")[0].split("-")[0]
    marketplace = file_dir_path.split("/")[-1]
    file_id = file_name.split(".")[0]
    bq_table_id = f"mba-pipeline.mba_{marketplace}.products_images_cropped"

    # only crop image if its a shirt image
    # TODO: open this function for de marketplace
    if "shirt" in file_path and marketplace in ["com", "de"]:
        # read image from storage
        client = storage.Client()
        source_bucket = client.get_bucket(bucket)
        source_blob = source_bucket.get_blob(file_path)
        image = np.asarray(bytearray(source_blob.download_as_string()), dtype="uint8")
        image_right_color = cv2.imdecode(image, cv2.IMREAD_UNCHANGED)
        # this loads image with different colors
        image = load_image_bytes(source_blob.download_as_string())

        # crop image
        # TODO find out which setting are best suited for croping
        # how much percent should croped image have of original image size
        scale = 0.65
        crop_img_bytes = get_cropped_image_bytes_io(image, scale)
        crop_img = crop_image(image_right_color, scale)

        # calculate most common colors
        most_common = get_most_common_colors(crop_img_bytes)
        most_common_dict = most_common_to_property([most_common])[0]
        tfile, tpath = tempfile.mkstemp("." + file_name.split(".")[-1])

        # store image file in temp path
        cv2.imwrite(tpath, crop_img)

        # add meta data
        # load existing exif data from image
        exif_dict = piexif.load(tpath)
        # insert custom data in usercomment field
        exif_dict["Exif"][piexif.ExifIFD.UserComment] = piexif.helper.UserComment.dump(
            json.dumps(most_common_dict),
            encoding="unicode"
        )
        # insert mutated data (serialised into JSON) into image
        piexif.insert(
            piexif.dump(exif_dict),
            tpath
        )

        # Uploading the temp image file to the bucket
        dest_bucket = client.get_bucket(dest_bucket_name)
        dest_blob = dest_bucket.blob(dest_file_path)
        dest_blob.metadata = most_common_dict
        dest_blob.upload_from_filename(tpath)

        # updating BQ
        client = bigquery.Client()

        url_gs = f"gs://{dest_bucket_name}/{dest_file_path}"
        url = f"https://storage.cloud.google.com/{dest_bucket_name}/{dest_file_path}"
        rows_to_insert = [
            {u"file_id": file_id, u"url": url, u"url_gs": url_gs, u"shop": shop, u"timestamp": str(datetime.now())},
        ]
        errors = client.insert_rows_json(bq_table_id, rows_to_insert)  # Make an API request.
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))