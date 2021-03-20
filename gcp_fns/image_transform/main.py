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

def get_most_common_colors(img, n=10):
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

def crop_image(image, scale, width_less):
    image_h, image_w = image.shape[0], image.shape[1]
    scale_l, scale_h = (1-scale)/2,scale + (1-scale)/2
    return image[int(image_h*scale_l):int(image_h*scale_h), int(image_w*(scale_l+width_less)):int(image_w*(scale_h+-width_less))]


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
    if "shirt" in file_path and marketplace == "com":
        # read image from storage
        client = storage.Client()
        source_bucket = client.get_bucket(bucket)
        source_blob = source_bucket.get_blob(file_path)
        image = np.asarray(bytearray(source_blob.download_as_string()), dtype="uint8")
        image = cv2.imdecode(image, cv2.IMREAD_UNCHANGED)

        # crop image
        # TODO find out which setting are best suited for croping
        # how much percent should croped image have of original image size
        scale = 0.65
        # home much less percent width shoul be croped in comparison to height
        width_less = 0.05
        crop_img = crop_image(image, scale, width_less)
        crop_img_pil = Image.fromarray(np.uint8(cm.gist_earth(crop_img)*255))
        #crop_img_pil = Image.fromarray(cm.gist_earth(bytearray(source_blob.download_as_string()), bytes=True))
        #crop_img_pil = Image.fromarray(np.uint8(image)).convert('RGB')
        # crop_img_pil = Image.fromarray(crop_img)
        # crop_img_pil.save("test.jpg")

        # calculate most common colors
        # TODO: image counter does not work correctly (red is not detected as example)
        tfile, tpath = tempfile.mkstemp("." + file_name.split(".")[-1])
        most_common = get_most_common_colors(crop_img)
        most_common_dict = most_common_to_property([most_common])[0]

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

        # meta = ImageMetadata(tpath)       
        # meta.read()
        # meta['Exif.Photo.UserComment']=json.dumps(most_common_dict)
        # meta.write()

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
            {u"file_id": file_id, u"url": url, u"url_gs": url_gs, u"shop": shop},
        ]
        errors = client.insert_rows_json(bq_table_id, rows_to_insert)  # Make an API request.
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))