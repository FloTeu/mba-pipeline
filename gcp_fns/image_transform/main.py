import os
import cv2
import numpy as np
from google.cloud import storage
from tempfile import NamedTemporaryFile
import tempfile

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

    bucket = event['bucket']
    file_path = event['name']
    file_dir_path, file_name = "/".join(file_path.split("/")[0:-1]), file_path.split("/")[-1]
    dest_bucket_name = bucket + "_public"
    dest_file_path = f"{file_dir_path}/croped/{file_name}"

    client = storage.Client()
    source_bucket = client.get_bucket(bucket)
    source_blob = source_bucket.get_blob(file_path)
    image = np.asarray(bytearray(source_blob.download_as_string()), dtype="uint8")
    image = cv2.imdecode(image, cv2.IMREAD_UNCHANGED)
    scale_percent = 50  # percent of original size
    width = int(image.shape[1] * scale_percent / 100)
    height = int(image.shape[0] * scale_percent / 100)
    dim = (width, height)
    # crop image
    # TODO find out which setting are best suited for croping
    image_h, image_w = image.shape[0], image.shape[1]
    scale = 0.65
    width_less = 0.05
    scale_l, scale_h = (1-scale)/2,scale + (1-scale)/2
    crop_img = image[int(image_h*scale_l):int(image_h*scale_h), int(image_w*(scale_l+width_less)):int(image_w*(scale_h+-width_less))]
    
    tfile, tpath = tempfile.mkstemp("." + file_name.split(".")[-1])
    cv2.imwrite(tpath, crop_img)
    # Uploading the temp image file to the bucket
    dest_bucket = client.get_bucket(dest_bucket_name)
    dest_blob = dest_bucket.blob(dest_file_path)
    dest_blob.upload_from_filename(tpath)



    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    #print('Metageneration: {}'.format(event['metageneration']))
    #print('Created: {}'.format(event['timeCreated']))
    #print('Updated: {}'.format(event['updated']))