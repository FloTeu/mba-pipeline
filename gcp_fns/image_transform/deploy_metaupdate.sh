gcloud functions deploy crop_image2publicMetaUpdate \
--runtime python38 \
--trigger-resource 5c0ae2727a254b608a4ee55a15a05fb7 \
--trigger-event google.storage.object.metadataUpdate \
--region europe-west3 --memory 1024MB --project=mba-pipeline