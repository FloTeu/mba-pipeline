
import scrapy
from scrapy.pipelines.images import ImagesPipeline

import os
import datetime
import hashlib
import mimetypes
from io import BytesIO
from PIL import Image

import numpy as np
import piexif
from google.cloud import bigquery


# IMPORTS FROM FilesPipeline
import logging
from scrapy.utils.misc import md5sum
from scrapy.utils.python import to_bytes
from scrapy.utils.request import referer_str
from scrapy.pipelines.files import FileException, FilesPipeline

from mwfunctions.image.color import CSS4Counter
from mwfunctions.image.metadata import pil_add_metadata, print_metadata

class ImageException(FileException):
    """General image error exception"""

logger = logging.getLogger(__name__)

'''
### Image Pipeline
'''

class MWScrapyImagePipelineBase(ImagesPipeline):

    def get_media_requests(self, item, info):
        # function 1
        assert len(item["asins"]) == len(item['image_urls']), "Length of asins and of image_urls need to be same"
        for i, image_url in enumerate(item['image_urls']):
            yield scrapy.Request(image_url, meta={"marketplace": item["marketplace"], "asin": item["asins"][i]})

    def file_downloaded(self, response, request, info, *, item=None):
        return self.image_downloaded(response, request, info)

    def image_downloaded(self, response, request, info, *, item=None):
        # function 4
        checksum = None
        for path, image, buf, most_common in self.get_images(response, request, info, item=item):
            if checksum is None:
                buf.seek(0)
                checksum = md5sum(buf)

            # init meta data with width and height
            width, height = image.size
            meta_dict = {'width': width, 'height': height}
            most_common_dict = self.most_common_to_property([most_common])[0]
            meta_dict.update(most_common_dict)

            # TODO add most common color info to meta information
            self.store.persist_file(
                path, buf, info,
                meta=meta_dict,
                headers={'Content-Type': 'image/jpeg'})
        # HERE ARE CUSTOM CHANGES
        return checksum, most_common

    def get_most_common_colors(self, response, n=10):
        img = np.array(Image.open(BytesIO(response.body)))
        try:
            counter = CSS4Counter(img)
            return counter.most_common(n)
        except Exception as e:
            print(str(e))
            return []

    def get_images(self, response, request, info, *, item=None):
        # function 3
        path = self.file_path(request, response=response, info=info, item=item)
        orig_image = Image.open(BytesIO(response.body))

        # HERE ARE CUSTOM CHANGES
        most_common = self.get_most_common_colors(response, n=10)
        most_common_dict = self.most_common_to_property([most_common])[0]
        orig_image = pil_add_metadata(orig_image, most_common_dict, path_to_data_dir="../scrapy_pipelines/data/")

        width, height = orig_image.size
        if width < self.min_width or height < self.min_height:
            raise ImageException("Image too small "
                                 f"({width}x{height} < "
                                 f"{self.min_width}x{self.min_height})")

        image, buf = self.convert_image(orig_image)
        yield path, image, buf, most_common

        for thumb_id, size in self.thumbs.items():
            thumb_path = self.thumb_path(request, thumb_id, response=response, info=info)
            thumb_image, thumb_buf = self.convert_image(image, size)
            yield thumb_path, thumb_image, thumb_buf

    def convert_image(self, image, size=None):
        if image.format == 'PNG' and image.mode == 'RGBA':
            background = Image.new('RGBA', image.size, (255, 255, 255))
            background.paste(image, image)
            image = background.convert('RGB')
        elif image.mode == 'P':
            image = image.convert("RGBA")
            background = Image.new('RGBA', image.size, (255, 255, 255))
            background.paste(image, image)
            image = background.convert('RGB')
        elif image.mode != 'RGB':
            image = image.convert('RGB')

        if size:
            image = image.copy()
            image.thumbnail(size, Image.ANTIALIAS)

        # load exif data
        exif_dict = piexif.load(image.info["exif"])
        exif_bytes = piexif.dump(exif_dict)
        # write image bytes in buffer + meta information
        buf = BytesIO()
        image.save(buf, 'JPEG', exif=exif_bytes)
        return image, buf

    def file_path(self, request, response=None, info=None, *, item=None):
        # function 2
        img_path = ""
        try:
            # HERE ARE CUSTOM CHANGES
            marketplace = request.meta.get("marketplace")
            asin = request.meta.get("asin")

            img_path = os.path.join(marketplace, asin + ".jpg")
        except:
            media_guid = hashlib.sha1(to_bytes(request.url)).hexdigest()
            media_ext = os.path.splitext(request.url)[1]
            # Handles empty and wild extensions by trying to guess the
            # mime type then extension or default to empty string otherwise
            if media_ext not in mimetypes.types_map:
                media_ext = ''
                media_type = mimetypes.guess_type(request.url)[0]
                if media_type:
                    media_ext = mimetypes.guess_extension(media_type)
            img_path = f'full/{media_guid}{media_ext}'
        return img_path

    def media_downloaded(self, response, request, info):
        referer = referer_str(request)

        if response.status != 200:
            logger.warning(
                'File (code: %(status)s): Error downloading file from '
                '%(request)s referred in <%(referer)s>',
                {'status': response.status,
                 'request': request, 'referer': referer},
                extra={'spider': info.spider}
            )
            raise FileException('download-error')

        if not response.body:
            logger.warning(
                'File (empty-content): Empty file from %(request)s referred '
                'in <%(referer)s>: no-content',
                {'request': request, 'referer': referer},
                extra={'spider': info.spider}
            )
            raise FileException('empty-content')

        status = 'cached' if 'cached' in response.flags else 'downloaded'
        logger.debug(
            'File (%(status)s): Downloaded file from %(request)s referred in '
            '<%(referer)s>',
            {'status': status, 'request': request, 'referer': referer},
            extra={'spider': info.spider}
        )
        self.inc_stats(info.spider, status)

        try:
            path = self.file_path(request, response=response, info=info)
            # HERE ARE CUSTOM CHANGES
            checksum, most_common = self.file_downloaded(response, request, info)
        except FileException as exc:
            logger.warning(
                'File (error): Error processing file from %(request)s '
                'referred in <%(referer)s>: %(errormsg)s',
                {'request': request, 'referer': referer, 'errormsg': str(exc)},
                extra={'spider': info.spider}, exc_info=True
            )
            raise
        except Exception as exc:
            logger.error(
                'File (unknown-error): Error processing file from %(request)s '
                'referred in <%(referer)s>',
                {'request': request, 'referer': referer},
                exc_info=True, extra={'spider': info.spider}
            )
            raise FileException(str(exc))

        # HERE ARE ALSO CUSTOM CHANGES
        return {'url': request.url, 'path': path, 'checksum': checksum, 'status': status, 'most_common': most_common}

    def most_common_to_property(self, most_commons):
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
            most_common_dict.update(
                {"color": color, "pixel_count": pixel_count, "hex": hex, "percentage": percentage, "rgb": rgb})
            most_common_dict_list.append(most_common_dict)

        return most_common_dict_list

    def item_completed(self, results, item, info):
        # updating BQ
        client = bigquery.Client()

        marketplace = item._values["marketplace"]
        bq_table_id = f"mba-pipeline.mba_{marketplace}.products_images"
        rows_to_insert = []
        for i, image_url in enumerate(item._values["image_urls"]):
            asin = item._values["asins"][i]
            url_mba_lowq = item._values["url_mba_lowqs"][i]
            url_gs = f"gs://5c0ae2727a254b608a4ee55a15a05fb7/mba-shirts/{marketplace}/{asin}.jpg"
            url = f"https://storage.cloud.google.com/5c0ae2727a254b608a4ee55a15a05fb7/mba-shirts/{marketplace}/{asin}.jpg"
            rows_to_insert.append(
                {u"asin": asin, u"url": url, u"url_gs": url_gs, u"url_mba_lowq": url_mba_lowq, u"url_mba_hq": image_url,
                 u"timestamp": str(datetime.datetime.now())})

        errors = client.insert_rows_json(bq_table_id, rows_to_insert)  # Make an API request.

        return item
