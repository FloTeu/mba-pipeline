
import scrapy
from scrapy.pipelines.images import ImagesPipeline

import os
import datetime
import hashlib
import mimetypes
from io import BytesIO
from PIL import Image
from contextlib import suppress

import numpy as np
import piexif
from google.cloud import bigquery


# IMPORTS FROM FilesPipeline
import logging
from scrapy.utils.misc import md5sum
from scrapy.utils.python import to_bytes
from scrapy.utils.request import referer_str
from scrapy.pipelines.files import FileException, FilesPipeline

from mwfunctions.cloud.bigquery import stream_dict_list2bq
from mwfunctions.image.color import CSS4Counter
from mwfunctions.image.metadata import tmp_pil_add_metadata, print_metadata
from mwfunctions.pydantic.bigquery_classes import BQMBAProductsImages
from mwfunctions.pydantic.crawling_classes import MBAImageItems, CrawlingType
from mwfunctions.pydantic.firestore.crawling_log_classes import FSMBACrawlingProductLogs, FSMBACrawlingProductLogsSubcollectionDoc
from mwfunctions.time import get_berlin_timestamp
from mwfunctions.cloud.firestore import create_client as create_fs_client

class ImageException(FileException):
    """General image error exception"""

logger = logging.getLogger(__name__)

'''
### Image Pipeline
'''

class MWScrapyImagePipelineBase(ImagesPipeline):

    def get_media_requests(self, item: MBAImageItems, info):
        if type(item) == dict and "pydantic_class" in item:
            item = item["pydantic_class"]

        # function 1
        if isinstance(item, MBAImageItems):
            # self.change_bucket_if_debug(info)
            for i, image_item in enumerate(item.image_items):
                yield scrapy.Request(image_item.url, meta={"marketplace": item.marketplace, "asin": image_item.asin})

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
            counter = CSS4Counter(img, maxsize=480)
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
        orig_image = tmp_pil_add_metadata(orig_image, most_common_dict)

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

    @staticmethod
    def get_storage_file_path(marketplace, asin):
        return os.path.join(marketplace, asin + ".jpg")

    def file_path(self, request, response=None, info=None, *, item=None):
        # function 2
        img_path = ""
        try:
            # HERE ARE CUSTOM CHANGES
            img_path = self.get_storage_file_path(request.meta.get("marketplace"), request.meta.get("asin"))
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

    def media_downloaded(self, response, request, info, *, item=None):
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

    def item_completed(self, results, item: MBAImageItems, info):

        if type(item) == dict and "pydantic_class" in item:
            item = item["pydantic_class"]


        if isinstance(item, MBAImageItems):
            marketplace = item.marketplace
            bq_table_id = f"{info.spider.bq_project_id}.mba_{marketplace}.products_images"

            crawling_product_logs: FSMBACrawlingProductLogs = FSMBACrawlingProductLogs(marketplace=marketplace)
            crawling_product_logs_image_subcol_path = f"{crawling_product_logs.get_fs_doc_path()}/{CrawlingType.IMAGE}"
            fs_client = create_fs_client()

            rows_to_insert = []
            for i, image_item in enumerate(item.image_items):
                # if downloaded successfully
                if results[i][0]:
                    with suppress(Exception):
                        # inc. crawling job counter for images successfully crawled
                        info.spider.crawling_job.count_inc("new_images_count")
                    file_path = self.get_storage_file_path(marketplace, image_item.asin)
                    rows_to_insert.append(BQMBAProductsImages(asin=image_item.asin, url_gs=f"gs://{self.store.bucket.name}/{self.store.prefix}{file_path}",
                    url_mba_lowq=image_item.url_lowq, url_mba_hq=image_item.url, timestamp=get_berlin_timestamp(without_tzinfo=True)).dict(json_serializable=False))

                    # update logs so that image is not crawled twice or more
                    crawling_product_logs_subcol_doc = FSMBACrawlingProductLogsSubcollectionDoc(doc_id=image_item.asin)
                    crawling_product_logs_subcol_doc.set_fs_col_path(crawling_product_logs_image_subcol_path)
                    crawling_product_logs_subcol_doc.update_timestamp()
                    crawling_product_logs_subcol_doc.write_to_firestore(exclude_doc_id=False, exclude_fields=[], write_subcollections=True,
                                            client=fs_client)

            #errors = client.insert_rows_json(bq_table_id, rows_to_insert)  # Make an API request.
            stream_dict_list2bq(bq_table_id, rows_to_insert, check_if_table_exists=info.spider.debug)
        return item
