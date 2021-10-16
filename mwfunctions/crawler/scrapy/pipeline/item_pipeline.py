import abc
import uuid
import os
from datetime import datetime, date

print(os.getcwd())


# IMPORTS FROM FilesPipeline
import logging
from itemadapter import ItemAdapter

from mwfunctions.environment import is_debug, get_gcp_project
from mwfunctions.pydantic.crawling_classes import MBAOverviewCrawlingJob, MBAProductCrawlingJob
import mwfunctions.cloud.firestore as firestore_fns

logger = logging.getLogger(__name__)

'''
### Item Pipeline
'''

class MWScrapyItemPipelineAbstract(abc.ABC):
    # abstract class for scrapy item pipeline. Items are not allowed to contain property "image_urls". In case of "image_urls" they will be process by scrapy.pipelines.images.ImagesPipeline

    @abc.abstractmethod
    def open_spider(self, spider):
        # function which is called at the beginning of spider process
        pass

    @abc.abstractmethod
    def close_spider(self, spider):
        # function which is called at the end of spider process
        pass

    # @abc.abstractmethod
    # def process_item(self, item, spider):
    #     # function which is called after item (object scrapy.item.Item) is yielded
    #     pass

class MWScrapyItemPipeline(MWScrapyItemPipelineAbstract):
    def __init__(self):
        pass

    def open_spider(self, spider):
        """ spider must have properties:
            * name (unique name of spider e.g. fashion_vinted)
            * marketplace (mba marketplace e.g. "de" or "com")
            * website_crawling_target (overview", product or overview_and_product)
        """
        # is debug can either be defined by spider property or from environment
        if hasattr(spider, 'debug'):
            self.debug = spider.debug
        else:
            self.debug = is_debug()

        #spider.update_settings(spider.settings)

        if self.debug:
            os.environ["GOOGLE_CLOUD_PROJECT"] = "merchwatch-dev"

        GCLOUD_PROJECT = get_gcp_project()
        assert "merchwatch" in GCLOUD_PROJECT, f"'{GCLOUD_PROJECT}' is not a merchwatch project"

        website_crawling_target = spider.website_crawling_target # can be either "overview", "product" or "overview_and_product"
        if website_crawling_target not in ["overview", "product", "overview_and_product"]:
            raise NotImplementedError("Item pipeline is only implemented for website_crawling_target overview, product and 'overview_and_product'")

        self.fs_product_data_col_path = f'{spider.marketplace}_shirts{"_debug" if self.debug else ""}'
        self.fs_log_col_path = f'crawling_jobs{"_debug" if self.debug else ""}'
        self.crawling_job = MBAOverviewCrawlingJob(marketplace=spider.marketplace)

        # spider properties update
        spider.crawling_job = self.crawling_job
        spider.debug = self.debug
        spider.fs_product_data_col_path = self.fs_product_data_col_path
        spider.fs_log_col_path = self.fs_log_col_path

        if self.debug:
            print(f"START CRAWLING {spider.name} IN DEBUG MODE")

    def close_spider(self, spider):
        # insert crawling job

        # save crawling job in firestore
        self.crawling_job.end_timestamp = datetime.now()
        firestore_fns.write_document_dict(self.crawling_job.dict(),f"{self.fs_log_col_path}/{self.crawling_job.id}")


    # def has_price_changed(self, product_id, item):
    #     return self.db.get_last_price(product_id) != item.price
    #
    # def update_dbs_if_price_has_changed(self, product_id, item):
    #     adapter = ItemAdapter(item)
    #     if self.has_price_changed(product_id, item):
    #         self.db.crawling_job.new_priced_products += 1
    #         # if price has changed, a new content entry is needed
    #         new_content_id = uuid.uuid4().hex
    #         self.db.insert_content(new_content_id, adapter.get('price'), adapter.get('is_discount'), adapter.get('discount_percentage'), adapter.get('title'), adapter.get('is_sold_out'))
    #         self.db.insert_product_to_content(product_id, new_content_id)
    #
    #         # store in firestore
    #         crawler_firestore_fns.set_price_doc_in_subcollection(self.fs_product_data_col_path,product_id, item.get_price_doc_dict())
    #
    # def update_crawling_job_by_already_crawled_overview_item(self, product_id):
    #     # check if the url has already been crawled in this run
    #     if self.db.has_product_been_crawled_in_current_run(product_id):
    #         self.db.crawling_job.duplicate_products += 1
    #
    #     # update crawling_ID to identify as active
    #     self.db.update_product_crawling_id(product_id, self.db.crawling_job.crawling_ID)
    #
    # def reactivate_product(self, product_id):
    #     # if product is crawled, but status in db was "active=False/0" it should be reactivated
    #     # -> update sql db + FS product document
    #     if not self.db.is_product_active(product_id):
    #         self.db.update_active_state_of_product(product_id, 1)
    #         crawler_firestore_fns.update_product_document(self.fs_product_data_col_path,product_id,{'active': True})
    #         self.db.crawling_job.reactivated_products += 1
    #
    # def process_overview_page_item(self, item):
    #     """
    #         Checks if product_url was already crawled with sql db
    #         update crawling job meta information
    #         update SQL db and FS
    #     """
    #     adapter = ItemAdapter(item)
    #     product_url = adapter.get('product_url')
    #     existing_master_id = adapter.get('master_id') # can also be None
    #
    #     # Checks if product_url was already crawled with sql db
    #     if self.db.has_overview_product_already_been_crawled(product_url):
    #         product_id = self.db.get_product_id_by_url(product_url)
    #         master_id = self.db.get_master_id_by_url(product_url)
    #
    #         self.update_crawling_job_by_already_crawled_overview_item(product_id)
    #
    #         # update master_id if the id does not match the id form the given parent
    #         # could be the case if a previous parent product does no longer exist
    #         # and a new product is now the parent
    #         if existing_master_id and existing_master_id != master_id:
    #             self.db.update_product_master_id(product_id, master_id)
    #
    #         # reactivate product
    #         self.reactivate_product(product_id)
    #
    #         self.update_dbs_if_price_has_changed(product_id, item)
    #
    #
    #         adapter.__setitem__("was_already_crawled", True)
    #     # case: product has not been crawled already
    #     else:
    #         self.db.crawling_job.new_products += 1
    #         # create new product_id and master_id and content_id (content id is only for sql db)
    #         product_id = uuid.uuid4().hex
    #         master_id = existing_master_id if existing_master_id else uuid.uuid4().hex
    #         content_id = uuid.uuid4().hex
    #
    #         # store in sqlite db
    #         self.db.insert_product_url(product_id, product_url, self.db.crawling_job.crawling_ID, master_id)
    #         self.db.insert_content(content_id,adapter.get('price'), adapter.get('is_discount'), adapter.get('discount_percentage'), adapter.get('title'), adapter.get('is_sold_out'))
    #         self.db.insert_product_to_content(product_id, content_id)
    #
    #         # update firestore
    #         crawler_firestore_fns.set_price_doc_in_subcollection(self.fs_product_data_col_path, product_id, item.get_price_doc_dict())
    #         crawler_firestore_fns.update_product_document(self.fs_product_data_col_path, product_id, {'active': True})
    #
    #         adapter.__setitem__("was_already_crawled", False)
    #
    #     # insert image urls if they are not already stored
    #     for image_url in adapter.get('image_url_list'):
    #         if not self.db.has_product_image_been_crawled(image_url):
    #             self.db.insert_image_url(product_id, image_url)
    #
    #     # call by reference update of item object
    #     adapter.__setitem__("product_id", product_id)
    #     adapter.__setitem__("master_id", master_id)
    #
    # def process_product_page_item(self, item):
    #     adapter = ItemAdapter(item)
    #     crawler_firestore_fns.update_product_document(self.fs_product_data_col_path, adapter.get("product_id"), {**item.get_product_doc_dict(), "created_timestamp": datetime.now()})
    #     self.db.insert_product_url_crawled(adapter.get("product_id"))
    #
    # def process_item(self, item, spider):
    #     # item must at least contain product_url
    #     adapter = ItemAdapter(item)
    #     if isinstance(item, ProductItemOverviewPage):
    #         self.process_overview_page_item(item)
    #     # update Firestore only if product was not already crawled
    #     if isinstance(item, ProductItemProductPage) and not adapter.get("was_already_crawled", False):
    #         self.process_product_page_item(item)
    #
    #     return item