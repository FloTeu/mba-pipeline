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
from mwfunctions.pydantic.bigquery_classes import BQTable
from mwfunctions.cloud.bigquery import stream_dict_list2bq
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

    @abc.abstractmethod
    def process_item(self, item, spider):
        # function which is called after item (object scrapy.item.Item) is yielded
        pass

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

        self.gcloud_project = get_gcp_project()
        assert "merchwatch" in self.gcloud_project, f"'{self.gcloud_project}' is not a merchwatch project"

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
        # save crawling job in firestore
        print("Save crawling job to Firestore")
        self.crawling_job.end_timestamp = datetime.now()
        firestore_fns.write_document_dict(self.crawling_job.dict(),f"{self.fs_log_col_path}/{self.crawling_job.id}")

    def process_item(self, item, spider):
        if isinstance(item, BQTable):
            stream_dict_list2bq(f"{self.gcloud_project}.mba_{spider.marketplace}.{item._bq_table_name}", [item.dict()])
        return item
