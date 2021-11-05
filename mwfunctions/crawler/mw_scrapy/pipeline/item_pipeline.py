import abc
import uuid
import os
from datetime import datetime, date
from google.cloud import bigquery
print(os.getcwd())


# IMPORTS FROM FilesPipeline
import logging
from itemadapter import ItemAdapter

from mwfunctions.environment import is_debug, get_gcp_project
from mwfunctions.pydantic.crawling_classes import MBAOverviewCrawlingJob, MBAProductCrawlingJob, CrawlingType, MBACrawlingJob, MBAImageCrawlingJob
from mwfunctions.pydantic.bigquery_classes import BQTable
from mwfunctions.cloud.bigquery import stream_dict_list2bq


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
        """ Function is executed after __init__ of spider
        spider must have properties:
            * name (unique name of spider e.g. fashion_vinted)
            * marketplace (mba marketplace e.g. "de" or "com")
            * website_crawling_target (overview", product or overview_and_product)
        """
        # TODO: Decide which functionality should be outsourced to dhis function or to MBASpider parent class
        # is debug can either be defined by spider property or from environment
        if hasattr(spider, 'debug'):
            self.debug = spider.debug
        else:
            self.debug = is_debug()

        #spider.update_settings(spider.settings)

        # debug mode is only available for project 'merchwatch-dev'
        if self.debug:
            os.environ["GOOGLE_CLOUD_PROJECT"] = "merchwatch-dev"

        # in case mba-pipeline normal prod project should be merchwatch (for FS for example) storage and BQ will be set to mba-pipeline if project is merchwatch
        if get_gcp_project() == "mba-pipeline":
            os.environ["GOOGLE_CLOUD_PROJECT"] = "merchwatch"

        self.gcloud_project = get_gcp_project()
        assert "merchwatch" in self.gcloud_project, f"'{self.gcloud_project}' is not a merchwatch project"

        website_crawling_target = spider.website_crawling_target # can be either "overview", "product" or "overview_and_product"
        if website_crawling_target not in CrawlingType.get_field_values():
            raise NotImplementedError(f"Item pipeline is only implemented for website_crawling_target {CrawlingType.get_field_values()}")

        self.fs_product_data_col_path = f'{spider.marketplace}_shirts{"_debug" if self.debug else ""}'
        self.fs_log_col_path = f'crawling_jobs{"_debug" if self.debug else ""}'
        if website_crawling_target == CrawlingType.OVERVIEW.value:
            request_input = {}
            for request_input_field in spider.mba_crawling_request.request_input_to_log_list:
                request_input[request_input_field] = spider.mba_crawling_request[request_input_field]
            self.crawling_job = MBAOverviewCrawlingJob(marketplace=spider.marketplace, id=spider.crawling_job_id, request_input=request_input)
        elif website_crawling_target == CrawlingType.PRODUCT.value:
            self.crawling_job = MBAProductCrawlingJob(marketplace=spider.marketplace, daily=spider.daily, id=spider.crawling_job_id)
        elif website_crawling_target == CrawlingType.IMAGE.value:
            self.crawling_job = MBAImageCrawlingJob(marketplace=spider.marketplace, id=spider.crawling_job_id, parent_id=spider.mba_image_request.parent_crawling_job_id)
            if spider.mba_image_request.parent_crawling_job_id:
                self.fs_log_col_path = f'crawling_jobs{"_debug" if self.debug else ""}/{spider.mba_image_request.parent_crawling_job_id}/image_pipeline_logs'
        else:
            raise NotImplementedError

        self.bq_project_id = 'mba-pipeline' if self.gcloud_project == "merchwatch" else "merchwatch-dev"
        self.bq_client = bigquery.Client(project=self.bq_project_id)

        # spider properties update
        spider.crawling_job = self.crawling_job
        spider.debug = self.debug
        spider.bq_client = self.bq_client
        spider.bq_project_id = self.bq_project_id
        spider.fs_product_data_col_path = self.fs_product_data_col_path
        spider.fs_log_col_path = self.fs_log_col_path

        if self.debug:
            print(f"START CRAWLING {spider.name} IN DEBUG MODE")

    def close_spider(self, spider):
        pass

    def process_item(self, item, spider):
        if type(item) == dict and "pydantic_class" in item:
            item = item["pydantic_class"]
        if isinstance(item, BQTable):
            stream_dict_list2bq(f"{self.bq_project_id}.mba_{spider.marketplace}.{item._bq_table_name}", [item.dict()], client=self.bq_client, check_if_table_exists=self.debug)
        return item
