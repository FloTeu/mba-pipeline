
import uuid

from pydantic import BaseModel, Field, validator
from datetime import date, datetime
from enum import Enum, IntEnum
from typing import Optional, Dict, List

from mwfunctions.pydantic.base_classes import MWBaseModel

class Marketplace(Enum):
    de = "de"
    com = "com"

class CrawlingType(Enum):
    overview = "overview"
    product = "product"
    realtime_research = "realtime_research"

class CrawlingJob(MWBaseModel):
    id: Optional[str] = Field(uuid.uuid4().hex, description="Unique Id of crawling job")
    start_timestamp: Optional[datetime] = Field(description="Datetime of crawling start")
    end_timestamp: Optional[datetime] = Field(description="Datetime of crawling end")
    finished_with_error: Optional[bool] = Field(False, description="Whether crawler finished with errors")
    error_msg: Optional[str] = Field(None, description="Optional. Python error message")
    request_count: Optional[int] = Field(0, description="Number of total requests sended")
    response_successful_count: Optional[int] = Field(0, description="Count of successfull responses with status code 200 and without captcha blocking")
    response_captcha_count: Optional[int] = Field(0, description="Count of captcha blocked responses")
    response_404_count: Optional[int] = Field(0, description="Count of status code 404 responses")
    response_5XX_count: Optional[int] = Field(0, description="Count of status code 5XX responses")
    response_3XX_count: Optional[int] = Field(0, description="Count of status code 3XX responses")
    warning_count: Optional[int] = Field(0, description="Count of warnings. e.g. if price could not be crawled due to geographic proxy problems (eu proxy for usa product)")

    def count_inc(self, field, increment=1):
        assert "count" in field, f"field must contain 'count' but is {field}"
        assert self.__contains__(field), f"{field} does not exist in model"
        self[field] += increment

    @validator("start_timestamp", always=True)
    def validate_start_timestamp(cls, start_timestamp, values): # Only if type is not None
        if "start_timestamp" not in values:
            return datetime.now()
        else:
            return start_timestamp


class MBACrawlingJob(CrawlingJob):
    marketplace: Marketplace = Field(description="MBA marketplace")
    crawling_type: CrawlingType = Field(description="Crawling type, which indicates which pages and what data is the target of crawling")

class MBAOverviewCrawlingJob(MBACrawlingJob):
    new_products_count: int = Field(0, description="Count of new products, which where not already in db")
    already_crawled_products_count: int = Field(0, description="Count of already crawled products")
    crawling_type: CrawlingType = Field("overview", description="Crawling type, which indicates which pages and what data is the target of crawling")
    new_images_count: Optional[int] = Field(0, description="Count of new images vrawled by overview crawler")

class MBAProductCrawlingJob(MBACrawlingJob):
    crawling_type: CrawlingType = Field("product", description="Crawling type, which indicates which pages and what data is the target of crawling")

class MBAImageItem(BaseModel):
    asin: str
    url: str = Field(description="url which should be downloaded with image pipeline. Should be high quality image")
    url_lowq: str = Field(description="url to low quality image")

class MBAImageItems(MWBaseModel):
    """Alles was wir für die Bilder brauchen


       #Caution: Every list element except list_properties_not_for_fs_document() must have same length to make sure FS documents get right field value
       #Caution: List properties should end with '_list' or 's' to make sure FS field name
    """
    marketplace: Marketplace
    image_items: List[MBAImageItem]
    fs_product_data_col_path: str = Field(description="path to product data e.g. de_shirts")
    gs_path_element_list: list = Field([], description="Optional List of storage path elements e.g. categories which are used to create gs_url. For example ['men','clothes','t-shirt']")

