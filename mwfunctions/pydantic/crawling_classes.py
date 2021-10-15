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

    @validator("start_timestamp", always=False)
    def validate_type(cls, start_timestamp, values): # Only if type is not None
        if "start_timestamp" not in values:
            return datetime.now()
        else:
            return start_timestamp

class MBACrawlingJob(CrawlingJob):
    marketplace: Marketplace = Field(description="MBA marketplace")
    crawling_type: CrawlingType = Field(description="Crawling type, which indicates which pages and what data is the target of crawling")

class MBAOverviewCrawlingJob(MBACrawlingJob):
    products_count_new: int = Field(0, description="Count of new products, which where not already in db")
    products_count_already_crawled: int = Field(0, description="Count of already crawled products")
    crawling_type: CrawlingType = Field("overview", description="Crawling type, which indicates which pages and what data is the target of crawling")

class MBAProductCrawlingJob(MBACrawlingJob):
    crawling_type: CrawlingType = Field("product", description="Crawling type, which indicates which pages and what data is the target of crawling")
