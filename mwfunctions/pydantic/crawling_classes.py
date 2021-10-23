
import uuid
import pytz

from pydantic import BaseModel, Field, validator
from datetime import date, datetime
from enum import Enum, IntEnum
from typing import Optional, Dict, List, Any

from mwfunctions.pydantic.base_classes import MWBaseModel
from mwfunctions.time import get_berlin_timestamp
from mwfunctions.crawler.preprocessing.excluded_asins import EXCLUDED_ASINS, STRANGE_LAYOUT

class Marketplace(Enum):
    DE="de"
    COM="com"

class CrawlingType(Enum):
    OVERVIEW = "overview"
    PRODUCT = "product"
    REALTIME_RESEARCH = "realtime_research"

class CrawlingInputItem(BaseModel):
    asin: str
    marketplace: str
    url: Optional[str] = Field(description="Urls which should be crawled")

    @validator("url", always=True)
    def validate_url(cls, url, values):
        return url if url in values else f"https://www.amazon.{values['marketplace']}/dp/{values['asin']}"

class PODProduct(Enum):
    SHIRT = "shirt"

class CrawlingSorting(Enum):
    BEST_SELLER="best_seller"
    NEWEST="newest"
    OLDEST="oldest"
    PRICE_ASC="price_up"
    PRICE_DESC="price_down"
    CUSTOMER_REVIEW_DESC='cust_review_desc'

class CrawlingJob(MWBaseModel):
    id: Optional[str] = Field(uuid.uuid4().hex, description="Unique Id of crawling job")
    start_timestamp: Optional[datetime] = Field(get_berlin_timestamp(without_tzinfo=True), description="Datetime of crawling start")
    end_timestamp: Optional[datetime] = Field(None, description="Datetime of crawling end")
    duration_in_min: Optional[float] = Field(0.0, description="Duration of crawling task in minutes")
    finished_with_error: Optional[bool] = Field(False, description="Whether crawler finished with errors")
    error_msg: Optional[str] = Field(None, description="Optional. Python error message")
    number_of_target_pages: Optional[int] = Field(None, description="E.g. number of overview pages or number of product pages")
    request_count: Optional[int] = Field(0, description="Number of total requests sended")
    response_successful_count: Optional[int] = Field(0, description="Count of successfull responses with status code 200 and without captcha blocking")
    response_captcha_count: Optional[int] = Field(0, description="Count of captcha blocked responses")
    response_404_count: Optional[int] = Field(0, description="Count of status code 404 responses")
    response_5XX_count: Optional[int] = Field(0, description="Count of status code 5XX responses")
    response_3XX_count: Optional[int] = Field(0, description="Count of status code 3XX responses")
    warning_count: Optional[int] = Field(0, description="Count of warnings. e.g. if price could not be crawled due to geographic proxy problems (eu proxy for usa product)")
    proxy_ban_count: Optional[int] = Field(0, description="Count of proxy ban. Which is only temporary.")

    class Config:
        validate_assignment = True

    def count_inc(self, field, increment=1):
        assert "count" in field, f"field must contain 'count' but is {field}"
        assert self.__contains__(field), f"{field} does not exist in model"
        self[field] += increment

    # @validator("end_timestamp")
    # def validate_end_timestamp(cls, end_timestamp, values):
    #     values["duration_in_min"] = float("%.2f" % ((end_timestamp - values["start_timestamp"]).seconds / 60)) if "start_timestamp" in values and end_timestamp else 0
    #     return end_timestamp

    def set_duration_in_min(self):
        self.duration_in_min = float("%.2f" % ((self.end_timestamp - self.start_timestamp).seconds / 60)) if self.start_timestamp and self.end_timestamp else 0

class MBACrawlingJob(CrawlingJob):
    marketplace: Marketplace = Field(description="MBA marketplace")
    crawling_type: CrawlingType = Field(description="Crawling type, which indicates which pages and what data is the target of crawling")

class MBAOverviewCrawlingJob(MBACrawlingJob):
    new_products_count: int = Field(0, description="Count of new products, which where not already in db")
    already_crawled_products_count: int = Field(0, description="Count of already crawled products")
    crawling_type: CrawlingType = Field("overview", description="Crawling type, which indicates which pages and what data is the target of crawling")
    new_images_count: Optional[int] = Field(0, description="Count of new images vrawled by overview crawler")

class MBAProductCrawlingJob(MBACrawlingJob):
    daily: bool = Field(description="daily=True -> Products should be crawled that already were crawled before, daily=False -> First time crawling")
    crawling_type: CrawlingType = Field("product", description="Crawling type, which indicates which pages and what data is the target of crawling")
    price_not_found_count: int = Field(0, description="Count of successfull responses without price information")

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

class CrawlingMBARequest(MWBaseModel):
    marketplace: Marketplace
    debug: bool = Field(False, description="Whether spider should be runned in debug mode or not. In debug mode pictures will be saved in debug storage dir and debug FS collections.")

class CrawlingMBAOverviewRequest(CrawlingMBARequest):
    sort: CrawlingSorting = Field(description="Sorting of MBA overview page")
    pod_product: PODProduct = Field(PODProduct.SHIRT, description="Type of product, e.g. shirt in future more should be possible")
    keyword: str = Field("", description="optional search term keyword. Simulation of customer search in amazon")
    pages: int = Field(0, description="Total number of overview pages that should be crawled. If 0 => maximum (400) pages will be crawled")
    start_page: int = Field(1, description="Start page in overview page. 1 is the first starting page")

class CrawlingMBADailyProportions(MWBaseModel):
    """ Proportions decide which prodicts should be crawled
        Sum of integers must be 1
    """
    best_seller: float = Field(0.7, description="70 % random pick of best sellers(Last crawled date in table products_mba_relevance)")
    lowest_bsr_count: float = Field(0.2, description="20% products which were crawled the least")
    # random must be last field due to validator
    random: float = Field(0.1, description="10% random pick of products")

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        assert sum(self.values()) > 0.99 and sum(self.values()) < 1.01, "Sum of proportions must be 1"

class Test(MWBaseModel):
    test_b: int

class CrawlingMBAProductRequest(CrawlingMBARequest):
    daily: bool = Field(description="daily=True -> Products should be crawled that already were crawled before, daily=False -> First time crawling")
    number_products: int = Field(description="Number of products that should be crawled")
    top_n: Optional[int] = Field(60, description="Number of top n bestellers/trending products etc. which should be prioritized for crawling")
    proportions: CrawlingMBADailyProportions = Field(description="Proportions of crawling products with sum equals 1")
    #test: Test
    excluded_asins: List[str] = Field(EXCLUDED_ASINS+STRANGE_LAYOUT, description="List of asins which should be excluded by crawling")
    asins_to_crawl: Optional[List[str]] = Field([], description="List of asins which should be crawled. If empty -> Asins will be downloaded by BQ automatically")
