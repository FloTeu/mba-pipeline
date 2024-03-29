
import pytz
from pydantic import BaseModel, Field, validator, PrivateAttr
# TODO_ lteral is only available since python3.8, but instance has python 3.7
from typing import Union, Dict, List, Optional #, Literal
from datetime import datetime, date

from mwfunctions.pydantic.base_classes import MWBaseModel, Marketplace
from mwfunctions.pydantic.firestore.utils import get_bsr_category
from mwfunctions.time import get_berlin_timestamp

class BQTable(MWBaseModel):
    ''' Child of BQTable must contain all columns of an BQ table so that new rows can directly be uploaded.
        Hint: Bigquery shows datetime obj with timezone "Etc/GMT0" by default. You can change timezone to Berlin with query: DATETIME(timestamp, 'Europe/Berlin')
    '''
    _bq_table_name: str = PrivateAttr() #Field(description="Table name in BQ. Can be used to upload data")

"""
    Raw Data of merchwatch_shirts BQ table
"""

class BQPlotDataRaw(MWBaseModel):
    plot_x:str = Field(description="Comma seperated string of dates related to bsr")
    plot_y:str = Field(description="Comma seperated string of bsr")
    plot_x_price:str = Field(default=None, description="Comma seperated string of dates related to price")
    plot_y_price:str = Field(default=None, description="Comma seperated string of prices")


class BQKeywordDataRaw(MWBaseModel):
    product_features:str = Field(description="Stringified list. E.g. ['Solid colors: 100% Cotton; Heather Grey: 90% Cotton, 10% Polyester; All Other Heathers: 50% Cotton, 50% Polyester', 'Imported', 'Buckle closure']")
    brand:str = Field(description="Brand of mba product")
    title:str = Field(description="Title of product")
    description: str = Field(default="", description="Description of product. Is not part of merchwatch_shirts table")
    #language: Literal['de', 'en'] = Field(default=None, description="Language of product text")
    language: str = Field(default=None, description="Language of product text")

    def get_keyword_text(self, marketplace):
        # all keyword related text combined with whitespaces.
        return " ".join([self.title + "."] + [self.brand + "."] + get_product_listings_by_list_str(self.product_features, marketplace) + [self.description])

    def get_filtered_keyword_list(self, marketplace, tr4k_lang_dict: Dict[str, object], language: str=None): # object = TextRank4Keyword of mwfunctions.text
        assert language or self.language, "Either 'langugae' or attribute 'language' must be provided"
        language = language if language else self.language
        text = self.get_keyword_text(marketplace)

        if language == "en":
            keywords = tr4k_lang_dict["en"].get_unsorted_keywords(text, candidate_pos=['NOUN', 'PROPN'], lower=False)
        elif language == "de":
            keywords = tr4k_lang_dict["de"].get_unsorted_keywords(text, candidate_pos=['NOUN', 'PROPN'], lower=False)
        elif marketplace == "com":
            keywords = tr4k_lang_dict["en"].get_unsorted_keywords(text, candidate_pos=['NOUN', 'PROPN'], lower=False)
        elif marketplace == "de":
            keywords = tr4k_lang_dict["de"].get_unsorted_keywords(text, candidate_pos=['NOUN', 'PROPN'], lower=False)
        else:
            keywords = tr4k_lang_dict["de"].get_unsorted_keywords(text, candidate_pos=['NOUN', 'PROPN'], lower=False)

        # filter keywords
        return filter_keywords(marketplace, keywords)

class BQMBAProductsImages(BQTable):
    # mba-pipeline:mba_de.products_images
    _bq_table_name: str = PrivateAttr("products_images")
    asin: str
    url_gs: str
    url: Optional[str]
    url_mba_lowq: str
    url_mba_hq: str
    timestamp: Optional[datetime] = Field(get_berlin_timestamp(without_tzinfo=True))

    @validator("url", always=True)
    def validate_url(cls, url, values):
        if "url" not in values:
            return values["url_gs"].replace("gs://", "https://storage.cloud.google.com/")
        else:
            return url

class BQMBAProductsMbaImages(BQTable):
    # mba-pipeline:mba_de.products_mba_images
    _bq_table_name: str = PrivateAttr("products_mba_images")
    asin: str
    url_image_lowq: str
    url_image_q2: str
    url_image_q3: str
    url_image_q4: str
    url_image_hq: str
    timestamp: Optional[datetime] = Field(get_berlin_timestamp(without_tzinfo=True))

    @classmethod
    def parse_with_one_url(cls, image_url, asin):
        # method can be used if only one image url exists
        return cls.parse_obj({"asin": asin, "url_image_lowq": image_url,
                       "url_image_q2": image_url, "url_image_q3": image_url,
                       "url_image_q4": image_url, "url_image_hq": image_url, "timestamp": datetime.now()
                       })


    def get_fs_import_dict(self):
        # transforms data to FS standard naming for e.g. FSMBAShirt class
        return {**self.dict(include={"url_image_q2", "url_image_q3", "url_image_q4"}), "url_mba_lowq": self.url_image_lowq, "url_mba_hq": self.url_image_hq}


class BQMBAOverviewProduct(BQTable):
    # mba-pipeline:mba_de.products
    _bq_table_name: str = PrivateAttr("products")
    asin: str
    title: str
    brand: Optional[str] = Field(None, description="brand of mba product")
    url_product: str
    url_image_lowq: str
    url_image_hq: str
    price: str
    uuid: Optional[str] = Field(None)
    timestamp: Optional[datetime] = Field(get_berlin_timestamp(without_tzinfo=True))

class BQMBAProductsMbaRelevance(BQTable):
    # mba-pipeline:products_mba_relevance
    _bq_table_name: str = PrivateAttr("products_mba_relevance")
    asin: str
    sort: str = Field(description="MBA sorting like. newest, bestseller etc.")
    number: int = Field(description="Number of apperance in overview crawling job")
    timestamp: Optional[datetime] = Field(get_berlin_timestamp(without_tzinfo=True))


class BQMBAProductsDetails(BQTable):
    _bq_table_name: str = PrivateAttr("products_details")
    asin: str
    title: str
    brand: str = Field(description="brand of mba product")
    url_brand: str
    price: str
    fit_types: str
    color_names: str
    color_count: int
    product_features: str
    description: str
    weight: str
    upload_date_str: str
    upload_date: datetime
    customer_review_score: str
    customer_review_count: int
    mba_bsr_str: str
    mba_bsr: str
    mba_bsr_categorie: str
    timestamp: Optional[datetime] = Field(get_berlin_timestamp(without_tzinfo=True))

    def get_fs_import_dict(self, marketplace: Marketplace):
        # transforms data to FS standard naming for e.g. FSMBAShirt class
        return {**self.dict(include={"asin", "title", "brand", "description", "upload_date"}), "listings": get_product_listings_by_list_str(self.product_features, marketplace)}


class BQMBAProductsDetailsDaily(BQTable):
    _bq_table_name: str = PrivateAttr("products_details_daily")
    asin: str
    price: float
    price_str: str
    bsr: int
    bsr_str: str
    array_bsr: str
    array_bsr_categorie: str
    customer_review_score_mean: float
    customer_review_score: str
    customer_review_count: int
    timestamp: Optional[datetime] = Field(get_berlin_timestamp(without_tzinfo=True))

    def get_fs_import_dict(self, marketplace: Marketplace):
        # transforms data to FS standard naming for e.g. FSMBAShirt class
        return {"price_last": self.price, "bsr_last": self.bsr, "bsr_category": get_bsr_category(self.array_bsr_categorie,marketplace), "score_count": self.customer_review_count, "score_last": self.customer_review_score_mean}


class BQMBAProductsNoBsr(BQTable):
    _bq_table_name: str = PrivateAttr("products_no_bsr")
    asin: str
    url:str
    timestamp: Optional[datetime] = Field(get_berlin_timestamp(without_tzinfo=True))

class BQMBAProductsNoMbaShirt(BQTable):
    _bq_table_name: str = PrivateAttr("products_no_mba_shirt")
    asin: str
    url: str
    timestamp: Optional[datetime] = Field(get_berlin_timestamp(without_tzinfo=True))


"""
### functions
"""

def get_product_listings_by_list_str(product_listings_str: str, marketplace: Marketplace) -> List[str]:
    product_features_list = bq_list_str_to_list(product_listings_str)
    product_features_list = [v.strip("'").strip('"') for v in product_features_list]
    return cut_product_feature_list(marketplace, product_features_list)


def bq_list_str_to_list(list_str):
    # create list out of BQ dumped list
    list_str = list_str.strip("[]")
    split_indices = []
    index_open = True
    quote_type = ""
    split_index = []
    for i, char in enumerate(list_str):
        if char == "'" or char == '"':
            if index_open:
                quote_type = char
                split_index.append(i)
                index_open = False
            # if we want a closing index two conditions must be fit 1. closing char must be equal to opening char + (comma char in next 3 chars or string ends in the next 3 chars)
            elif ("," in list_str[i: i +4] or i+ 4 > len(list_str)) and char == quote_type:
                split_index.append(i)
                split_indices.append(split_index)
                # reset split index
                split_index = []
                # search for next index to open list element
                index_open = True
    list_return = []
    for split_index in split_indices:
        list_element = list_str[split_index[0]:split_index[1]]
        list_return.append(list_element)
    return list_return


def is_product_feature_listing(marketplace, product_feature):
    """If on bullet point/ product feature is a listing created by user (contains relevant keywords)"""
    if marketplace == "com":
        return not any(indicator in product_feature.lower() for indicator in
               ["solid color", "imported", "machine wash cold", "lightweight", "classic fit", "classic cut", "double-stitched", "half sleeve", "closure:"])
    elif marketplace == "de":
        return not any(indicator in product_feature.lower() for indicator in
               ["unifarben", "baumwolle", "klassisch geschnitten", "doppelt genäht", "pflegehinweis", "polyester", "grau meliert", "halbarm", "verschluss:"])
    else:
        raise ValueError("Not defined for marketplace %s" % marketplace)

def cut_product_feature_list(marketplace, product_features_list):
    if marketplace in ["de", "com"]:
        product_features_list = [feature for feature in product_features_list if
                                 is_product_feature_listing(marketplace, feature)]
    else:
        raise NotImplementedError

    return product_features_list


def filter_keywords(marketplace, keywords, single_words_to_filter=["t","du"]):
    from mwfunctions.text import KEYWORDS_TO_REMOVE_MARKETPLACE_DICT
    keywords_filtered = []
    for keyword_in_text in keywords:
        if keyword_in_text[len(keyword_in_text)-2:len(keyword_in_text)] in [" t", " T"]:
            keyword_in_text = keyword_in_text[0:len(keyword_in_text)-2]
        filter_keyword = False
        if len(keyword_in_text) < 3:
            filter_keyword = True
        else:
            for keyword_to_remove in KEYWORDS_TO_REMOVE_MARKETPLACE_DICT[marketplace]:
                if keyword_to_remove.lower() in keyword_in_text.lower() or keyword_in_text.lower() in single_words_to_filter:
                    filter_keyword = True
                    break
        if not filter_keyword:
            keywords_filtered.append(keyword_in_text)
    return keywords_filtered

