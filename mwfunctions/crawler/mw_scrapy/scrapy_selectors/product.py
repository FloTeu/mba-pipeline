import scrapy
import dateparser
import dateutil.parser

from deep_translator import GoogleTranslator
from urllib.parse import urlparse
from datetime import datetime
from contextlib import suppress

def get_price(response, marketplace):
    # old code outcommented
    # price_div = response.css('div#price')
    # price_str = price_div.css('span#priceblock_ourprice::text').get()
    price_str = response.css('div#centerCol span.a-price span::text').get()
    price = 0.0
    if price_str == None:
        raise ValueError("Could not get price information")
    else:
        try:
            if marketplace == "de":
                price = float(price_str.split("\xa0")[0].split("€")[0].replace(",", "."))
            else:
                if price_str[0] == "$":
                    price = float(price_str.split("$")[1].replace(",", "."))
                elif price_str[-1] == "$":
                    price = float(price_str.split("$")[0].replace(",", "."))
                else:
                    raise NotImplementedError
        except:
            print("Could not get price as float")

    return price_str, price


def mba_bsr_str_to_mba_data(mba_bsr_str, marketplace):
    mba_bsr = 0
    array_mba_bsr = []
    array_mba_bsr_categorie = []

    if "Nr. " in mba_bsr_str: #  marketplace de or com with germand language
        bsr_iterator = mba_bsr_str.split("Nr. ")
        bsr_iterator = bsr_iterator[1:len(bsr_iterator)]
        for bsr_str in bsr_iterator:
            # response happens to contain also , seperated integer like 296,206
            bsr = int(bsr_str.split("in")[0].replace(".", "").replace(",", ""))
            array_mba_bsr.append(bsr)
            bsr_categorie = bsr_str.split("(")[0].replace("\xa0", " ").split("in ")[1].strip()
            array_mba_bsr_categorie.append(bsr_categorie)
        mba_bsr = int(bsr_iterator[0].split("in")[0].replace(".", "").replace(",", ""))
    elif "#" in mba_bsr_str: # marketplace com
        bsr_iterator = mba_bsr_str.split("#")
        bsr_iterator = bsr_iterator[1:len(bsr_iterator)]
        for bsr_str in bsr_iterator:
            bsr = int(bsr_str.split("in")[0].replace(".", "").replace(",", ""))
            array_mba_bsr.append(bsr)
            bsr_categorie = bsr_str.split("(")[0].replace("\xa0", " ").split("in ")[1].strip()
            array_mba_bsr_categorie.append(bsr_categorie)
        mba_bsr = int(bsr_iterator[0].split("in")[0].replace(".", "").replace(",", ""))
    else:
        raise ValueError("BSR data filtering not defined for marketplace %s" % marketplace)

    return mba_bsr, array_mba_bsr, array_mba_bsr_categorie


def get_bsr(response, marketplace):
    product_information = response.css('div#detailBullets')
    bsr_li = product_information.css("li#SalesRank")
    # try to get bsr out of first li of second ul if id SalesRank is not provided in html
    if bsr_li == None or bsr_li == []:
        try:
            bsr_li = product_information.css("ul")[1].css("li")[0:1]
        except:
            pass

    mba_bsr_str = ""
    mba_bsr = 0
    array_mba_bsr = []
    array_mba_bsr_categorie = []
    if bsr_li != None and bsr_li != [] and type(bsr_li) == scrapy.selector.unified.SelectorList and "".join(
            bsr_li.css("::text").getall()).replace("\n", "").strip() != "":
        try:
            mba_bsr_str = "".join(bsr_li.css("::text").getall()).replace("\n", "")
            mba_bsr, array_mba_bsr, array_mba_bsr_categorie = mba_bsr_str_to_mba_data(mba_bsr_str, marketplace)
        except Exception as e:
            if len(product_information) > 0:
                raise ValueError(
                    f"Could not get bsr information. \n Product information block: {product_information[0].extract()} ")
            else:
                raise ValueError(f"Could not get bsr information. \n Product information block: {product_information} ")
    else:
        customer_review_score_mean, customer_review_score, customer_review_count = get_customer_review(response, marketplace)
        if customer_review_score_mean > 0:
            if len(product_information) > 0:
                raise ValueError(
                    f"Designs has reviews but no bsr (impossible). \n Product information block: {product_information[0].extract()}")
            else:
                raise ValueError(
                    f"Designs has reviews but no bsr (impossible). \n Product information block: {product_information}")

    return mba_bsr_str, mba_bsr, array_mba_bsr, array_mba_bsr_categorie


def get_customer_review(response, marketplace):
    product_information = response.css('div#detailBullets')
    customer_review_div = product_information.css("div#detailBullets_averageCustomerReviews")
    customer_review_score_mean = 0.0
    customer_review_score = ""
    customer_review_count = 0
    if customer_review_div != None and customer_review_div != []:
        try:
            try:
                customer_review_score = customer_review_div.css("span.a-declarative")[0].css("a")[0].css("i")[0].css(
                    "span::text").get()
            except:
                customer_review_score = ""
            try:
                customer_review_count = int(
                    customer_review_div.css("span#acrCustomerReviewText::text").get().split(" ")[0])
            except:
                customer_review_count = 0
            try:
                if marketplace == "com":
                    customer_review_score_mean = float(customer_review_score.split(" out of")[0])
                elif marketplace == "de":
                    customer_review_score_mean = float(customer_review_score.split(" von")[0].replace(",", "."))
            except:
                customer_review_score_mean = 0.0
        except:
            pass

    return customer_review_score_mean, customer_review_score, customer_review_count


def get_title(response):
    title = response.css('span#productTitle::text').get()
    if title == None:
        raise ValueError("Could not get title information")
    else:
        return title.strip()


def get_brand_infos(response):
    brand = response.css('a#bylineInfo::text').get()
    url_brand = response.css('a#bylineInfo::attr(href)').get()
    if brand == None:
        raise ValueError("Could not get brand name")
    if url_brand == None:
        raise ValueError("Could not get brand url")

    parsed_uri = urlparse(response.url)
    mba_base_url = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)
    url_brand = mba_base_url + url_brand.strip()
    brand = brand.strip()
    return brand, url_brand


def get_fit_types(response):
    array_fit_types = []
    div_fit_types = response.css('div#variation_fit_type span.a-size-base')
    if div_fit_types != None and len(div_fit_types) > 0:
        for fit_type in div_fit_types:
            array_fit_types.append(fit_type.css("::text").get().strip())
        return array_fit_types
    else:
        try:
            fit_type = response.css('div#variation_fit_type span::text').get().strip()
            array_fit_types.append(fit_type)
            return array_fit_types
        except:
            raise ValueError("Could not get fit types")


def get_color_infos(response):
    array_color_names = []
    span_color_names = response.css('div#variation_color_name span.a-declarative')
    if span_color_names == []:
        span_color_names = response.css("div#inline-twister-expander-content-color_name li")
    if span_color_names != None and len(span_color_names) > 0:
        for color_name in span_color_names:
            color_name = color_name.css("img::attr(alt)").get()
            if color_name != "":
                array_color_names.append(color_name)
        return array_color_names, len(array_color_names)
    else:
        try:
            color = response.css('div#variation_color_name span.selection::text').get().strip()
            array_color_names.append(color)
            return array_color_names, len(array_color_names)
        except:
            raise ValueError("Could not get color names")


def get_product_features(response):
    product_feature = response.css('div#feature-bullets')
    if product_feature == None:
        product_feature = response.css("div#dpx-feature-bullets")
    if product_feature != None:
        array_product_features = []
        for feature in product_feature.css("ul li"):
            array_product_features.append(feature.css("::text").get().strip())
        return array_product_features
    else:
        raise ValueError("Could not get product feature")


def get_description(response):
    product_description = response.css('div#productDescription span *::text').get()
    if product_description != None:
        # try to get text from p tag if nothing could be found in span tag
        if product_description.strip() == "":
            product_description = response.css('div#productDescription p *::text').get()
        return product_description.strip()
    else:
        raise ValueError("Could not get product description")

def get_image_url(response):
    try:
        return response.css('.image img::attr(src)').get()
    except Exception as e:
        raise ValueError("Could not get image url")

def get_weight(response):
    weight = "not found"
    product_information = get_product_information_lis(response)
    if product_information != None:
        for li in product_information:
            try:
                info_text = li.css("span span::text").getall()[0].lower()
                if "gewicht" in info_text or "weight" in info_text or "abmessung" in info_text or "dimension" in info_text:
                    weight = li.css("span span::text").getall()[1]
                    return weight.strip()
            except Exception as e:
                print(str(e))
                raise ValueError("Could not get weight")
        raise ValueError("Could not get weight")
    else:
        raise ValueError("Could not get weight")

def get_product_information_lis(response):
    """ Contains every li tag in detail product information (upload_date, bsr, customer review, weight etc.)
        Might be possible that customer review and bsr are excluded (First query (detailBullets) is empty)
    """
    product_information = response.css('div#detailBullets li')
    if product_information == None or product_information == []:
        product_information = response.css('div#dpx-detail-bullets_feature_div li')
    if product_information == None or product_information == []:
        product_information = response.css('div#detailBullets_feature_div li')
    return product_information

def get_upload_date(response):
    possible_datetime_formats = ["%d, %B %Y", "%d %B %Y", "%dst %B %Y", "%B %dnd, %Y", "%dnd %B %Y", "%dnd, %B %Y", "%drd %B %Y", "%dth %B %Y", "%B %d, %Y", "%d. %B %Y"] # read from right to left
    upload_date_str = None
    upload_date_str_en = None
    product_information = get_product_information_lis(response)
    if product_information != None and product_information != []:
        for li in product_information:
            try:
                info_text = li.css("span span::text").getall()[0].lower()
                if "seit" in info_text or "available" in info_text:
                    upload_date_str = li.css("span span::text").getall()[1]
                    # TODO check if this code works for different proxy locations and marketplaces. What happens if amazon changes format??
                    upload_date_str_en = GoogleTranslator(source='auto', target='en').translate(upload_date_str)
                    while len(possible_datetime_formats) > 0:
                        with suppress(Exception):
                            upload_date_obj = datetime.strptime(upload_date_str_en, possible_datetime_formats.pop())
                            break
                    #upload_date_obj = dateparser.parse(upload_date_str)
                    upload_date = upload_date_obj.strftime('%Y-%m-%d')
                    return upload_date.strip(), upload_date_obj
            except:
                if upload_date_str and upload_date_str_en:
                    raise ValueError(f"Could not get upload date. upload string {upload_date_str}, translated: {upload_date_str_en}")
                else:
                    raise ValueError("Could not get upload date")
        raise ValueError("Could not get upload date")
    else:
        raise ValueError("Could not get upload date")
