import re
from mwfunctions.pydantic.base_classes import Marketplace


def get_search_number_products_bar_text(response) -> str:
    try:
        count_results_bar_text = response.css('span.celwidget div.a-section span::text')[0].get()
    except IndexError:
        count_results_bar_text = response.css("div.sg-col-inner .a-section span::text")[0].get()
    return count_results_bar_text

# scrapy selector for overview pages. Raise error if value cannot be received
def mba_get_number_of_products_in_niche(response, marketplace: Marketplace) -> int: # could throw IndexError or ValueError
    count_results_bar_text = get_search_number_products_bar_text(response)
    if marketplace in [Marketplace.COM, Marketplace.UK]:
        return int(count_results_bar_text.split(" results")[0].split(" ")[-1].replace(',', '').replace('.', ''))
    elif marketplace == Marketplace.DE:
        return int(count_results_bar_text.split(" Ergebnis")[0].split(" ")[-1].replace(',', '').replace('.', ''))
    else:
        raise NotImplementedError

def mba_get_number_of_products_in_niche_is_exact(response) -> bool: # could throw IndexError or ValueError
    count_results_bar_text = get_search_number_products_bar_text(response)
    # TODO: check for japanese indicator
    return not any([indicator in count_results_bar_text for indicator in ["over", "mehr", "plus", "più", "más", "余り"]])



def mba_get_title(response):
    title = response.css("a.a-link-normal.a-text-normal")[0].css("span::text").get()
    if title == None:
        raise ValueError("Could not get title information")
    else:
        return title.strip()


def mba_get_brand(response):
    brand = response.css("h5.s-line-clamp-1 span::text")[0].get()
    if brand == None:
        raise ValueError("Could not get brand information")
    else:
        return brand.strip()


def mba_get_url_product(response, url_mba):
    url_product = response.css("div.a-section.a-spacing-none a::attr(href)").get()
    if url_product == None:
        raise ValueError("Could not get url_product information")
    else:
        return "/".join(url_mba.split("/")[0:3]) + url_product


def mba_get_img_urls(response):
    is_url = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    urls = response.css("div.a-section a img::attr(srcset)").get()
    if urls == None:
        raise ValueError("Could not get img_urls information")
    else:
        img_url = []
        for url in urls.split(" "):
            if re.match(is_url, url) is not None:
                img_url.append(url)
        if len(img_url) == 5:
            return img_url[0], img_url[1], img_url[2], img_url[3], img_url[4]
        else:
            raise ValueError("Could not get all 5 img_urls information")

def mba_get_price(response, marketplace):
    if marketplace == "com":
        price = response.css("span.a-price-whole::text")[0].get() + \
                response.css("span.a-price-decimal::text")[0].get() + \
                response.css("span.a-price-fraction::text")[0].get()
    else:
        price = response.css("span.a-price-whole::text")[0].get()
    if price == None:
        raise ValueError("Could not get price information")
    else:
        return price.strip()

def mba_get_asin(response):
    try:
        asin = response.xpath("..").attrib["data-asin"]
    except Exception as e:
        raise ValueError("Could not get asin. " + str(e))
    if asin == None:
        raise ValueError("Could not get asin")
    else:
        return asin.strip()

def mba_get_uuid(response):
    uuid = response.xpath("..").attrib["data-uuid"]
    if uuid == None:
        raise ValueError("Could not get uuid")
    else:
        return uuid.strip()