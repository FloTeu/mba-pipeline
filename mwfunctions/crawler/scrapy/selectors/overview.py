import re
# scrapy selector for overview pages. Raise error if value cannot be received

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
    asin = response.xpath("..").attrib["data-asin"]
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