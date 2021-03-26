# Scrapy settings for mba_crawler project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html
from proxy import proxy_handler

BOT_NAME = 'mba_crawler'

SPIDER_MODULES = ['mba_crawler.spiders']
NEWSPIDER_MODULE = 'mba_crawler.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'mba_crawler (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False
use_public_proxies = False
use_private_proxies = False
only_usa = True

if use_public_proxies:
    ROTATING_PROXY_LIST = proxy_handler.get_public_http_proxy_list(only_usa=only_usa)
elif use_private_proxies:
    ROTATING_PROXY_LIST = proxy_handler.get_private_http_proxy_list(only_usa=only_usa)
else:
    ROTATING_PROXY_LIST = proxy_handler.get_http_proxy_list(only_usa=only_usa)

ROTATING_PROXY_BAN_POLICY = 'mba_crawler.policy.MyBanPolicy'
ROTATING_PROXY_CLOSE_SPIDER = True
ROTATING_PROXY_PAGE_RETRY_TIMES = 20 

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 5

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 5

DOWNLOAD_TIMEOUT = 40

MAX_CAPTCHA_NUMBER = 10
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'mba_crawler.middlewares.MbaCrawlerSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# TODO: ADD rotating user agent middleware
# issue: https://stackoverflow.com/questions/56889999/how-to-rotate-proxies-and-user-agents
DOWNLOADER_MIDDLEWARES = {
    #'mba_crawler.middlewares.MbaCrawlerDownloaderMiddleware': 543,
    'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
    'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#    'mba_crawler.pipelines.MbaCrawlerImagePipeline': 300,
# }

# IMAGES_STORE = 'gs://5c0ae2727a254b608a4ee55a15a05fb7_public/mba-shirts/'
# GCS_PROJECT_ID = 'mba-pipeline'

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 5
if use_public_proxies or (not use_public_proxies and not use_private_proxies):
    # The maximum download delay to be set in case of high latencies
    AUTOTHROTTLE_MAX_DELAY = 30
else:
    # The maximum download delay to be set in case of high latencies
    AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 10
# Enable showing throttling stats for every response received:
AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
