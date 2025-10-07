SPIDER_MODULES = ['nashville.spiders']
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright_stealth.handler.ScrapyPlaywrightStealthDownloadHandler",
    "https": "scrapy_playwright_stealth.handler.ScrapyPlaywrightStealthDownloadHandler",
}

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

