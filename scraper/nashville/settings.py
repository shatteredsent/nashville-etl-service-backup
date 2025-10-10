BOT_NAME = "nashville"
SPIDER_MODULES = ["scraper.nashville.spiders"]
NEWSPIDER_MODULE = "scraper.nashville.spiders"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
ROBOTSTXT_OBEY = False
ITEM_PIPELINES = {
   "scraper.nashville.pipelines.PostgresPipeline": 300,
}
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"