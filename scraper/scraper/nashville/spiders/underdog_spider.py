import scrapy
from scrapy_playwright.page import PageMethod
from datetime import datetime
from nashville.items import EventItem
class UnderdogSpider(scrapy.Spider):
    name = 'underdog'
    allowed_domains = ['tunehatch.com']
    custom_settings = {
        'PLAYWRIGHT_BROWSER_TYPE': 'firefox',
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
            'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
        },
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
    }
    def start_requests(self):
        url = 'https://tunehatch.com/e/upcomingShows/12778804/darkMode=true'
        yield scrapy.Request(
            url=url,
            callback=self.parse,
            meta={
                'playwright': True,
                'playwright_page_methods': [
                    PageMethod('wait_for_selector', '.flyerCardContainer', timeout=15000),
                    PageMethod('wait_for_timeout', 2000),
                ],
            },
        )
    def parse(self, response):
        events = response.css('.flyerCardContainer')
        self.logger.info(f'Found {len(events)} events at The Underdog')
        for event in events:
            item = EventItem()
            item['name'] = event.css('h1.text-2xl.font-black::text').get()
            item['event_date'] = event.css('h2::text').get()
            event_url = event.css('a::attr(href)').get()
            item['url'] = response.urljoin(event_url) if event_url else None
            item['venue_name'] = 'The Underdog'
            item['venue_address'] = 'Nashville, TN'
            item['description'] = None
            item['source'] = 'underdog'
            yield item