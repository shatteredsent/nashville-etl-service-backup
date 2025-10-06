import scrapy
from scrapy_playwright.page import PageMethod
from scraper.nashville.items import BusinessItem
class NashvilleComSpider(scrapy.Spider):
    name = 'nashville_com'    
    custom_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    }
    def start_requests(self):
        url = 'https://www.nashville.com/calendar-of-events/'
        yield scrapy.Request(
            url,
            headers=self.custom_headers,
            meta=dict(
                playwright=True,
                playwright_include_page=True,
                playwright_page_coroutines=[
                    PageMethod('wait_for_selector', 'div.tribe-events-calendar-list__event-row')
                ]
            )
        )
    async def parse(self, response):
        page = response.meta["playwright_page"]
        content = await page.content()
        await page.close()        
        selector = scrapy.Selector(text=content)        
        events = selector.css('div.tribe-events-calendar-list__event-row')        
        for event in events:
            item = BusinessItem()
            item['source'] = 'nashville.com'
            item['name'] = event.css('h3.tribe-events-calendar-list__event-title a::text').get()            
            detail_page_url = event.css('h3.tribe-events-calendar-list__event-title a::attr(href)').get()
            if detail_page_url:
                yield response.follow(
                    detail_page_url, 
                    callback=self.parse_event_details, 
                    meta={'item': item},
                    headers=self.custom_headers
                )
    def parse_event_details(self, response):
        item = response.meta['item']        
        date_string = response.css('span.tribe-event-date-start::text').get()
        time_string = response.css('span.tribe-event-time::text').get()
        if date_string and time_string:
            item['event_date'] = f"{date_string.strip()} @ {time_string.strip()}"
        elif date_string:
            item['event_date'] = date_string.strip()
        else:
            item['event_date'] = None            
        item['url'] = response.url
        item['venue_name'] = response.css('dd.tribe-venue a::text').get()
        address_parts = response.css('span.tribe-street-address::text, span.tribe-locality::text').getall()
        item['venue_address'] = ', '.join(part.strip().strip(',') for part in address_parts if part)
        item['description'] = " ".join(response.css('div.tribe-events-single-event-description p::text').getall()).strip()
        
        yield item