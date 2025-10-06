import scrapy
from scrapy_playwright.page import PageMethod
from scraper.nashville.items import BusinessItem
class HotelsSpider(scrapy.Spider):
    name = 'hotels'    
    custom_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    }
    def start_requests(self):
        url = 'https://www.nashville.com/hotels/'
        yield scrapy.Request(
            url,
            headers=self.custom_headers,
            meta=dict(
                playwright=True,
                playwright_include_page=True,
                playwright_page_coroutines=[
                    PageMethod('wait_for_selector', 'div.entry p a[rel="no follow"]')
                ]
            )
        )
    async def parse(self, response):
        page = response.meta["playwright_page"]
        content = await page.content()
        await page.close()        
        selector = scrapy.Selector(text=content)
        for link in selector.css('div.entry a[rel="no follow"]'):
            name = link.css('::text').get()
            if not name or "Best Rate" in name:
                continue
            hotel_block = link.xpath('./ancestor::p[1]')
            if not hotel_block:
                continue
            item = BusinessItem()
            item['name'] = name.strip()
            item['url'] = link.css('::attr(href)').get()
            all_bold_text = hotel_block.css('strong::text').getall()
            address_parts = [
                text.strip() for text in all_bold_text 
                if text.strip() and ("Nashville" in text or any(char.isdigit() for char in text))
            ]
            item['venue_address'] = ', '.join(address_parts)
            all_text_nodes = hotel_block.xpath('.//text()').getall()
            description_parts = []
            is_description = False
            for text in all_text_nodes:
                stripped_text = text.strip()
                if not stripped_text:
                    continue
                if name in stripped_text:
                    is_description = True
                    continue
                if is_description and "Nashville.com" not in stripped_text and stripped_text not in address_parts:
                    description_parts.append(stripped_text)
            item['description'] = ' '.join(description_parts).strip()            
            item['source'] = 'nashville.com-hotels'
            item['category'] = 'hotel'
            item['venue_name'] = item['name']
            item['venue_city'] = 'Nashville'
            item['event_date'] = None
            item['event_id'] = None
            item['neighborhood'] = None

            yield item

