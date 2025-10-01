import scrapy
from scrapy_playwright.page import PageMethod
from nashville.items import BusinessItem
def should_abort_request(request):
    if "google" in request.url or "ad" in request.url:
        return True
    if request.resource_type in ("image", "font"):
        return True
    return False
class GuruSpider(scrapy.Spider):
    name = 'guru'    
    def start_requests(self):
        url = 'https://nashvilleguru.com/48116/lunch-restaurants-nashville'
        yield scrapy.Request(
            url,
            meta=dict(
                playwright=True,
                playwright_include_page=True,
                playwright_context_kwargs={
                    "abort_request": should_abort_request,
                },
                playwright_page_methods=[
                    PageMethod('wait_for_selector', 'div.entry-content ul li:first-child')
                ]
            )
        )
    async def parse(self, response):
        list_items = response.css('ul li')        
        for item_li in list_items:
            item = BusinessItem()
            item['name'] = item_li.css('a::text').get()
            item['url'] = item_li.css('a::attr(href)').get()
            
            all_text = item_li.css('::text').getall()
            if len(all_text) > 1:
                description_text = all_text[-1]
                item['description'] = description_text.replace(' - ', '').strip()
            else:
                item['description'] = None
                
            item['source'] = 'nashvilleguru'
            item['neighborhood'] = None
            yield item