import scrapy
from nashville.items import BusinessItem
class GuruSpider(scrapy.Spider):
    name = 'guru'
    start_urls = ['https://nashvilleguru.com/48116/lunch-restaurants-nashville']
    def parse(self, response):
        list_items = response.css('div.entry-content ul li')
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