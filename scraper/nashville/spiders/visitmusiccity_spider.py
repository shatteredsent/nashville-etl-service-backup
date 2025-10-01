import scrapy
from nashville.items import EventItem
class VisitMusicCitySpider(scrapy.Spider):
    name = 'visitmusiccity'
    start_urls = ['https://www.visitmusiccity.com/nashville-events/upcoming-events']
    def parse(self, response):
        events = response.css('article.generic-card.generic-card--grid')        
        for event in events:
            item = EventItem()
            item['name'] = event.css('h5.generic-card__heading a::text').get()
            item['url'] = response.urljoin(event.css('h5.generic-card__heading a::attr(href)').get())
            item['event_date'] = event.css('div.generic-card__event-dates::text').get()
            item['event_id'] = None
            item['venue_name'] = None
            item['venue_city'] = 'Nashville'
            item['venue_address'] = None
            item['description'] = None
            item['source'] = 'visitmusiccity'
            if item['name']:
                item['name'] = item['name'].strip()
            if item['event_date']:
                item['event_date'] = item['event_date'].strip()
            yield item
        next_page = response.css('li.pager__item--next a::attr(href)').get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)
