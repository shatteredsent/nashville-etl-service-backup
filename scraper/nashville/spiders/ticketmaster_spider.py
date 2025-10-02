import scrapy
import os
from urllib.parse import urlencode
from nashville.items import BusinessItem
class TicketmasterSpider(scrapy.Spider):
    name = 'ticketmaster'
    base_url = 'https://app.ticketmaster.com/discovery/v2/events.json'
    def start_requests(self):
        api_key = os.getenv('TICKETMASTER_API_KEY')
        if not api_key:
            self.logger.error("TICKETMASTER_API_KEY not found in environment variables.")
            return
        params = {
            'apikey': api_key,
            'dmaId': '343',
            'size': 200,
            'sort': 'date,asc'
        }
        url = f"{self.base_url}?{urlencode(params)}"
        yield scrapy.Request(url=url, callback=self.parse)
    def parse(self, response):
        if response.status == 401:
            self.logger.error("Ticketmaster API request failed (401 Unauthorized). Check your API key.")
            return
        data = response.json()
        if '_embedded' not in data or 'events' not in data['_embedded']:
            self.logger.warning("No events found in Ticketmaster API response for Nashville.")
            return
        events = data['_embedded']['events']
        for event in events:
            item = BusinessItem()
            item['source'] = 'ticketmaster'
            item['name'] = event.get('name')
            item['url'] = event.get('url')
            item['event_id'] = event.get('id')
            item['description'] = event.get('info') or event.get('pleaseNote')            
            try:
                event_date = event['dates']['start'].get('localDate')
                event_time = event['dates']['start'].get('localTime', '')
                item['event_date'] = f"{event_date} {event_time}".strip()
            except KeyError:
                item['event_date'] = None
            try:
                venue = event['_embedded']['venues'][0]
                item['venue_name'] = venue.get('name')
                item['venue_city'] = venue.get('city', {}).get('name')
                item['venue_address'] = venue.get('address', {}).get('line1')
            except (KeyError, IndexError):
                item['venue_name'] = None
                item['venue_city'] = None
                item['venue_address'] = None            
            item['neighborhood'] = None
            yield item