import scrapy
import os
from urllib.parse import urlencode
from nashville.items import BusinessItem
from datetime import datetime, timezone
class TicketmasterSpider(scrapy.Spider):
    name = 'ticketmaster'
    base_url = 'https://app.ticketmaster.com/discovery/v2/events.json'
    def start_requests(self):
        api_key = os.getenv('TICKETMASTER_API_KEY')
        if not api_key:
            self.logger.error("TICKETMASTER_API_KEY not found in environment variables.")
            return
        now_utc = datetime.now(timezone.utc)
        start_datetime = now_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        params = {
            'apikey': api_key,
            'dmaId': '343',
            'size': 200,
            'sort': 'date,asc',
            'startDateTime': start_datetime
        }
        url = f"{self.base_url}?{urlencode(params)}"
        yield scrapy.Request(url=url, callback=self.parse, errback=self.handle_error, meta={'params': params})
    def parse(self, response):
        if response.status != 200:
            self.logger.error(f"Ticketmaster API request failed with status {response.status}")
            return
        data = response.json()
        if '_embedded' in data and 'events' in data['_embedded']:
            for event in data['_embedded']['events']:
                item = self.parse_event(event)
                if item:
                    yield item
        page_info = data.get('page', {})
        current_page = page_info.get('number', 0)
        total_pages = page_info.get('totalPages', 0)
        if current_page < total_pages - 1 and current_page < 5:
            next_page = current_page + 1
            params = response.meta['params']
            params['page'] = next_page
            next_url = f"{self.base_url}?{urlencode(params)}"
            yield scrapy.Request(
                url=next_url,
                callback=self.parse,
                errback=self.handle_error,
                meta={'params': params}
            )
    def parse_event(self, event):
        url = event.get('url')
        if not url or not url.startswith('http'):
            self.logger.debug(f"Skipping event with invalid URL: {event.get('name')}")
            return None
        item = BusinessItem()
        item['source'] = 'ticketmaster'
        item['name'] = event.get('name', '').strip()
        item['url'] = url
        item['event_id'] = event.get('id')
        description = event.get('info') or event.get('pleaseNote')
        item['description'] = description.strip() if description else None
        try:
            event_date = event['dates']['start'].get('localDate')
            event_time = event['dates']['start'].get('localTime', '')
            item['event_date'] = f"{event_date} {event_time}".strip()
        except (KeyError, TypeError):
            item['event_date'] = None
        try:
            venue = event['_embedded']['venues'][0]
            item['venue_name'] = venue.get('name', '').strip()
            item['venue_city'] = venue.get('city', {}).get('name', '').strip()
            item['venue_address'] = venue.get('address', {}).get('line1', '').strip()
        except (KeyError, IndexError):
            item['venue_name'] = None
            item['venue_city'] = None
            item['venue_address'] = None
        item['neighborhood'] = None
        if item.get('venue_city') and 'nashville' in item['venue_city'].lower():
            return item
        else:
            self.logger.debug(f"Skipping event in another city: {item['name']} in {item.get('venue_city')}")
            return None
    def handle_error(self, failure):
        self.logger.error(f"Ticketmaster request failed: {failure.value}")