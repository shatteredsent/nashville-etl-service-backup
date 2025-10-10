import scrapy
import json
import os
from urllib.parse import urlencode
from scraper.nashville.items import BusinessItem

class SeatgeekSpider(scrapy.Spider):
    name = 'seatgeek'
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client_id = os.getenv('SEATGEEK_CLIENT_ID')
        if not self.client_id:
            raise ValueError("SEATGEEK_CLIENT_ID not found in environment variables")
        self.base_url = 'https://api.seatgeek.com/2/events'

    def start_requests(self):
        params = {
            'client_id': self.client_id,
            'venue.city': 'Nashville',
            'venue.state': 'TN',
            'per_page': 50,
            'page': 1
        }
        url = f"{self.base_url}?{urlencode(params)}"
        self.logger.info(f"Fetching events from: {url}")
        yield scrapy.Request(
            url=url,
            callback=self.parse,
            errback=self.handle_error,
            meta={'page': 1, 'params': params}
        )

    def parse(self, response):
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON: {e}")
            return
        
        if 'events' not in data:
            self.logger.warning("No events found in response")
            return

        events = data.get('events', [])
        for event in events:
            yield self.parse_event(event)

        meta = data.get('meta', {})
        total = meta.get('total', 0)
        per_page = meta.get('per_page', 50)
        current_page = response.meta['page']
        total_pages = (total + per_page - 1) // per_page

        if current_page < total_pages and current_page < 10: # Limit to first 10 pages
            next_page = current_page + 1
            params = response.meta['params']
            params['page'] = next_page
            next_url = f"{self.base_url}?{urlencode(params)}"
            self.logger.info(f"Fetching page {next_page} of {total_pages}")
            yield scrapy.Request(
                url=next_url,
                callback=self.parse,
                errback=self.handle_error,
                meta={'page': next_page, 'params': params}
            )

    def parse_event(self, event):
        item = BusinessItem()
        item['name'] = event.get('title') or event.get('short_title')
        item['event_id'] = str(event.get('id'))
        item['url'] = event.get('url')
        item['source'] = 'seatgeek'
        item['description'] = event.get('description')
        item['event_date'] = event.get('datetime_utc') or event.get('datetime_local')
        
        venue = event.get('venue')
        if venue:
            item['venue_name'] = venue.get('name')
            item['venue_city'] = venue.get('city')
            address_parts = [
                venue.get('address', ''),
                venue.get('extended_address', '')
            ]
            item['venue_address'] = ', '.join(filter(None, address_parts))
        
        return item

    def handle_error(self, failure):
        self.logger.error(f"Request failed: {failure.value}")