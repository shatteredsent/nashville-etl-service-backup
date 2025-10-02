import scrapy
import json
import os
from urllib.parse import urlencode
from dotenv import load_dotenv
from nashville.items import EventItem
from datetime import datetime

# Load environment variables
load_dotenv()


class TicketmasterSpider(scrapy.Spider):
    name = 'ticketmaster'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_key = os.getenv('TICKETMASTER_API_KEY')
        if not self.api_key:
            raise ValueError(
                "TICKETMASTER_API_KEY not found in environment variables")

        self.base_url = 'https://app.ticketmaster.com/discovery/v2/events.json'

    def start_requests(self):
        """Initial API request for Nashville events"""
        params = {
            'apikey': self.api_key,
            'dmaId': '343',  # Nashville DMA
            'stateCode': 'TN',
            'city': 'Nashville',
            'size': 200,  # Max events per page
            'page': 0,
            'sort': 'date,asc'
        }

        url = f"{self.base_url}?{urlencode(params)}"
        self.logger.info(f"Fetching events from: {url}")

        yield scrapy.Request(
            url=url,
            callback=self.parse,
            errback=self.handle_error,
            meta={'page': 0}
        )

    def parse(self, response):
        """Parse JSON response and extract events"""
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON: {e}")
            return

        # Check if events exist
        if '_embedded' not in data or 'events' not in data['_embedded']:
            self.logger.warning("No events found in response")
            return

        events = data['_embedded']['events']
        self.logger.info(
            f"Found {len(events)} events on page {response.meta['page']}")

        # Process each event
        for event in events:
            yield self.parse_event(event)

        # Handle pagination
        page_info = data.get('page', {})
        current_page = page_info.get('number', 0)
        total_pages = page_info.get('totalPages', 0)

        # Fetch next page if available (limit to reasonable number)
        if current_page < total_pages - 1 and current_page < 5:  # Limit to first 5 pages
            next_page = current_page + 1
            params = {
                'apikey': self.api_key,
                'dmaId': '343',
                'stateCode': 'TN',
                'city': 'Nashville',
                'size': 200,
                'page': next_page,
                'sort': 'date,asc'
            }

            next_url = f"{self.base_url}?{urlencode(params)}"
            self.logger.info(f"Fetching page {next_page} of {total_pages}")

            yield scrapy.Request(
                url=next_url,
                callback=self.parse,
                errback=self.handle_error,
                meta={'page': next_page}
            )

    def parse_event(self, event):
        """Extract event data and create EventItem"""
        item = EventItem()

        # Basic event info
        item['name'] = event.get('name')
        item['event_id'] = event.get('id')
        item['url'] = event.get('url')
        item['description'] = event.get('info') or event.get('pleaseNote')
        item['source'] = 'ticketmaster'

        # Date information
        dates = event.get('dates', {})
        start = dates.get('start', {})
        item['event_date'] = start.get('dateTime') or start.get('localDate')

        # Venue information
        if '_embedded' in event and 'venues' in event['_embedded']:
            venue = event['_embedded']['venues'][0]  # Take first venue
            item['venue_name'] = venue.get('name')
            item['venue_city'] = venue.get('city', {}).get('name')

            # Build address
            address_parts = []
            if 'address' in venue and 'line1' in venue['address']:
                address_parts.append(venue['address']['line1'])
            if 'city' in venue:
                address_parts.append(venue['city'].get('name', ''))
            if 'state' in venue:
                address_parts.append(venue['state'].get('stateCode', ''))
            if 'postalCode' in venue:
                address_parts.append(venue['postalCode'])

            item['venue_address'] = ', '.join(filter(None, address_parts))
        else:
            item['venue_name'] = None
            item['venue_city'] = None
            item['venue_address'] = None

        return item

    def handle_error(self, failure):
        """Handle request errors"""
        self.logger.error(f"Request failed: {failure.value}")
