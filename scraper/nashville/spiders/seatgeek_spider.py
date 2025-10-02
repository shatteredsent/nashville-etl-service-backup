import scrapy
import json
import os
from urllib.parse import urlencode
from dotenv import load_dotenv
from nashville.items import EventItem

# Load environment variables
load_dotenv()


class SeatgeekSpider(scrapy.Spider):
    name = 'seatgeek'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client_id = os.getenv('SEATGEEK_CLIENT_ID')
        if not self.client_id:
            raise ValueError(
                "SEATGEEK_CLIENT_ID not found in environment variables")

        self.base_url = 'https://api.seatgeek.com/2/events'

    def start_requests(self):
        """Initial API request for Nashville events"""
        params = {
            'client_id': self.client_id,
            'venue.city': 'Nashville',
            'venue.state': 'TN',
            'per_page': 100,  # Max per page
            'page': 1
        }

        url = f"{self.base_url}?{urlencode(params)}"
        self.logger.info(f"Fetching events from: {url}")

        yield scrapy.Request(
            url=url,
            callback=self.parse,
            errback=self.handle_error,
            meta={'page': 1}
        )

    def parse(self, response):
        """Parse JSON response and extract events"""
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON: {e}")
            return

        # Check if events exist
        if 'events' not in data:
            self.logger.warning("No events found in response")
            return

        events = data.get('events', [])
        self.logger.info(
            f"Found {len(events)} events on page {response.meta['page']}")

        # Process each event
        for event in events:
            yield self.parse_event(event)

        # Handle pagination
        meta = data.get('meta', {})
        total = meta.get('total', 0)
        per_page = meta.get('per_page', 100)
        current_page = response.meta['page']
        total_pages = (total + per_page - 1) // per_page  # ceiling division

        # Fetch next page if available (limit to reasonable number)
        if current_page < total_pages and current_page < 10:  # Limit to first 10 pages
            next_page = current_page + 1

            params = {
                'client_id': self.client_id,
                'venue.city': 'Nashville',
                'venue.state': 'TN',
                'per_page': 100,
                'page': next_page
            }

            next_url = f"{self.base_url}?{urlencode(params)}"
            self.logger.info(
                f"Fetching page {next_page} of {total_pages}")

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
        item['name'] = event.get('title') or event.get('short_title')
        item['event_id'] = str(event.get('id'))
        item['url'] = event.get('url')
        item['source'] = 'seatgeek'

        # Description - SeatGeek doesn't always have detailed descriptions
        item['description'] = event.get('description')

        # Date information
        item['event_date'] = event.get('datetime_utc') or event.get(
            'datetime_local')

        # Venue information
        venue = event.get('venue')
        if venue:
            item['venue_name'] = venue.get('name')
            item['venue_city'] = venue.get('city')

            # Build address
            address_parts = []
            if venue.get('address'):
                address_parts.append(venue['address'])
            if venue.get('extended_address'):
                address_parts.append(venue['extended_address'])
            if venue.get('city'):
                address_parts.append(venue['city'])
            if venue.get('state'):
                address_parts.append(venue['state'])
            if venue.get('postal_code'):
                address_parts.append(venue['postal_code'])

            item['venue_address'] = ', '.join(filter(None, address_parts))
        else:
            item['venue_name'] = None
            item['venue_city'] = None
            item['venue_address'] = None

        return item

    def handle_error(self, failure):
        """Handle request errors"""
        self.logger.error(f"Request failed: {failure.value}")
