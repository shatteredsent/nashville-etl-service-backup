import scrapy
import os
import json
from dotenv import load_dotenv
from scraper.nashville.items import BusinessItem
load_dotenv()
class GooglePlacesSpider(scrapy.Spider):
    name = 'google_places'
    allowed_domains = []
    base_url = 'https://places.googleapis.com/v1/places:searchNearby'
    NASHVILLE_LAT = 36.1627
    NASHVILLE_LNG = -86.7816
    RADIUS = 15000
    TYPES_TO_SEARCH = [
        'restaurant',
        'lodging',
        'tourist_attraction',
        'park',
        'museum',
        'bar'
    ]
    def start_requests(self):
        self.api_key = os.getenv('GOOGLE_API_KEY')
        if not self.api_key:
            self.logger.error(
                "GOOGLE_API_KEY not found in environment variables")
            return
        for place_type in self.TYPES_TO_SEARCH:
            body = {
                "includedTypes": [place_type],
                "maxResultCount": 20,
                "locationRestriction": {
                    "circle": {
                        "center": {
                            "latitude": self.NASHVILLE_LAT,
                            "longitude": self.NASHVILLE_LNG
                        },
                        "radius": self.RADIUS
                    }
                }
            }
            yield scrapy.Request(
                url=self.base_url,
                method='POST',
                headers={

                    'Content-Type': 'application/json',

                    'X-Goog-Api-Key': self.api_key,

                    'X-Goog-FieldMask': 'places.displayName,places.formattedAddress,places.location,places.rating,places.userRatingCount,places.id,places.types'
                },
                body=json.dumps(body),
                callback=self.parse,
                meta={'place_type': place_type},
                dont_filter=True
            )
    def parse(self, response):
        data = json.loads(response.text)
        place_type = response.meta['place_type']
        if response.status != 200:
            self.logger.error(
                f"API error for '{place_type}': Status {response.status}")
            return
        places = data.get('places', [])
        if not places:
            self.logger.info(f"No results found for type: {place_type}")
            return
        for place in places:
            item = BusinessItem()
            item['source'] = 'google_places'
            display_name = place.get('displayName', {})
            item['name'] = display_name.get('text', 'Unknown')
            item['venue_address'] = place.get('formattedAddress', '')
            item['category'] = place_type
            location = place.get('location', {})
            item['latitude'] = location.get('latitude')
            item['longitude'] = location.get('longitude')
            place_id = place.get('id', '').replace(
                'places/', '')
            if item['name'] and item['latitude'] and item['longitude']:
                item['url'] = f"https://www.google.com/maps/search/?api=1&query={item['latitude']},{item['longitude']}&query_place_id={place_id}"
            rating = place.get('rating', 'N/A')
            rating_count = place.get('userRatingCount', 0)
            item['description'] = f"Rating: {rating} ({rating_count} reviews)"
            item['venue_city'] = 'Nashville'
            yield item
        self.logger.info(
            f"Scraped {len(places)} places for type: {place_type}")