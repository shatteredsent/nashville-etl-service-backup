import os
from dotenv import load_dotenv
import scrapy
import requests
from nashville.items import BusinessItem

load_dotenv()


class YelpSpider(scrapy.Spider):
    name = 'yelp'
    CATEGORIES = ['musicvenues', 'venues', 'bars',
                  'nightlife', 'restaurants', 'arts']
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': None
        },
        'DEFAULT_REQUEST_HEADERS': None
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_key = os.getenv('YELP_API_KEY')
        if not self.api_key:
            raise ValueError("YELP_API_KEY not found in environment variables")
        self.base_url = 'https://api.yelp.com/v3/businesses/search'
        self.headers = {'Authorization': f'Bearer {self.api_key}',
                        'Accept': 'application/json', 'Accept-Language': 'en_US'}
        self.logger.info(f"Using API key starting with: {self.api_key[:5]}...")

    def start_requests(self):
        params = {
            'location': 'Nashville, TN',
            'limit': 50,
            'categories': ','.join(self.CATEGORIES),
            'sort_by': 'rating',
            'radius': 40000,
            'offset': 0
        }
        self.logger.info(f"Starting venue search with parameters: {params}")

        while True:
            response = requests.get(
                self.base_url, headers=self.headers, params=params)
            if response.status_code != 200:
                self.logger.error(
                    f"Request failed with status {response.status_code}")
                break

            try:
                data = response.json()
                businesses = data.get('businesses', [])
                self.logger.info(
                    f"Found {len(businesses)} venues (offset: {params['offset']})")

                for business in businesses:
                    yield self.parse_business(business)

                total = data.get('total', 0)
                next_offset = params['offset'] + params['limit']
                if next_offset >= min(total, 1000):
                    break

                params['offset'] = next_offset
                self.logger.info(
                    f"Fetching next page with offset {next_offset}/{min(total, 1000)}")
            except Exception as e:
                self.logger.error(f"Failed to process response: {e}")
                break

    def parse_business(self, business):
        item = BusinessItem()
        item['name'] = business.get('name')
        item['url'] = business.get('url')
        item['source'] = 'yelp'

        desc_parts = []
        if business.get('rating'):
            desc_parts.append(f"Rating: {business['rating']}/5")
        if business.get('review_count'):
            desc_parts.append(f"Reviews: {business['review_count']}")
        if business.get('price'):
            desc_parts.append(f"Price: {business['price']}")
        if business.get('categories'):
            desc_parts.append(
                f"Categories: {', '.join(cat['title'] for cat in business['categories'])}")
        if business.get('display_phone'):
            desc_parts.append(f"Phone: {business['display_phone']}")
        if business.get('hours', [{}])[0].get('is_open_now') is not None:
            desc_parts.append(
                f"Status: {'Open' if business['hours'][0]['is_open_now'] else 'Closed'}")

        location = business.get('location', {})
        if location.get('display_address'):
            desc_parts.append(
                f"Address: {', '.join(location['display_address'])}")
        item['neighborhood'] = location.get('neighborhoods', [None])[
            0] or location.get('city')

        item['description'] = ' | '.join(desc_parts)
        return item

        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error(
                f"HttpError on {response.url}, Status: {response.status}")
            self.logger.error(f"Response headers: {response.headers}")
            self.logger.error(f"Response Body: {response.text}")
        else:
            self.logger.error("Unknown error occurred")
