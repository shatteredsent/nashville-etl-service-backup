"""Nashville ArcGIS Spider - Simplified and Consolidated"""
import scrapy
from scrapy.http import FormRequest
import logging
from typing import Optional, Tuple, Dict, Any
from pyproj import Transformer
from scraper.nashville.items import BusinessItem


class NashvilleArcGISSpider(scrapy.Spider):
    name = 'nashville_arcgis'
    allowed_domains = ['services2.arcgis.com']
    custom_settings = {
        'CONCURRENT_REQUESTS': 8,
        'DOWNLOAD_DELAY': 0.25,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
    }
    SOURCE_CRS = 2274  # Tennessee State Plane
    TARGET_CRS = 4326  # WGS84 (lat/lng)
    RECORDS_PER_REQUEST = 1000
    VALID_LAT_RANGE = (35.0, 37.0)
    VALID_LNG_RANGE = (-88.0, -85.0)

    DATASETS = [
        {
            'name': 'Parks',
            'url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Parks_Facilities/FeatureServer/1',
            'category': 'park',
            'name_field': 'FacilityName',
            'address_field': 'Address',
            'extra_fields': ['FacilityType', 'Description', 'PhoneNumber', 'Website'],
        },
        {
            'name': 'Libraries',
            'url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Library_Facilities/FeatureServer/0',
            'category': 'public_facility',
            'name_field': 'FacilityName',
            'address_field': 'Address',
            'extra_fields': ['PhoneNumber', 'MondayOpen', 'MondayClose'],
        },
        {
            'name': 'Fire Stations',
            'url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Fire_Station_Locations/FeatureServer/0',
            'category': 'public_facility',
            'name_field': 'FacilityName',
            'address_field': 'Address',
            'extra_fields': [],
        },
        {
            'name': 'Police Precincts',
            'url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Police_Precincts_view/FeatureServer/0',
            'category': 'public_facility',
            'name_field': 'FacilityName',
            'address_field': 'Address',
            'extra_fields': ['CommanderName', 'PhoneNumber', 'Website'],
        },
        {
            'name': 'Public Health Clinics',
            'url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Public_Health_Clinics_view/FeatureServer/0',
            'category': 'public_facility',
            'name_field': 'ClinicName',
            'address_field': 'Address',
            'extra_fields': ['Phone', 'Hours'],
        },
        {
            'name': 'Public Artwork',
            'url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Metro_Arts_Public_Artwork_view/FeatureServer/0',
            'category': 'point_of_interest',
            'name_field': 'ArtworkName',
            'address_field': 'Location',
            'extra_fields': ['FirstName', 'LastName', 'Medium', 'WebLink'],
        },
        {
            'name': 'Cemetery Survey',
            'url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Davidson_County_Cemetery_Survey_Table_view/FeatureServer/0',
            'category': 'point_of_interest',
            'name_field': 'Cemetery_Name',
            'address_field': 'Street',
            'extra_fields': ['Graveyard_Type', 'Known_Burials', 'Map_ID', 'Locale'],
        },
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.transformer = Transformer.from_crs(
            self.SOURCE_CRS, self.TARGET_CRS, always_xy=True)
        self.logger.info(
            f"Spider initialized with {len(self.DATASETS)} datasets")

    def start_requests(self):
        for dataset in self.DATASETS:
            self.logger.info(
                f"Starting scrape: {dataset['name']} ({dataset['category']})")
            yield self._create_request(dataset, 0)

    def _create_request(self, dataset: Dict[str, Any], offset: int):
        formdata = {
            'where': '1=1',
            'outFields': ','.join([dataset['name_field'], dataset['address_field']] + dataset['extra_fields']),
            'returnGeometry': 'true',
            'f': 'json',
            'resultOffset': str(offset),
            'resultRecordCount': str(self.RECORDS_PER_REQUEST)
        }
        return FormRequest(
            url=f"{dataset['url']}/query",
            formdata=formdata,
            callback=self.parse,
            meta={'dataset': dataset, 'offset': offset},
            errback=self.handle_error,
            dont_filter=True
        )

    def parse(self, response):
        dataset, offset = response.meta['dataset'], response.meta['offset']
        try:
            data = response.json()
        except ValueError as e:
            self.logger.error(f"JSON parse error for {dataset['name']}: {e}")
            return
        if 'error' in data:
            self.logger.error(
                f"API error for {dataset['name']}: {data['error']}")
            return
        features = data.get('features', [])
        if not features:
            self.logger.info(
                f"Completed {dataset['name']} - no more features at offset {offset}")
            return
        self.logger.info(
            f"Processing {len(features)} features from {dataset['name']} (offset: {offset})")
        for feature in features:
            if item := self._parse_feature(feature, dataset):
                yield item
        if len(features) >= self.RECORDS_PER_REQUEST:
            yield self._create_request(dataset, offset + self.RECORDS_PER_REQUEST)

    def _parse_feature(self, feature: Dict[str, Any], dataset: Dict[str, Any]) -> Optional[BusinessItem]:
        """Parse single feature into BusinessItem"""
        attrs, geom = feature.get(
            'attributes', {}), feature.get('geometry', {})
        if not (name := self._get_valid_name(attrs.get(dataset['name_field']))):
            return None
        lng, lat = self._extract_coords(geom)
        item = BusinessItem()
        item['source'] = 'nashville_arcgis'
        item['category'] = dataset['category']
        item['venue_city'] = 'Nashville'
        item['name'] = name
        item['venue_address'] = self._get_address(attrs, dataset)
        item['longitude'] = lng
        item['latitude'] = lat
        item['description'] = self._build_description(attrs, dataset)
        item['url'] = f"https://www.google.com/maps/search/?api=1&query={lat},{lng}" if (
            lat and lng) else None
        return item if (item['venue_address'] or (lat and lng)) else None

    def _get_valid_name(self, name: Any) -> Optional[str]:
        if not name:
            return None
        name_str = str(name).strip()
        return name_str if name_str.lower() not in ('none', '') and len(name_str) >= 2 else None

    def _get_address(self, attrs: Dict[str, Any], dataset: Dict[str, Any]) -> Optional[str]:
        if not (address := attrs.get(dataset['address_field'])):
            return None
        address = str(address).strip()
        if dataset['name'] == 'Cemetery Survey' and (locale := attrs.get('Locale')):
            if (locale := str(locale).strip().lower()) not in ('', 'none'):
                address = f"{address}, {locale}"
        return address

    def _extract_coords(self, geom: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        try:
            if 'x' in geom and 'y' in geom:
                return self._transform_coords(float(geom['x']), float(geom['y']))
            if rings := geom.get('rings'):
                if ring := rings[0]:
                    x_coords = [float(p[0]) for p in ring if len(p) >= 2]
                    y_coords = [float(p[1]) for p in ring if len(p) >= 2]
                    if x_coords and y_coords:
                        return self._transform_coords(
                            sum(x_coords) / len(x_coords),
                            sum(y_coords) / len(y_coords)
                        )
            if paths := geom.get('paths'):
                if path := paths[0]:
                    mid_idx = len(path) // 2
                    if mid_idx < len(path) and len(path[mid_idx]) >= 2:
                        return self._transform_coords(
                            float(path[mid_idx][0]),
                            float(path[mid_idx][1])
                        )
        except (ValueError, TypeError, IndexError) as e:
            self.logger.warning(f"Coordinate extraction failed: {e}")
        return None, None

    def _transform_coords(self, x: float, y: float) -> Tuple[Optional[float], Optional[float]]:
        try:
            lng, lat = self.transformer.transform(x, y)
            if (self.VALID_LAT_RANGE[0] <= lat <= self.VALID_LAT_RANGE[1] and
                    self.VALID_LNG_RANGE[0] <= lng <= self.VALID_LNG_RANGE[1]):
                return lng, lat
            self.logger.warning(f"Coordinates out of range: {lat}, {lng}")
        except Exception as e:
            self.logger.warning(f"Transform failed: {e}")
        return None, None

    def _build_description(self, attrs: Dict[str, Any], dataset: Dict[str, Any]) -> str:
        """Build description from extra fields"""
        parts = [dataset['name']]
        for field in dataset['extra_fields']:
            if (value := attrs.get(field)) and str(value).strip().lower() != 'none':
                value_str = str(value).strip()
                parts.append(
                    f"{field}: {value_str[:100]}{'...' if len(value_str) > 100 else ''}")
        return ' | '.join(parts)

    def handle_error(self, failure):
        """Handle request errors"""
        dataset = failure.request.meta.get('dataset', {})
        self.logger.error(
            f"Request failed for {dataset.get('name', 'Unknown')}: {failure.value}")

    def closed(self, reason):
        """Log stats when spider closes"""
        self.logger.info(f"Spider closed: {reason}")
