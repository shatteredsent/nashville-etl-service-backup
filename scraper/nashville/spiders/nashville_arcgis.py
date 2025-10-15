import scrapy
from scrapy.http import FormRequest
from typing import Optional, Tuple, Dict, Any
from pyproj import Transformer
from scraper.nashville.items import BusinessItem
import os


class NashvilleArcGISSpider(scrapy.Spider):
    name = 'nashville_arcgis'
    allowed_domains = ['services2.arcgis.com']
    custom_settings = {
        'CONCURRENT_REQUESTS': int(os.getenv('ARCGIS_CONCURRENT', '8')),
        'DOWNLOAD_DELAY': float(os.getenv('ARCGIS_DELAY', '0.25')),
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
    }
    SOURCE_CRS = "EPSG:2274"
    TARGET_CRS = "EPSG:4326"
    RECORDS_PER_REQUEST = 1000
    VALID_LAT_RANGE = (35.0, 37.0)
    VALID_LNG_RANGE = (-88.0, -85.0)
    INVALID_STRINGS = frozenset(
        ['none', '', 'unknown', 'n/a', 'na', 'unnamed', 'null'])
    DATASETS = [
        {'name': 'Parks', 'url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Parks_Facilities/FeatureServer/1', 'category': 'park', 'name_field': 'FacilityName',
            'address_field': 'Address', 'extra_fields': ['FacilityType', 'Description', 'PhoneNumber', 'Website'], 'where': "FacilityType IS NOT NULL AND Address IS NOT NULL", 'enabled': True},
        {'name': 'Libraries', 'url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Library_Facilities/FeatureServer/0', 'category': 'public_facility', 'name_field': 'FacilityName',
            'address_field': 'Address', 'extra_fields': ['PhoneNumber', 'MondayOpen', 'MondayClose'], 'where': "FacilityName IS NOT NULL AND Address IS NOT NULL", 'enabled': True},
        {'name': 'Fire Stations', 'url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Fire_Station_Locations/FeatureServer/0', 'category': 'public_facility',
            'name_field': 'FacilityName', 'address_field': 'Address', 'extra_fields': [], 'where': "FacilityName IS NOT NULL AND Address IS NOT NULL", 'enabled': True},
        {'name': 'Police Precincts', 'url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Police_Precincts_view/FeatureServer/0', 'category': 'public_facility',
            'name_field': 'FacilityName', 'address_field': 'Address', 'extra_fields': ['CommanderName', 'PhoneNumber', 'Website'], 'where': "FacilityName IS NOT NULL AND Address IS NOT NULL", 'enabled': True},
        {'name': 'Public Health Clinics', 'url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Public_Health_Clinics_view/FeatureServer/0', 'category': 'public_facility',
            'name_field': 'ClinicName', 'address_field': 'Address', 'extra_fields': ['Phone', 'Hours'], 'where': "ClinicName IS NOT NULL AND Address IS NOT NULL", 'enabled': True},
        {'name': 'Public Artwork', 'url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Metro_Arts_Public_Artwork_view/FeatureServer/0', 'category': 'point_of_interest',
            'name_field': 'ArtworkName', 'address_field': 'Location', 'extra_fields': ['FirstName', 'LastName', 'Medium', 'WebLink'], 'where': "ArtworkName IS NOT NULL", 'enabled': True},
        {'name': 'Cemetery Survey', 'url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Davidson_County_Cemetery_Survey_Table_view/FeatureServer/0', 'category': 'point_of_interest',
            'name_field': 'Cemetery_Name', 'address_field': 'Street', 'extra_fields': ['Graveyard_Type', 'Known_Burials', 'Map_ID'], 'where': "Cemetery_Name IS NOT NULL", 'enabled': False},
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stats_counter = {'total': 0, 'yielded': 0,
                              'no_name': 0, 'no_coords': 0, 'out_of_range': 0}
        try:
            self.transformer = Transformer.from_crs(
                self.SOURCE_CRS, self.TARGET_CRS, always_xy=True)
        except Exception as e:
            self.logger.error(f"Failed to initialize transformer: {e}")
            raise
        self.logger.info(
            f"Initialized with {sum(1 for d in self.DATASETS if d.get('enabled', True))}/{len(self.DATASETS)} enabled datasets")

    def start_requests(self):
        for dataset in self.DATASETS:
            if not dataset.get('enabled', True):
                self.logger.info(f"Skipping disabled: {dataset['name']}")
                continue
            self.logger.info(
                f"Starting: {dataset['name']} ({dataset['category']})")
            yield self._create_request(dataset, 0)

    def _create_request(self, dataset: Dict[str, Any], offset: int):
        return FormRequest(url=f"{dataset['url']}/query", formdata={'where': dataset.get('where', '1=1'), 'outFields': ','.join([dataset['name_field'], dataset['address_field']] + dataset['extra_fields']), 'returnGeometry': 'true', 'f': 'json', 'resultOffset': str(offset), 'resultRecordCount': str(self.RECORDS_PER_REQUEST)}, callback=self.parse, meta={'dataset': dataset, 'offset': offset}, errback=self.handle_error, dont_filter=True)

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
        if not (features := data.get('features', [])):
            self.logger.info(f"Completed {dataset['name']} at offset {offset}")
            return
        self.logger.info(
            f"Processing {len(features)} features from {dataset['name']} (offset: {offset})")
        items_yielded = 0
        for feature in features:
            self.stats_counter['total'] += 1
            if item := self._parse_feature(feature, dataset):
                items_yielded += 1
                self.stats_counter['yielded'] += 1
                yield item
        self.logger.info(
            f"Yielded {items_yielded}/{len(features)} from {dataset['name']}")
        if len(features) >= self.RECORDS_PER_REQUEST:
            yield self._create_request(dataset, offset + self.RECORDS_PER_REQUEST)

    def _parse_feature(self, feature: Dict[str, Any], dataset: Dict[str, Any]) -> Optional[BusinessItem]:
        if 'attributes' not in feature or 'geometry' not in feature:
            self.logger.warning(
                f"Missing required keys in feature: {feature.keys()}")
            return None
        attrs, geom = feature['attributes'], feature['geometry']
        if not (name := self._get_valid_name(attrs.get(dataset['name_field']))):
            self.stats_counter['no_name'] += 1
            return None
        lng, lat = self._extract_coords(geom)
        if not (lat and lng):
            self.stats_counter['no_coords'] += 1
            self.logger.warning(f"Skipping {name} - no valid coordinates")
            return None
        return BusinessItem(source='nashville_arcgis', category=dataset['category'], venue_city='Nashville', name=name, venue_address=self._get_address(attrs, dataset), longitude=lng, latitude=lat, description=self._build_description(attrs, dataset), url=f"https://www.google.com/maps/search/?api=1&query={lat},{lng}")

    def _get_valid_name(self, name: Any) -> Optional[str]:
        if not name:
            return None
        name_str = str(name).strip()
        return name_str if name_str.lower() not in self.INVALID_STRINGS and len(name_str) >= 2 else None

    def _get_address(self, attrs: Dict[str, Any], dataset: Dict[str, Any]) -> Optional[str]:
        if not (address := attrs.get(dataset['address_field'])):
            return None
        address = str(address).strip()
        return address if address.lower() not in self.INVALID_STRINGS else None

    def _extract_coords(self, geom: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        try:
            if 'x' in geom and 'y' in geom:
                return self._transform_coords(float(geom['x']), float(geom['y']))
            if rings := geom.get('rings'):
                if ring := rings[0]:
                    x_coords = [float(p[0]) for p in ring if len(p) >= 2]
                    y_coords = [float(p[1]) for p in ring if len(p) >= 2]
                    if x_coords and y_coords:
                        return self._transform_coords(sum(x_coords) / len(x_coords), sum(y_coords) / len(y_coords))
            if paths := geom.get('paths'):
                if path := paths[0]:
                    mid_idx = len(path) // 2
                    if mid_idx < len(path) and len(path[mid_idx]) >= 2:
                        return self._transform_coords(float(path[mid_idx][0]), float(path[mid_idx][1]))
        except (ValueError, TypeError, IndexError) as e:
            self.logger.debug(f"Coordinate extraction failed: {e}")
        return None, None

    def _transform_coords(self, x: float, y: float) -> Tuple[Optional[float], Optional[float]]:
        try:
            lng, lat = self.transformer.transform(x, y)
            if self.VALID_LAT_RANGE[0] <= lat <= self.VALID_LAT_RANGE[1] and self.VALID_LNG_RANGE[0] <= lng <= self.VALID_LNG_RANGE[1]:
                return lng, lat
            self.stats_counter['out_of_range'] += 1
            self.logger.debug(f"Coordinates out of range: {lat}, {lng}")
        except Exception as e:
            self.logger.debug(f"Transform failed: {e}")
        return None, None

    def _build_description(self, attrs: Dict[str, Any], dataset: Dict[str, Any]) -> str:
        parts = [dataset['name']]
        for field in dataset['extra_fields']:
            if (value := attrs.get(field)) and str(value).strip().lower() not in self.INVALID_STRINGS:
                value_str = str(value).strip()
                parts.append(
                    f"{field}: {value_str[:100]}{'...' if len(value_str) > 100 else ''}")
        return ' | '.join(parts)

    def handle_error(self, failure):
        dataset = failure.request.meta.get('dataset', {})
        self.logger.error(
            f"Request failed for {dataset.get('name', 'Unknown')}: {failure.value}")

    def closed(self, reason):
        self.logger.info(f"Spider closed: {reason}")
        self.logger.info(f"Stats: {self.stats_counter}")
        if self.stats_counter['total'] > 0:
            yield_rate = (
                self.stats_counter['yielded'] / self.stats_counter['total']) * 100
            self.logger.info(f"Yield rate: {yield_rate:.1f}%")
