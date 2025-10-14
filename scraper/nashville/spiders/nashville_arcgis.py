"""
Production-Ready Nashville ArcGIS Spider

Scrapes public facility data from Nashville's ArcGIS REST API services.
Handles coordinate transformation from Tennessee State Plane to WGS84.

Author: Nashville ETL Team
Last Updated: 2025-10-14
"""

import scrapy
from scrapy.http import Request, FormRequest, Response
from typing import Dict, List, Optional, Tuple, Generator, Any
from pyproj import Transformer
from pyproj.exceptions import ProjError
from urllib.parse import urlencode
import structlog

from scraper.nashville.items import BusinessItem


logger = structlog.get_logger(__name__)


class ArcGISConfig:
    SOURCE_CRS = 2274  # Tennessee State Plane (EPSG:2274)
    TARGET_CRS = 4326  # WGS84 (EPSG:4326)
    RECORDS_PER_REQUEST = 1000
    MAX_RETRIES = 3
    MAX_DESCRIPTION_LENGTH = 100
    VALID_COORDINATE_RANGES = {
        'latitude': (35.0, 37.0),   # Nashville area latitude range
        'longitude': (-88.0, -85.0)  # Nashville area longitude range
    }
    MIN_NAME_LENGTH = 2
    SKIP_NULL_NAMES = True
    DATASETS = [
        {
            'name': 'Parks',
            'service_url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Parks_Facilities/FeatureServer/1',
            'category': 'park',
            'name_field': 'FacilityName',
            'address_field': 'Address',
            'required_fields': ['FacilityName', 'Address'],
            'extra_fields': ['FacilityType', 'Description', 'PhoneNumber', 'Website'],
        },
        {
            'name': 'Libraries',
            'service_url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Library_Facilities/FeatureServer/0',
            'category': 'public_facility',
            'name_field': 'FacilityName',
            'address_field': 'Address',
            'required_fields': ['FacilityName', 'Address'],
            'extra_fields': ['PhoneNumber', 'MondayOpen', 'MondayClose'],
        },
        {
            'name': 'Fire Stations',
            'service_url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Fire_Station_Locations/FeatureServer/0',
            'category': 'public_facility',
            'name_field': 'FacilityName',
            'address_field': 'Address',
            'required_fields': ['FacilityName'],
            'extra_fields': [],
        },
        {
            'name': 'Police Precincts',
            'service_url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Police_Precincts_view/FeatureServer/0',
            'category': 'public_facility',
            'name_field': 'FacilityName',
            'address_field': 'Address',
            'required_fields': ['FacilityName'],
            'extra_fields': ['CommanderName', 'PhoneNumber', 'Website'],
        },
        {
            'name': 'Public Health Clinics',
            'service_url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Public_Health_Clinics_view/FeatureServer/0',
            'category': 'public_facility',
            'name_field': 'ClinicName',
            'address_field': 'Address',
            'required_fields': ['ClinicName'],
            'extra_fields': ['Phone', 'Hours'],
        },
        {
            'name': 'Public Artwork',
            'service_url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Metro_Arts_Public_Artwork_view/FeatureServer/0',
            'category': 'point_of_interest',
            'name_field': 'ArtworkName',
            'address_field': 'Location',
            'required_fields': ['ArtworkName'],
            'extra_fields': ['FirstName', 'LastName', 'Medium', 'WebLink'],
        },
        {
            'name': 'Cemetery Survey',
            'service_url': 'https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Davidson_County_Cemetery_Survey_Table_view/FeatureServer/0',
            'category': 'point_of_interest',
            'name_field': 'Cemetery_Name',
            'address_field': 'Street',
            'required_fields': ['Cemetery_Name'],
            'extra_fields': ['Graveyard_Type', 'Known_Burials', 'Map_ID', 'Locale'],
        },
    ]


class CoordinateTransformer:
    def __init__(self, source_crs: int, target_crs: int):
        try:
            self.transformer = Transformer.from_crs(
                source_crs,
                target_crs,
                always_xy=True
            )
            logger.info(
                "coordinate_transformer_initialized",
                source_crs=source_crs,
                target_crs=target_crs
            )
        except ProjError as e:
            logger.error(
                "coordinate_transformer_init_failed",
                error=str(e),
                source_crs=source_crs,
                target_crs=target_crs
            )
            raise

    def transform(self, x: float, y: float) -> Tuple[Optional[float], Optional[float]]:
        try:
            lng, lat = self.transformer.transform(float(x), float(y))
            lat_range = ArcGISConfig.VALID_COORDINATE_RANGES['latitude']
            lng_range = ArcGISConfig.VALID_COORDINATE_RANGES['longitude']
            if not (lat_range[0] <= lat <= lat_range[1] and
                    lng_range[0] <= lng <= lng_range[1]):
                logger.warning(
                    "coordinates_out_of_range",
                    latitude=lat,
                    longitude=lng,
                    expected_lat_range=lat_range,
                    expected_lng_range=lng_range
                )
                return None, None
            return lng, lat
        except (ValueError, TypeError, ProjError) as e:
            logger.warning(
                "coordinate_transformation_failed",
                x=x,
                y=y,
                error=str(e),
                error_type=type(e).__name__
            )
            return None, None


class GeometryProcessor:
    @staticmethod
    def calculate_polygon_centroid(rings: List[List[List[float]]]) -> Tuple[Optional[float], Optional[float]]:
        if not rings or not rings[0]:
            return None, None
        try:
            ring = rings[0]
            x_coords = (float(point[0]) for point in ring if len(point) >= 2)
            y_coords = (float(point[1]) for point in ring if len(point) >= 2)
            x_list = list(x_coords)
            y_list = list(y_coords)
            if not x_list or not y_list:
                return None, None
            centroid_x = sum(x_list) / len(x_list)
            centroid_y = sum(y_list) / len(y_list)
            return centroid_x, centroid_y
        except (ValueError, TypeError, ZeroDivisionError) as e:
            logger.warning(
                "polygon_centroid_calculation_failed",
                error=str(e),
                error_type=type(e).__name__
            )
            return None, None

    @staticmethod
    def calculate_path_midpoint(paths: List[List[List[float]]]) -> Tuple[Optional[float], Optional[float]]:
        if not paths or not paths[0]:
            return None, None
        try:
            path = paths[0]
            mid_idx = len(path) // 2
            if mid_idx >= len(path) or len(path[mid_idx]) < 2:
                return None, None
            return float(path[mid_idx][0]), float(path[mid_idx][1])
        except (ValueError, TypeError, IndexError) as e:
            logger.warning(
                "path_midpoint_calculation_failed",
                error=str(e),
                error_type=type(e).__name__
            )
            return None, None


class NashvilleArcGISSpider(scrapy.Spider):
    name = 'nashville_arcgis'
    allowed_domains = ['services2.arcgis.com']
    custom_settings = {
        'CONCURRENT_REQUESTS': 8,
        'DOWNLOAD_DELAY': 0.25,
        'RETRY_TIMES': ArcGISConfig.MAX_RETRIES,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.coord_transformer = CoordinateTransformer(
            ArcGISConfig.SOURCE_CRS,
            ArcGISConfig.TARGET_CRS
        )
        self.geom_processor = GeometryProcessor()
        self.stats = {
            'total_items': 0,
            'failed_transforms': 0,
            'skipped_no_name': 0,
            'skipped_validation': 0
        }
        logger.info(
            "spider_initialized",
            spider_name=self.name,
            datasets_count=len(ArcGISConfig.DATASETS)
        )

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        """Generate initial requests for all configured datasets"""
        for dataset in ArcGISConfig.DATASETS:
            logger.info(
                "starting_dataset_scrape",
                dataset_name=dataset['name'],
                category=dataset['category']
            )
            yield self._create_query_request(dataset, offset=0)

    def _create_query_request(
        self,
        dataset: Dict[str, Any],
        offset: int = 0
    ) -> scrapy.Request:
        out_fields = [
            dataset['name_field'],
            dataset['address_field'],
        ] + dataset.get('extra_fields', [])
        params = {
            'where': '1=1',
            'outFields': ','.join(out_fields),
            'returnGeometry': 'true',
            'f': 'json',
            'resultOffset': offset,
            'resultRecordCount': ArcGISConfig.RECORDS_PER_REQUEST
        }
        url = f"{dataset['service_url']}/query?{urlencode(params)}"
        return scrapy.Request(
            url=url,
            callback=self.parse,
            meta={
                'dataset': dataset,
                'offset': offset,
                'retry_count': 0
            },
            errback=self.handle_error,
            dont_filter=True
        )

    def parse(self, response: Response) -> Generator[BusinessItem, None, None]:
        dataset = response.meta['dataset']
        offset = response.meta['offset']
        try:
            data = response.json()
        except ValueError as e:
            logger.error(
                "json_parse_error",
                dataset_name=dataset['name'],
                offset=offset,
                error=str(e),
                response_status=response.status
            )
            return
        if 'error' in data:
            error_info = data['error']
            logger.error(
                "api_error_response",
                dataset_name=dataset['name'],
                error_code=error_info.get('code'),
                error_message=error_info.get('message'),
                error_details=error_info.get('details')
            )
            return
        if 'features' not in data:
            logger.warning(
                "missing_features_field",
                dataset_name=dataset['name'],
                response_keys=list(data.keys())
            )
            return
        features = data['features']
        features_count = len(features)
        if features_count == 0:
            logger.info(
                "no_more_features",
                dataset_name=dataset['name'],
                offset=offset
            )
            return
        logger.info(
            "processing_features",
            dataset_name=dataset['name'],
            offset=offset,
            count=features_count
        )
        items_yielded = 0
        for feature in features:
            item = self._parse_feature(feature, dataset)
            if item:
                items_yielded += 1
                self.stats['total_items'] += 1
                yield item
        logger.info(
            "features_processed",
            dataset_name=dataset['name'],
            offset=offset,
            items_yielded=items_yielded,
            total_features=features_count
        )
        if features_count >= ArcGISConfig.RECORDS_PER_REQUEST:
            next_offset = offset + ArcGISConfig.RECORDS_PER_REQUEST
            logger.info(
                "fetching_next_page",
                dataset_name=dataset['name'],
                next_offset=next_offset
            )
            yield self._create_query_request(dataset, offset=next_offset)
        else:
            total_records = offset + features_count
            logger.info(
                "dataset_complete",
                dataset_name=dataset['name'],
                total_records=total_records
            )

    def _parse_feature(
        self,
        feature: Dict[str, Any],
        dataset: Dict[str, Any]
    ) -> Optional[BusinessItem]:
        attrs = feature.get('attributes', {})
        geom = feature.get('geometry', {})
        name_field = dataset['name_field']
        name = attrs.get(name_field)
        if not self._validate_name(name, dataset['name']):
            self.stats['skipped_no_name'] += 1
            return None
        item = BusinessItem()
        item['source'] = 'nashville_arcgis'
        item['category'] = dataset['category']
        item['venue_city'] = 'Nashville'
        item['name'] = str(name).strip()
        address_field = dataset['address_field']
        address = attrs.get(address_field)
        if address:
            item['venue_address'] = str(address).strip()
            if dataset['name'] == 'Cemetery Survey' and attrs.get('Locale'):
                locale = str(attrs['Locale']).strip()
                if locale and locale.lower() != 'none':
                    item['venue_address'] = f"{item['venue_address']}, {locale}"
        else:
            item['venue_address'] = None
        lng, lat = self._extract_coordinates(geom, item['name'])
        item['longitude'] = lng
        item['latitude'] = lat
        item['description'] = self._build_description(attrs, dataset)
        if lat and lng:
            item['url'] = f"https://www.google.com/maps/search/?api=1&query={lat},{lng}"
        else:
            item['url'] = None
        if not self._validate_item(item):
            self.stats['skipped_validation'] += 1
            return None
        return item

    def _validate_name(self, name: Any, dataset_name: str) -> bool:
        if not name:
            logger.debug(
                "skipping_feature_no_name",
                dataset_name=dataset_name
            )
            return False
        name_str = str(name).strip().lower()
        if name_str == 'none' or len(name_str) < ArcGISConfig.MIN_NAME_LENGTH:
            logger.debug(
                "skipping_feature_invalid_name",
                dataset_name=dataset_name,
                name=name_str
            )
            return False
        return True

    def _extract_coordinates(
        self,
        geom: Dict[str, Any],
        name: str
    ) -> Tuple[Optional[float], Optional[float]]:
        if 'x' in geom and 'y' in geom:
            lng, lat = self.coord_transformer.transform(geom['x'], geom['y'])
            if lng is None:
                self.stats['failed_transforms'] += 1
            return lng, lat
        elif 'rings' in geom and geom['rings']:
            x_centroid, y_centroid = self.geom_processor.calculate_polygon_centroid(
                geom['rings']
            )
            if x_centroid and y_centroid:
                lng, lat = self.coord_transformer.transform(
                    x_centroid, y_centroid)
                if lng is None:
                    self.stats['failed_transforms'] += 1
                return lng, lat
        elif 'paths' in geom and geom['paths']:
            x_mid, y_mid = self.geom_processor.calculate_path_midpoint(
                geom['paths']
            )
            if x_mid and y_mid:
                lng, lat = self.coord_transformer.transform(x_mid, y_mid)
                if lng is None:
                    self.stats['failed_transforms'] += 1
                return lng, lat
        logger.debug(
            "no_valid_geometry",
            feature_name=name,
            geometry_keys=list(geom.keys())
        )
        return None, None

    def _build_description(
        self,
        attrs: Dict[str, Any],
        dataset: Dict[str, Any]
    ) -> str:
        description_parts = [dataset['name']]
        for field in dataset.get('extra_fields', []):
            value = attrs.get(field)
            if value and str(value).strip().lower() != 'none':
                value_str = str(value).strip()
                if len(value_str) > ArcGISConfig.MAX_DESCRIPTION_LENGTH:
                    value_str = value_str[:ArcGISConfig.MAX_DESCRIPTION_LENGTH] + '...'
                description_parts.append(f"{field}: {value_str}")
        return ' | '.join(description_parts)

    def _validate_item(self, item: BusinessItem) -> bool:
        if not item.get('name'):
            return False
        has_address = bool(item.get('venue_address'))
        has_coords = bool(item.get('latitude') and item.get('longitude'))

        if not (has_address or has_coords):
            logger.debug(
                "item_missing_location_data",
                name=item.get('name')
            )
            return False
        return True

    def handle_error(self, failure):
        dataset = failure.request.meta.get('dataset', {})
        dataset_name = dataset.get('name', 'Unknown')
        offset = failure.request.meta.get('offset', 0)
        logger.error(
            "request_failed",
            dataset_name=dataset_name,
            offset=offset,
            url=failure.request.url,
            error_type=failure.type.__name__,
            error_value=str(failure.value)
        )

    def closed(self, reason):
        logger.info(
            "spider_closed",
            spider_name=self.name,
            reason=reason,
            total_items_scraped=self.stats['total_items'],
            failed_coordinate_transforms=self.stats['failed_transforms'],
            skipped_no_name=self.stats['skipped_no_name'],
            skipped_validation=self.stats['skipped_validation']
        )
