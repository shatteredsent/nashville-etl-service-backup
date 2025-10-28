from typing import Dict, List
from .standardizer import standardize_date, standardize_venue_name, standardize_price
from .categorizer import categorize_event
def transform_event(raw_event: Dict) -> Dict:
    transformed = raw_event.copy()
    if 'event_date' in transformed:
        transformed['event_date'] = standardize_date(
            transformed['event_date'],
            source=transformed.get('source')
        )
    if 'venue_name' in transformed:
        transformed['venue_name'] = standardize_venue_name(
            transformed['venue_name']
        )
    if 'price' in transformed:
        transformed['price'] = standardize_price(transformed['price'])
    trusted_sources = {'nashville_arcgis', 'ticketmaster', 'yelp', 'google_places'}
    if transformed.get('source') not in trusted_sources or not transformed.get('category'):
        category, genre = categorize_event(
            transformed.get('name', ''),
            transformed.get('description', ''),
            transformed.get('venue_name', '')
        )
        transformed['category'] = category
        transformed['genre'] = genre
    return transformed
def transform_events(raw_events: List[Dict]) -> List[Dict]:
    return [transform_event(event) for event in raw_events]
