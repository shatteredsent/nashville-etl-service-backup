import os
import json
import psycopg2
from datetime import datetime
def get_db_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])
def transform_arcgis_data(raw_item: dict) -> dict:
    raw_data = json.loads(raw_item['raw_json'])
    category = raw_data.get('category', 'Civic Facility')
    try:
        latitude = float(raw_data.get('latitude')) if raw_data.get('latitude') else None
        longitude = float(raw_data.get('longitude')) if raw_data.get('longitude') else None
    except (ValueError, TypeError):
        latitude = None
        longitude = None
        print(f"WARNING: Skipped bad coordinates for ArcGIS item: {raw_data.get('name')}")
    clean_item = {
        'source': 'Nashville ArcGIS',
        'name': raw_data.get('name'),
        'venue_name': raw_data.get('name'),
        'venue_address': raw_data.get('venue_address'),
        'venue_city': raw_data.get('venue_city', 'Nashville'),
        'description': raw_data.get('description'),
        'url': raw_data.get('url'),
        'category': category.replace('_', ' ').title(),
        'latitude': latitude,
        'longitude': longitude,
        'event_date': None,
        'season': None,
        'genre': None,
    }
    if not clean_item['name'] or not clean_item['venue_name']:
        return None
    return clean_item
def transform_ticketmaster_data(raw_item: dict) -> dict:
    raw_data = json.loads(raw_item['raw_json'])
    event_date = raw_data.get('event_date')
    is_business = 'venue_city' not in raw_data 
    clean_item = {
        'source': 'Ticketmaster',
        'name': raw_data.get('name'),
        'url': raw_data.get('url'),
        'venue_name': raw_data.get('venue_name'),
        'venue_address': raw_data.get('venue_address'),
        'venue_city': raw_data.get('venue_city'),
        'description': raw_data.get('description'),
        'event_date': event_date,
        'category': raw_data.get('category', 'Event').title(),
        'genre': raw_data.get('genre'),
        'season': raw_data.get('season'),
        'latitude': float(raw_data.get('latitude')) if raw_data.get('latitude') else None,
        'longitude': float(raw_data.get('longitude')) if raw_data.get('longitude') else None,
    }
    if not clean_item['name'] or not clean_item['venue_name']:
        return None
    return clean_item
def transform_yelp_data(raw_item: dict) -> dict:
    raw_data = json.loads(raw_item['raw_json'])
    clean_item = {
        'source': 'Yelp',
        'name': raw_data.get('name'),
        'venue_name': raw_data.get('name'),
        'venue_address': raw_data.get('venue_address'),
        'venue_city': raw_data.get('venue_city', 'Nashville'),
        'description': raw_data.get('description'),
        'url': raw_data.get('url'),
        'category': raw_data.get('category', 'Business').title(),
        'latitude': float(raw_data.get('latitude')) if raw_data.get('latitude') else None,
        'longitude': float(raw_data.get('longitude')) if raw_data.get('longitude') else None,
        'event_date': None,
        'season': None,
        'genre': None,
    }
    if not clean_item['name']:
        return None
    return clean_item
def transform_google_data(raw_item: dict) -> dict:
    raw_data = json.loads(raw_item['raw_json'])
    clean_item = {
        'source': 'Google Places',
        'name': raw_data.get('name'),
        'venue_name': raw_data.get('name'),
        'venue_address': raw_data.get('venue_address'),
        'venue_city': raw_data.get('venue_city', 'Nashville'),
        'description': raw_data.get('description'),
        'url': raw_data.get('url'),
        'category': raw_data.get('category', 'Attraction').title(),
        'latitude': float(raw_data.get('latitude')) if raw_data.get('latitude') else None,
        'longitude': float(raw_data.get('longitude')) if raw_data.get('longitude') else None,
        'event_date': None,
        'season': None,
        'genre': None,
    }
    if not clean_item['name']:
        return None
    return clean_item
def transform_generic_data(raw_item: dict) -> dict:
    raw_data = json.loads(raw_item['raw_json'])
    source_map = {
        'nashville.com-events': 'Nashville Events',
        'nashville.com-hotels': 'Nashville Hotels',
        'underdog': 'Underdog Venue',
    }
    display_source = source_map.get(raw_item['source_spider'], raw_item['source_spider'])
    
    clean_item = {
        'source': display_source,
        'name': raw_data.get('name'),
        'venue_name': raw_data.get('venue_name'),
        'venue_address': raw_data.get('venue_address'),
        'venue_city': raw_data.get('venue_city', 'Nashville'),
        'description': raw_data.get('description'),
        'url': raw_data.get('url'),
        'category': raw_data.get('category', 'General').title(),
        'event_date': raw_data.get('event_date'),
        'latitude': float(raw_data.get('latitude')) if raw_data.get('latitude') else None,
        'longitude': float(raw_data.get('longitude')) if raw_data.get('longitude') else None,
        'season': raw_data.get('season'),
        'genre': raw_data.get('genre'),
    }
    if not clean_item['name']:
        return None
    return clean_item

def run_transformations():
    print("transform started")
    conn=get_db_connection()
    cursor=conn.cursor()
    cursor.execute("SELECT raw_json, source_spider FROM raw_data")
    raw_results=cursor.fetchall()
    transformed_events=[] 
    for row in raw_results:
        raw_json_str, source_spider = row
        raw_item = {'raw_json': raw_json_str, 'source_spider': source_spider}
        transformed = None
        try: 
            if source_spider == 'nashville_arcgis':
                transformed = transform_arcgis_data(raw_item)
            elif source_spider == 'ticketmaster':
                transformed = transform_ticketmaster_data(raw_item)
            elif source_spider == 'yelp':
                transformed = transform_yelp_data(raw_item)
            elif source_spider == 'google_places':
                transformed = transform_google_data(raw_item)
            elif source_spider == 'generic':
                transformed = transform_generic_data(raw_item)
        except Exception as e:
            print(f"CRITICAL ERROR: Failed to process item from {source_spider}. Error: {str(e)}")
            continue 
        if transformed:
            transformed_events.append(transformed)
    print(f"transforming {len(raw_results)} raw items...") 
    ts_vector_sql="to_tsvector('english', COALESCE(%s, '') || ' ' || COALESCE(%s, '') || ' ' || COALESCE(%s, '') || ' ' || COALESCE(%s, ''))"
    insert_query=f"""
        INSERT INTO events (name, url, event_date, venue_name, venue_address, description, source, category, genre, season, latitude, longitude, search_vector)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, {ts_vector_sql})
        ON CONFLICT (url) DO NOTHING
        """ 
    records_to_insert = [] 
    for event in transformed_events:
        text_for_search=(event.get('name'),event.get('venue_name'),event.get('venue_address'),event.get('description'))
        event_values=(event.get('name'),event.get('url'),event.get('event_date'),event.get('venue_name'),event.get('venue_address'),event.get('description'),event.get('source'),event.get('category'),event.get('genre'),event.get('season'),event.get('latitude'),event.get('longitude'))
        records_to_insert.append(event_values + text_for_search) 
    for record in records_to_insert:
        cursor.execute(insert_query, record)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"transform all done. {len(records_to_insert)} items loaded to events table.")
if __name__=='__main__':
    run_transformations()
