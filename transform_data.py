import os
import json
import psycopg2
from scraper.nashville.transform import transform_events
def run_transformations():
    print("transform started")
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cursor = conn.cursor()
    cursor.execute("SELECT raw_json FROM raw_data")
    raw_results = cursor.fetchall()
    raw_events = [json.loads(row[0]) for row in raw_results]
    print(f"transforming {len(raw_events)} raw items...")
    transformed_events = transform_events(raw_events)
    insert_query = """
        INSERT INTO events (name, url, event_date, venue_name, venue_address, description, source, category, genre, season, latitude, longitude )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (url) DO NOTHING
        """
    for event in transformed_events:
        cursor.execute(insert_query, (
            event.get('name'), event.get('url'), event.get('event_date'), event.get('venue_name'),
            event.get('venue_address'), event.get('description'), event.get('source'), event.get('category'),
            event.get('genre'), event.get('season'), event.get('latitude'), event.get('longitude')
        ))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"transform all done. {len(transformed_events)} items loaded to events table.")
if __name__ == '__main__':
    run_transformations()
