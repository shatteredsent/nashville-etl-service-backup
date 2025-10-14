import os
import json
import psycopg2
from scraper.nashville.transform import transform_events
def run_transformations():
    print("transform started")
    conn=psycopg2.connect(os.environ['DATABASE_URL'])
    cursor=conn.cursor()
    cursor.execute("SELECT raw_json FROM raw_data")
    raw_results=cursor.fetchall()
    raw_events=[json.loads(row[0])for row in raw_results]
    print(f"transforming {len(raw_events)} raw items...")
    transformed_events=transform_events(raw_events)
    ts_vector_sql="to_tsvector('english', COALESCE(%s, '') || ' ' || COALESCE(%s, '') || ' ' || COALESCE(%s, '') || ' ' || COALESCE(%s, ''))"
    insert_query=f"""
        INSERT INTO events (name, url, event_date, venue_name, venue_address, description, source, category, genre, season, latitude, longitude, search_vector)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, {ts_vector_sql})
        ON CONFLICT (url) DO NOTHING
        """
    for event in transformed_events:
        text_for_search=(event.get('name'),event.get('venue_name'),event.get('venue_address'),event.get('description'))
        event_values=(event.get('name'),event.get('url'),event.get('event_date'),event.get('venue_name'),event.get('venue_address'),event.get('description'),event.get('source'),event.get('category'),event.get('genre'),event.get('season'),event.get('latitude'),event.get('longitude'))
        cursor.execute(insert_query,event_values+text_for_search)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"transform all done. {len(transformed_events)} items loaded to events table.")
if __name__=='__main__':
    run_transformations()