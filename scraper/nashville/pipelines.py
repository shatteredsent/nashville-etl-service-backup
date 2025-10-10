import os
import psycopg2
import json
from scraper.nashville.transform import transform_event
class PostgresPipeline:
    def open_spider(self, spider):
        self.connection = psycopg2.connect(os.environ['DATABASE_URL'])
        self.cursor = self.connection.cursor()
    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()
    def process_item(self, item, spider):
        try:
            self.cursor.execute(
                "INSERT INTO raw_data (source_spider, raw_json) VALUES (%s, %s)",
                (spider.name, json.dumps(dict(item)))
            )
            transformed_item = transform_event(dict(item))
            self.cursor.execute(
                """
                INSERT INTO events (name, url, event_date, venue_name, venue_address, description, source, category, genre, season, latitude, longitude) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
                """,
                (
                    transformed_item.get('name'),
                    transformed_item.get('url'),
                    transformed_item.get('event_date'),
                    transformed_item.get('venue_name'),
                    transformed_item.get('venue_address'),
                    transformed_item.get('description'),
                    transformed_item.get('source'),
                    transformed_item.get('category'),
                    transformed_item.get('genre'),
                    transformed_item.get('season'),
                    transformed_item.get('latitude'),
                    transformed_item.get('longitude')
                )
            )
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            spider.logger.error(f"Error in pipeline for item from {spider.name}: {e}")
        
        return item