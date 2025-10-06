import sqlite3
import os
class SQLitePipeline:
    def open_spider(self, spider):
        db_path = os.path.join(os.path.dirname(__file__), '..', '..', 'scraped_data.db')
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()
    def close_spider(self, spider):
        self.connection.close()
    def process_item(self, item, spider):
        self.cursor.execute(
            "INSERT OR IGNORE INTO events (name, url, event_date, venue_name, venue_address, description, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                item.get('name'),
                item.get('url'),
                item.get('event_date'),
                item.get('venue_name'),
                item.get('venue_address'),
                item.get('description'),
                item.get('source'),
            )
        )
        self.connection.commit()
        return item