import os
import psycopg2
import json
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
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            spider.logger.error(f"Error saving raw item to database: {e}")
        return item

