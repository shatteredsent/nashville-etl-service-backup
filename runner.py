import os
import sqlite3
import sys
from dotenv import load_dotenv
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

sys.path.append(os.path.join(os.path.dirname(__file__), "scraper"))
DB_FILE = "scraped_data.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            url TEXT UNIQUE,
            event_date TEXT,
            venue_name TEXT,
            venue_address TEXT,
            description TEXT,
            source TEXT,
            category TEXT,
            genre TEXT,
            season TEXT
        )
    ''')
    conn.commit()
    conn.close()

def run_all_spiders():
    load_dotenv()
    init_db()
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM events")
    conn.commit()
    conn.close()
    
    os.environ['SCRAPY_SETTINGS_MODULE'] = 'scraper.nashville.settings'
    settings = get_project_settings()
    settings.set('ITEM_PIPELINES', {'scraper.nashville.pipelines.SQLitePipeline': 1})
    
    process = CrawlerProcess(settings)
    all_spider_names = process.spider_loader.list()
    print(f"Found spiders: {', '.join(all_spider_names)}")
    
    for spider_name in all_spider_names:
        print(f"Queuing spider: {spider_name}")
        process.crawl(spider_name)
        
    process.start()
    print("--- All spiders finished ---")

if __name__ == '__main__':
    run_all_spiders()