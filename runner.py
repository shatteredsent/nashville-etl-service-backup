import os
import sqlite3
import sys
import subprocess
from dotenv import load_dotenv

load_dotenv()
DB_FILE = "scraped_data.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, url TEXT UNIQUE, event_date TEXT,
            venue_name TEXT, venue_address TEXT, description TEXT, source TEXT,
            category TEXT, genre TEXT, season TEXT, latitude REAL, longitude REAL
        )
    ''')
    conn.commit()
    conn.close()

def run_all_spiders():
    init_db()
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM events")
    conn.commit()
    conn.close()
    
    scrapy_executable = "scrapy"
    project_dir = os.path.join(os.path.dirname(__file__), 'scraper')
    
    env = os.environ.copy()
    env['PYTHONPATH'] = os.path.dirname(__file__)

    try:
        result = subprocess.run(
            [scrapy_executable, "list"],
            cwd=project_dir, capture_output=True, text=True, check=True, env=env
        )
        spider_names = result.stdout.strip().split('\n')
        print(f"Found spiders: {spider_names}")
    except subprocess.CalledProcessError as e:
        print("--- Could not find spiders. The 'scrapy list' command failed. ---")
        print(f"--- STDERR from scrapy list: {e.stderr} ---")
        return
    except Exception as e:
        print(f"An unexpected error occurred while trying to list spiders: {e}")
        return

    for spider_name in spider_names:
        print(f"--- Running spider: {spider_name} ---")
        try:
            subprocess.run(
                [scrapy_executable, "crawl", spider_name],
                cwd=project_dir, check=True, env=env
            )
        except Exception as e:
            print(f"--- Spider '{spider_name}' failed with an error: {e} ---")
            
    print("--- All spiders finished ---")

if __name__ == '__main__':
    run_all_spiders()