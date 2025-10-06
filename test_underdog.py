import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'scraper'))
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from dotenv import load_dotenv

load_dotenv()
os.environ['SCRAPY_SETTINGS_MODULE'] = 'scraper.nashville.settings'
settings = get_project_settings()

process = CrawlerProcess(settings)
process.crawl('underdog')
process.start()
