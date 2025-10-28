import os
import sys
import subprocess
import psycopg2
from celery import Celery, chain
from celery.schedules import crontab
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from transform_data import run_transformations

def get_db_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])
celery_app=Celery('tasks',broker='redis://redis:6379/0',backend='redis://redis:6379/0')
@celery_app.task
def run_all_spiders_task():
    print("--- Scrape and Cleanup ---")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("TRUNCATE TABLE events, raw_data RESTART IDENTITY CASCADE;")
        conn.commit()
        print("Database tables events and raw_data truncated successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error during database TRUNCATE: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

    project_dir='/app/scraper'
    scrapy_executable="scrapy"
    env=os.environ.copy()
    env['PYTHONPATH']='/app'

    try:
        result=subprocess.run([scrapy_executable,"list"],cwd=project_dir,capture_output=True,text=True,check=True,env=env)
        spider_names=result.stdout.strip().split('\n')
        print(f"Found spiders: {spider_names}")
    except subprocess.CalledProcessError as e:
        print("--- Could not find spiders. ---")
        print(f"--- STDERR: {e.stderr} ---")
        raise e
    except Exception as e:
        print(f"An unexpected error occurred while trying to list spiders: {e}")
        raise e

    for spider_name in spider_names:
        print(f"--- Running spider: {spider_name} ---")
        try:
            subprocess.run([scrapy_executable,"crawl",spider_name],cwd=project_dir,check=True,env=env)
        except Exception as e:
            print(f"--- Spider '{spider_name}' failed with an error: {e} ---")
            
    print("--- All scraping commands issued. ---")
    return "All spiders have finished."

@celery_app.task
def transform_data_task(previous_task_result):
    print(f"--- Transformation task starting (triggered by completion of previous task) ---")
    run_transformations()
    print("--- all done transforming. ---")
    return "Transformation complete."

@celery_app.task(name='tasks.scrape_and_transform_chain')
def scrape_and_transform_chain():
    workflow=chain(run_all_spiders_task.s(),transform_data_task.s())
    workflow.apply_async()

celery_app.conf.beat_schedule={'run-full-etl-every-3-hours':
    {'task':'tasks.scrape_and_transform_chain','schedule':
        crontab(minute=0,hour='*/3'),'args':()}}
celery_app.conf.timezone='UTC'