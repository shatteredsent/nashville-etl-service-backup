"""
Alternative ETL runner that includes the Transform layer.
This is separate from runner.py to demonstrate the transform functionality.
Andrew can integrate this approach into the main runner.py if desired.
"""
import os
import sqlite3
import json
from transform import transform_events


def demo_etl_with_transform():
    """
    Demonstrate ETL with Transform layer using existing scraped data.
    Assumes you've already run: scrapy crawl underdog -o underdog_events.json
    """
    print("\n=== ETL WITH TRANSFORM DEMO ===\n")

    # 1. EXTRACT (already done - load existing raw data)
    print("1. EXTRACT: Loading raw scraped data...")
    raw_file = 'underdog_events.json'

    if not os.path.exists(raw_file):
        print(
            f"Error: {raw_file} not found. Run: scrapy crawl underdog -o {raw_file}")
        return

    with open(raw_file, 'r') as f:
        try:
            # Try loading as JSON array first
            raw_events = json.load(f)
            if not isinstance(raw_events, list):
                raw_events = [raw_events]
        except json.JSONDecodeError:
            # Fall back to JSON Lines
            f.seek(0)
            raw_events = []
            for line in f:
                line = line.strip()
                if line and line not in ['[', ']', ',']:
                    try:
                        raw_events.append(json.loads(line.rstrip(',')))
                    except json.JSONDecodeError:
                        continue

    print(f"   Loaded {len(raw_events)} raw events\n")

    # 2. TRANSFORM (new step)
    print("2. TRANSFORM: Standardizing and categorizing...")
    transformed_events = transform_events(raw_events)
    print(f"   Transformed {len(transformed_events)} events\n")

    # 3. LOAD (show what would be inserted)
    print("3. LOAD: Preview of transformed data:")
    for i, event in enumerate(transformed_events[:3], 1):
        print(f"\n   Event {i}:")
        print(f"      Name:     {event.get('name')}")
        print(f"      Date:     {event.get('event_date')}")
        print(f"      Category: {event.get('category')}")
        print(f"      Genre:    {event.get('genre')}")

    print(f"\n   ... and {len(transformed_events) - 3} more events")
    print("\n=== DEMO COMPLETE ===")
    print("\nTo integrate this into the main pipeline, discuss with Andrew.")


if __name__ == '__main__':
    demo_etl_with_transform()
