"""Test the transform layer with data from all scrapers."""
import json
import os
from pathlib import Path
from transform import transform_events


def load_scraped_data(filename):
    """Load scraped data if it exists."""
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            # Try loading as JSON array first
            try:
                f.seek(0)
                data = json.load(f)
                return data if isinstance(data, list) else [data]
            except json.JSONDecodeError:
                # If that fails, try JSON Lines format (one JSON object per line)
                f.seek(0)
                data = []
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            data.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
                return data if data else None
    return None


def test_source(source_name, data):
    """Test transformation for a specific source."""
    print(f"\n{'=' * 80}")
    print(f"TESTING: {source_name.upper()}")
    print(f"{'=' * 80}")

    if not data:
        print(f"⚠ No data found for {source_name}")
        return

    print(f"Found {len(data)} events")

    try:
        # Transform the data
        transformed = transform_events(data)

        # Show first event transformation
        if transformed:
            print(
                f"\n--- Sample Event: {transformed[0].get('name', 'Unknown')} ---")
            print(f"Original Date:  {data[0].get('event_date')}")
            print(f"Transformed:    {transformed[0].get('event_date')}")
            print(f"Category:       {transformed[0].get('category')}")
            print(f"Genre:          {transformed[0].get('genre')}")
            print(f"Venue:          {transformed[0].get('venue_name')}")

        # Save transformed output
        output_file = f'transformed_{source_name}.json'
        with open(output_file, 'w') as f:
            json.dump(transformed, f, indent=2)
        print(f"\n✓ Saved to {output_file}")

    except Exception as e:
        print(f"✗ Error transforming {source_name}: {e}")


def main():
    """Test all available scrapers."""
    print("=" * 80)
    print("TRANSFORM LAYER TEST - ALL SOURCES")
    print("=" * 80)

    # Define all possible data sources
    sources = {
        'underdog': 'underdog_events.json',
        'seatgeek': 'seatgeek_events.json',
        'ticketmaster': 'ticketmaster_events.json',
        'yelp': 'yelp_events.json',
    }

    # Test each source
    for source_name, filename in sources.items():
        data = load_scraped_data(filename)
        test_source(source_name, data)

    print(f"\n{'=' * 80}")
    print("TEST COMPLETE")
    print("=" * 80)
    print("\nTo generate test data, run:")
    print("  scrapy crawl underdog -o underdog_events.json")
    print("  scrapy crawl seatgeek -o seatgeek_events.json")
    print("  (etc.)")


if __name__ == '__main__':
    main()
