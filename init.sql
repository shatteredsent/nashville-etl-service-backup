CREATE TABLE if NOT EXISTS raw_data (
    id SERIAL PRIMARY KEY,
    source_spider TEXT,
    raw_json TEXT
);
CREATE TABLE if NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    name TEXT,
    url TEXT UNIQUE,
    event_date TEXT,
    venue_name TEXT,
    venue_address TEXT,
    description TEXT,
    source TEXT,
    category TEXT,
    genre TEXT,
    season TEXT,
    latitude REAL,
    longitude REAL
);