import os
import sqlite3
import subprocess
import sys
from flask import Flask, render_template_string, redirect, url_for, request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
app = Flask(__name__)
DB_FILE = "scraped_data.db"
PER_PAGE = 25


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
            source TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print("Database initialized.")


@app.route('/')
def index():
    page = request.args.get('page', 1, type=int)
    selected_source = request.args.get('source', '')
    offset = (page - 1) * PER_PAGE
    if not os.path.exists(DB_FILE):
        init_db()
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    sources_query = cursor.execute(
        "SELECT DISTINCT source FROM events ORDER BY source")
    sources = [row['source'] for row in sources_query.fetchall()]
    params = []
    where_clause = ""
    if selected_source:
        where_clause = "WHERE source = ?"
        params.append(selected_source)
    total_events_query = cursor.execute(
        f"SELECT COUNT(*) FROM events {where_clause}", params)
    total_events = total_events_query.fetchone()[0]
    total_pages = (total_events + PER_PAGE - 1) // PER_PAGE
    final_query = f"SELECT * FROM events {where_clause} ORDER BY event_date ASC LIMIT ? OFFSET ?"
    final_params = []
    if selected_source:
        final_params.append(selected_source)
    final_params.extend([PER_PAGE, offset])
    cursor.execute(final_query, final_params)
    events = cursor.fetchall()
    conn.close()
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Nashville Scraper</title>
        <style>
            body { font-family: sans-serif; margin: 2em; }
            table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }
            th, td { border: 1px solid #dddddd; text-align: left; padding: 8px; vertical-align: top; }
            tr:nth-child(even) { background-color: #f2f2f2; }
            .controls-container { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
            .action-button { color: white; padding: 10px 24px; text-align: center; font-size: 16px; cursor: pointer; border: none; margin-right: 10px;}
            .scrape-button { background-color: #008CBA; }
            .clear-button { background-color: #f44336; }
            .filter-form select, .filter-form button { padding: 8px; font-size: 16px; }
            .pagination { display: inline-block; }
            .pagination a {
                color: black;
                float: left;
                padding: 8px 16px;
                text-decoration: none;
                transition: background-color .3s;
                border: 1px solid #ddd;
                margin: 0 4px;
            }
            .pagination a.active {
                background-color: #008CBA;
                color: white;
                border: 1px solid #008CBA;
            }
            .pagination a:hover:not(.active) {background-color: #ddd;}
        </style>
    </head>
    <body>
        <h1>SPIDEY-SENSES TINGLING</h1>
        <div class="controls-container">
            <div>
                <form action="/scrape" method="post" style="display: inline-block;">
                    <button class="action-button scrape-button">RUN SUPER CREEPY STEALTHY ARACHNIDS </button>
                </form>
                <form action="/clear" method="post" style="display: inline-block;">
                    <button class="action-button clear-button">CLEAR DATA</button>
                </form>
            </div>
            <div class="filter-form">
                <form action="{{ url_for('index') }}" method="get">
                    <select name="source" onchange="this.form.submit()">
                        <option value="">All Sources</option>
                        {% for source in sources %}
                            <option value="{{ source }}" {% if source == selected_source %}selected{% endif %}>
                                {{ source }}
                            </option>
                        {% endfor %}
                    </select>
                </form>
            </div>
        </div>
        <table>
            <tr><th>Name</th><th>Date</th><th>Venue</th><th>Source</th></tr>
            {% for event in events %}
            <tr>
                <td><a href="{{ event.url }}" target="_blank">{{ event.name }}</a></td>
                <td>{{ event.event_date }}</td>
                <td>{{ event.venue_name }}</td>
                <td>{{ event.source }}</td>
            </tr>
            {% endfor %}
        </table>
        <div class="pagination">
            {% if page > 1 %}
                <a href="{{ url_for('index', page=page-1, source=selected_source) }}">&laquo; Previous</a>
            {% endif %}
            {% for p in range(1, total_pages + 1) %}
                {% if p == page %}
                    <a href="#" class="active">{{ p }}</a>
                {% else %}
                    <a href="{{ url_for('index', page=p, source=selected_source) }}">{{ p }}</a>
                {% endif %}
            {% endfor %}
            {% if page < total_pages %}
                <a href="{{ url_for('index', page=page+1, source=selected_source) }}">Next &raquo;</a>
            {% endif %}
        </div>
    </body>
    </html>
    """
    return render_template_string(html, events=events, page=page, total_pages=total_pages, sources=sources, selected_source=selected_source)


@app.route('/clear', methods=['POST'])
def clear_data():
    if os.path.exists(DB_FILE):
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM events")
        conn.commit()
        conn.close()
        print("Database cleared by user.")
    return redirect(url_for('index'))


@app.route('/scrape', methods=['POST'])
def scrape():
    runner_script = os.path.join(os.path.dirname(__file__), 'runner.py')
    python_executable = sys.executable
    print("--- GO! GO! GO! GO! RUNNER! YOU CAN DO IT! PUT YOUR SCRIPT IN TO IT!!!! ---")
    try:
        subprocess.run([python_executable, runner_script],
                       check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print("--- OH NO!!!! IT BROKE!!!! COMPLETE FAILURE!!! TIME TO GROW HAIR JUST TO PULL IT OUT!! ---")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
    print("--- OK RUNNER! THAT IS ALL. YOU DID GOOD. GO GET SOME BYTES AND RECHARGE!!! ---")
    return redirect(url_for('index'))


if __name__ == "__main__":
    init_db()
    app.run(host='0.0.0.0', port=8000, debug=True)
