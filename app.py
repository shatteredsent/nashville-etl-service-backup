import os
import psycopg2
import subprocess
import sys
from flask import Flask, render_template_string, redirect, url_for, request
from datetime import datetime

app = Flask(__name__)
PER_PAGE = 25

def get_db_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])

def format_date_filter(iso_date_str):
    if not iso_date_str: return ""
    try:
        dt_object = datetime.fromisoformat(iso_date_str)
        return dt_object.strftime('%b %d, %Y at %I:%M %p')
    except (ValueError, TypeError): return iso_date_str
app.jinja_env.filters['format_date'] = format_date_filter

@app.route('/')
def index():
    page = request.args.get('page', 1, type=int)
    selected_source = request.args.get('source', '')
    offset = (page - 1) * PER_PAGE
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT to_regclass('public.events');")
    table_exists = cursor.fetchone()[0]
    conn.rollback()

    if not table_exists:
        events, sources, total_pages = [], [], 0
    else:
        cursor.execute("SELECT DISTINCT source FROM events ORDER BY source")
        sources = [row[0] for row in cursor.fetchall()]
        params = []
        where_clause = ""
        if selected_source:
            where_clause = "WHERE source = %s"
            params.append(selected_source)
        
        cursor.execute(f"SELECT COUNT(*) FROM events {where_clause}", params)
        total_events = cursor.fetchone()[0]
        total_pages = (total_events + PER_PAGE - 1) // PER_PAGE
        
        final_query = f"SELECT * FROM events {where_clause} ORDER BY event_date ASC, name ASC LIMIT %s OFFSET %s"
        final_params = params + [PER_PAGE, offset]
        cursor.execute(final_query, final_params)
        
        colnames = [desc[0] for desc in cursor.description]
        events = [dict(zip(colnames, row)) for row in cursor.fetchall()]
    
    cursor.close()
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
                color: black; float: left; padding: 8px 16px; text-decoration: none;
                transition: background-color .3s; border: 1px solid #ddd; margin: 0 4px;
            }
            .pagination a.active { background-color: #008CBA; color: white; border: 1px solid #008CBA; }
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
                            <option value="{{ source }}" {% if source == selected_source %}selected{% endif %}>{{ source }}</option>
                        {% endfor %}
                    </select>
                </form>
            </div>
        </div>
        <table>
            <tr><th>Name</th><th>Date / Season</th><th>Venue</th><th>Address</th><th>Source</th></tr>
            {% for event in events %}
            <tr>
                <td><a href="{{ event.url }}" target="_blank">{{ event.name }}</a></td>
                <td>
                    {% if event.event_date %}{{ event.event_date | format_date }}{% elif event.season %}{{ event.season }}{% else %}N/A{% endif %}
                </td>
                <td>{{ event.venue_name }}</td>
                <td>{{ event.venue_address }}</td>
                <td>{{ event.source }}</td>
            </tr>
            {% endfor %}
        </table>
        <div class="pagination">
            {% if page > 1 %}<a href="{{ url_for('index', page=page-1, source=selected_source) }}">&laquo; Previous</a>{% endif %}
            {% for p in range(1, total_pages + 1) %}
                {% if p == page %}<a href="#" class="active">{{ p }}</a>{% else %}<a href="{{ url_for('index', page=p, source=selected_source) }}">{{ p }}</a>{% endif %}
            {% endfor %}
            {% if page < total_pages %}<a href="{{ url_for('index', page=page+1, source=selected_source) }}">Next &raquo;</a>{% endif %}
        </div>
    </body>
    </html>
    """
    return render_template_string(html, events=events, page=page, total_pages=total_pages, sources=sources, selected_source=selected_source)

@app.route('/clear', methods=['POST'])
def clear_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE events, raw_data RESTART IDENTITY;")
    conn.commit()
    cursor.close()
    conn.close()
    return redirect(url_for('index'))

@app.route('/scrape', methods=['POST'])
def scrape():
    from tasks import run_all_spiders_task, transform_data_task
    from celery import chain
    
    clear_data()
    print("Dispatching ETL chain to Celery worker.")
    task_chain = chain(run_all_spiders_task.s(), transform_data_task.s())
    task_chain.delay()
    return redirect(url_for('index'))

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000, debug=True)