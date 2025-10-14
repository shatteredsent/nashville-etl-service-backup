import os
import psycopg2
import subprocess
import sys
from flask import Flask, render_template_string, redirect, url_for, request
from datetime import datetime
from urllib.parse import urlencode
from celery import chain
from tasks import scrape_and_transform_chain
app=Flask(__name__)
PER_PAGE=25
def get_db_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])
def format_date_filter(iso_date_str):
    if not iso_date_str:return""
    try:
        dt_object=datetime.fromisoformat(iso_date_str.split('+')[0])
        return dt_object.strftime('%b %d, %Y at %I:%M %p')
    except (ValueError,TypeError):return iso_date_str
app.jinja_env.filters['format_date']=format_date_filter
@app.route('/')
def index():
    page=request.args.get('page',1,type=int)
    selected_source=request.args.get('source','')
    selected_category=request.args.get('category','')
    search_term=request.args.get('search','').strip()
    offset=(page-1)*PER_PAGE
    conn=None
    cursor=None
    events,sources,categories,total_pages,total_events=[],[],[],0,0
    try:
        conn=get_db_connection()
        cursor=conn.cursor()
        cursor.execute("SELECT to_regclass('public.events');")
        table_exists=cursor.fetchone()[0]
        conn.rollback()
        if not table_exists:
            pass
        else:
            cursor.execute("SELECT DISTINCT source FROM events WHERE source IS NOT NULL ORDER BY source")
            sources=[row[0]for row in cursor.fetchall()]
            cursor.execute("SELECT DISTINCT category FROM events WHERE category IS NOT NULL ORDER BY category")
            categories=[row[0]for row in cursor.fetchall()]
            conditions=[]
            params=[]
            if selected_source:
                conditions.append("source=%s")
                params.append(selected_source)
            if selected_category:
                conditions.append("category=%s")
                params.append(selected_category)
            if search_term:
                conditions.append("search_vector @@ plainto_tsquery('english',%s)")
                params.append(search_term)
            where_clause=f"WHERE {' AND '.join(conditions)}"if conditions else""
            count_query=f"SELECT COUNT(*) FROM events {where_clause}"
            cursor.execute(count_query,tuple(params))
            total_events=cursor.fetchone()[0]
            total_pages=(total_events+PER_PAGE-1)//PER_PAGE
            order_clause="ORDER BY ts_rank(search_vector,plainto_tsquery('english',%s)) DESC"if search_term else"ORDER BY event_date ASC, name ASC"
            final_query=f"SELECT * FROM events {where_clause} {order_clause} LIMIT %s OFFSET %s"
            final_params=list(params)
            if search_term:
                final_params.append(search_term)
            final_params.extend([PER_PAGE,offset])
            cursor.execute(final_query,tuple(final_params))
            colnames=[desc[0]for desc in cursor.description]
            events=[dict(zip(colnames,row))for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error querying events,sources,categories: {e}",file=sys.stderr)
    finally:
        if cursor:cursor.close()
        if conn:conn.close()
    current_filters={'source':selected_source,'category':selected_category,'search':search_term}
    html="""
<!DOCTYPE html>
<html>
<head>
    <title>Nashville ETL Dashboard</title>
    <style>
        body { font-family: sans-serif; margin: 2em; }
        table { border-collapse: collapse; width: 100%; margin-bottom: 20px; table-layout: fixed; }
        th, td { border: 1px solid #dddddd; text-align: left; padding: 8px; vertical-align: top; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        th:nth-child(1) { width: 30%; }
        th:nth-child(2) { width: 15%; }
        th:nth-child(3) { width: 15%; }
        th:nth-child(4) { width: 25%; }
        th:nth-child(5) { width: 10%; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        .controls-container { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; flex-wrap: wrap;}
        .filter-group { display: flex; gap: 10px; }
        .filter-group select, .filter-group input[type="text"], .filter-group button { padding: 8px; font-size: 16px; border: 1px solid #ccc; border-radius: 4px; }
        .action-button { background-color: #008CBA; color: white; padding: 10px 24px; text-align: center; font-size: 16px; cursor: pointer; border: none; margin-right: 10px; border-radius: 4px;}
        .clear-button { background-color: #f44336; }
        .pagination { margin-top: 10px; }
        .pagination a {
            color: black; float: left; padding: 8px 16px; text-decoration: none;
            transition: background-color .3s; border: 1px solid #ddd; margin: 0 4px; border-radius: 4px;
        }
        .pagination a.active { background-color: #008CBA; color: white; border: 1px solid #008CBA; }
        .pagination a:hover:not(.active) {background-color: #ddd;}
    </style>
</head>
<body>
    <h1>Nashville ETL Dashboard (Pre-Scheduled Data)</h1>
    <div class="controls-container">
        <form action="{{ url_for('index') }}" method="get" class="filter-group">            
            <select name="source">
                <option value="">All Sources</option>
                {% for source in sources %}
                    <option value="{{ source }}" {% if source == selected_source %}selected{% endif %}>{{ source }}</option>
                {% endfor %}
            </select>
            <select name="category">
                <option value="">All Categories</option>
                {% for category in categories %}
                    <option value="{{ category }}" {% if category == selected_category %}selected{% endif %}>{{ category }}</option>
                {% endfor %}
            </select>
            <button type="submit" class="action-button">Filter/Search</button>
            <a href="{{ url_for('index') }}" class="action-button clear-button" style="text-decoration: none;">Reset Filters</a>
        </form>
        <form action="/clear" method="post" style="display: inline-block;">
            <button class="action-button clear-button">CLEAR ALL DATA</button>
        </form>
        <form action="/launch_manual_scrape" method="post" style="display: inline-block;">
            <button class="action-button" style="background-color: green;">Manual Run</button>
        </form>
    </div>
    {% if total_events == 0 %}
        <p>No events found matching your criteria. Data refreshes automatically every 3 hours.</p>
    {% else %}
        <p>Displaying {{ events|length }} events on this page. Total matching events: **{{ total_events }}**</p>
        <table>
            <tr><th>Name</th><th>Date / Season</th><th>Venue</th><th>Address</th><th>Source</th></tr>
            {% for event in events %}
            <tr>
                <td><a href="{{ event.url }}" target="_blank">{{ event.name }}</a></td>
                <td>{% if event.event_date %}{{ event.event_date | format_date }}{% elif event.season %}{{ event.season }}{% else %}N/A{% endif %}</td>
                <td>{{ event.venue_name }}</td>
                <td>{{ event.venue_address }}</td>
                <td>{{ event.source }}</td>
            </tr>
            {% endfor %}
        </table>
        <div class="pagination">
            {% set page_args = {'source': selected_source, 'category': selected_category, 'search': search_term} %}
            {% if page > 1 %}<a href="{{ url_for('index', page=page-1, **page_args) }}">&laquo; Previous</a>{% endif %}
            {% for p in range(1, total_pages + 1) %}
                {% if p == page %}<a href="#" class="active">{{ p }}</a>{% else %}<a href="{{ url_for('index', page=p, **page_args) }}">{{ p }}</a>{% endif %}
            {% endfor %}
            {% if page < total_pages %}<a href="{{ url_for('index', page=page+1, **page_args) }}">Next &raquo;</a>{% endif %}
        </div>
    {% endif %}
</body>
</html>
"""
    return render_template_string(html,events=events,page=page,total_pages=total_pages,sources=sources,categories=categories,selected_source=selected_source,selected_category=selected_category,search_term=search_term)
@app.route('/clear',methods=['POST'])
def clear_data():
    conn=get_db_connection()
    cursor=conn.cursor()
    try:
        cursor.execute("TRUNCATE TABLE events, raw_data RESTART IDENTITY CASCADE;")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error clearing PostgreSQL database: {e}",file=sys.stderr)
    finally:
        cursor.close()
        conn.close()
    return redirect(url_for('index'))
@app.route('/launch_manual_scrape',methods=['POST'])
def launch_manual_scrape():
    clear_data()
    print("Dispatching ETL chain to Celery worker.")
    scrape_and_transform_chain.delay()
    return redirect(url_for('index'))
if __name__=="__main__":
    app.run(host='0.0.0.0',port=8000,debug=True)