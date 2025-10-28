import sys
import os
import subprocess
from flask import Flask, render_template_string, redirect, url_for, request, flash
from datetime import datetime
from werkzeug.utils import secure_filename
from tasks import scrape_and_transform_chain
from db_extractor import PostgresExtractor

app = Flask(__name__)
app.secret_key = os.environ.get(
    'SECRET_KEY', 'dev-secret-key-change-in-production')
PER_PAGE = 25
db_manager = PostgresExtractor()

UPLOAD_FOLDER = '/tmp/pdf_uploads'
ALLOWED_EXTENSIONS = {'pdf'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


def format_date_filter(iso_date_str):
    if not iso_date_str:
        return ""
    try:
        dt_object = datetime.fromisoformat(iso_date_str.split('+')[0])
        return dt_object.strftime('%b %d, %Y at %I:%M %p')
    except (ValueError, TypeError):
        return iso_date_str


def get_pagination_range(current_page, total_pages, max_visible=5):
    if total_pages <= max_visible + 2:
        return {
            'show_first': False,
            'show_last': False,
            'show_left_ellipsis': False,
            'show_right_ellipsis': False,
            'pages': list(range(1, total_pages + 1))
        }
    if current_page <= 3:
        return {
            'show_first': False,
            'show_last': True,
            'show_left_ellipsis': False,
            'show_right_ellipsis': True,
            'pages': list(range(1, min(6, total_pages)))
        }
    elif current_page >= total_pages - 2:
        return {
            'show_first': True,
            'show_last': False,
            'show_left_ellipsis': True,
            'show_right_ellipsis': False,
            'pages': list(range(max(total_pages - 4, 2), total_pages + 1))
        }
    else:
        return {
            'show_first': True,
            'show_last': True,
            'show_left_ellipsis': True,
            'show_right_ellipsis': True,
            'pages': [current_page - 1, current_page, current_page + 1]
        }


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


app.jinja_env.filters['format_date'] = format_date_filter


@app.route('/')
def index():
    page = request.args.get('page', 1, type=int)
    selected_source = request.args.get('source', '')
    selected_category = request.args.get('category', '')
    search_term = request.args.get('search', '').strip()

    events, sources, categories, total_pages, total_events = db_manager.fetch_paginated_data(
        page, selected_source, selected_category, search_term
    )
    pagination = get_pagination_range(page, total_pages)
    current_filters = {
        'source': selected_source,
        'category': selected_category,
        'search': search_term
    }
    html = """
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
        .upload-button { background-color: #9C27B0; }
        .pagination { margin-top: 10px; display: flex; gap: 5px; align-items: center; }
        .pagination a, .pagination span {
            color: black;
            padding: 8px 16px;
            text-decoration: none;
            transition: background-color .3s;
            border: 1px solid #ddd;
            border-radius: 4px;
            display: inline-block;
        }
        .pagination a.active { background-color: #008CBA; color: white; border: 1px solid #008CBA; }
        .pagination a:hover:not(.active) { background-color: #ddd; }
        .pagination span.disabled {
            color: #ccc;
            cursor: not-allowed;
            background-color: #f5f5f5;
        }
        .pagination .ellipsis {
            border: none;
            background: none;
            padding: 8px 4px;
        }
        .upload-form {
            display: inline-block;
            margin-right: 10px;
        }
        .upload-form input[type="file"] {
            display: none;
        }
        .upload-label {
            background-color: #9C27B0;
            color: white;
            padding: 10px 24px;
            text-align: center;
            font-size: 16px;
            cursor: pointer;
            border: none;
            border-radius: 4px;
            display: inline-block;
        }
        .upload-label:hover {
            background-color: #7B1FA2;
        }
        .flash-messages {
            margin-bottom: 20px;
        }
        .flash-message {
            padding: 10px;
            margin-bottom: 10px;
            border-radius: 4px;
        }
        .flash-success {
            background-color: #4CAF50;
            color: white;
        }
        .flash-error {
            background-color: #f44336;
            color: white;
        }
    </style>
</head>
<body>
    <h1>Nashville ETL Dashboard (Pre-Scheduled Data)</h1>
    
    {% with messages = get_flashed_messages(with_categories=true) %}
        {% if messages %}
            <div class="flash-messages">
                {% for category, message in messages %}
                    <div class="flash-message flash-{{ category }}">{{ message }}</div>
                {% endfor %}
            </div>
        {% endif %}
    {% endwith %}
    
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
        
        <div style="display: flex; gap: 10px;">
            <form action="/upload_pdf" method="post" enctype="multipart/form-data" id="upload-form" class="upload-form">
                <input type="file" id="pdf-upload" name="pdf_file" accept=".pdf" style="display: none;">
                <label for="pdf-upload" class="upload-label">Upload Document</label>
            </form>
            <script>
                let isSubmitting = false;
                document.getElementById('pdf-upload').addEventListener('change', function() {
                    if (this.files.length > 0 && !isSubmitting) {
                        isSubmitting = true;
                        console.log('Submitting PDF:', this.files[0].name);
                        document.getElementById('upload-form').submit();
                    }
                });
            </script>
            
            <form action="/clear" method="post" style="display: inline-block;">
                <button class="action-button clear-button">CLEAR ALL DATA</button>
            </form>
            <form action="/launch_manual_scrape" method="post" style="display: inline-block;">
                <button class="action-button" style="background-color: green;">Manual Run</button>
            </form>
        </div>
    </div>
    {% if total_events == 0 %}
        <p>No events found matching your criteria. Data refreshes automatically every 3 hours.</p>
    {% else %}
        <p>Displaying {{ events|length }} events on this page. Total matching events: <strong>{{ total_events }}</strong></p>
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
        {% if total_pages > 1 %}
        <div class="pagination">
            {% set page_args = {'source': selected_source, 'category': selected_category, 'search': search_term} %}            
            {% if page > 1 %}
                <a href="{{ url_for('index', page=page-1, **page_args) }}">&laquo; Back</a>
            {% else %}
                <span class="disabled">&laquo; Back</span>
            {% endif %}            
            {% if pagination.show_first %}
                <a href="{{ url_for('index', page=1, **page_args) }}">1</a>
            {% endif %}            
            {% if pagination.show_left_ellipsis %}
                <span class="ellipsis">...</span>
            {% endif %}            
            {% for p in pagination.pages %}
                {% if p == page %}
                    <a href="#" class="active">{{ p }}</a>
                {% else %}
                    <a href="{{ url_for('index', page=p, **page_args) }}">{{ p }}</a>
                {% endif %}
            {% endfor %}            
            {% if pagination.show_right_ellipsis %}
                <span class="ellipsis">...</span>
            {% endif %}            
            {% if pagination.show_last %}
                <a href="{{ url_for('index', page=total_pages, **page_args) }}">{{ total_pages }}</a>
            {% endif %}            
            {% if page < total_pages %}
                <a href="{{ url_for('index', page=page+1, **page_args) }}">Next &raquo;</a>
            {% else %}
                <span class="disabled">Next &raquo;</span>
            {% endif %}
        </div>
        {% endif %}
    {% endif %}
</body>
</html>
    """
    return render_template_string(
        html,
        events=events,
        page=page,
        total_pages=total_pages,
        pagination=pagination,
        sources=sources,
        categories=categories,
        selected_source=selected_source,
        selected_category=selected_category,
        search_term=search_term,
        total_events=total_events
    )


@app.route('/upload_pdf', methods=['POST'])
def upload_pdf():
    if 'pdf_file' not in request.files:
        flash('No file selected', 'error')
        return redirect(url_for('index'))

    file = request.files['pdf_file']

    if file.filename == '':
        flash('No file selected', 'error')
        return redirect(url_for('index'))

    if not allowed_file(file.filename):
        flash('Invalid file type. Please upload a PDF file.', 'error')
        return redirect(url_for('index'))

    filepath = None
    try:
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_filename = f"{timestamp}_{filename}"
        filepath = os.path.join(UPLOAD_FOLDER, unique_filename)

        # Save file
        file.save(filepath)
        print(f"PDF saved to: {filepath}", file=sys.stderr)

        # Run spider
        result = run_pdf_spider(filepath)

        if result:
            # Run transformation immediately
            print("Running transformation after PDF upload...", file=sys.stderr)
            transform_result = run_transformation()

            if transform_result:
                flash(
                    'PDF processed and transformed successfully! Check the dashboard.', 'success')
            else:
                flash(
                    'PDF processed but transformation failed. Run Manual Run to complete.', 'error')
        else:
            flash('PDF processed but no data extracted. Check file format.', 'error')

    except Exception as e:
        flash(f'Error processing PDF: {str(e)}', 'error')
        print(f"PDF processing error: {e}", file=sys.stderr)
    finally:
        # Clean up file
        if filepath and os.path.exists(filepath):
            try:
                os.remove(filepath)
                print(f"Cleaned up temp file: {filepath}", file=sys.stderr)
            except Exception as e:
                print(f"Error removing temp file: {e}", file=sys.stderr)

    return redirect(url_for('index'))


def run_pdf_spider(pdf_path):
    """Run PDF spider as subprocess"""
    try:
        project_dir = '/app/scraper'
        env = os.environ.copy()
        env['PYTHONPATH'] = '/app'

        print(f"Starting PDF spider for: {pdf_path}", file=sys.stderr)

        result = subprocess.run(
            ['scrapy', 'crawl', 'pdf', '-a', f'pdf_path={pdf_path}'],
            cwd=project_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            print(f"PDF spider stderr: {result.stderr}", file=sys.stderr)
            return False

        print(f"PDF spider stdout: {result.stdout}", file=sys.stderr)
        print("PDF spider completed successfully", file=sys.stderr)
        return True

    except subprocess.TimeoutExpired:
        print("PDF spider timeout", file=sys.stderr)
        return False
    except Exception as e:
        print(f"PDF spider subprocess error: {e}", file=sys.stderr)
        return False


def run_transformation():
    """Run transformation on raw_data"""
    try:
        env = os.environ.copy()
        env['PYTHONPATH'] = '/app'

        print("Starting transformation...", file=sys.stderr)

        result = subprocess.run(
            ['python', '/app/transform_data.py'],
            env=env,
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode != 0:
            print(f"Transformation error: {result.stderr}", file=sys.stderr)
            return False

        print(f"Transformation output: {result.stdout}", file=sys.stderr)
        return True

    except subprocess.TimeoutExpired:
        print("Transformation timeout", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Transformation error: {e}", file=sys.stderr)
        return False


@app.route('/clear', methods=['POST'])
def clear_data():
    conn = db_manager._get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "TRUNCATE TABLE events, raw_data RESTART IDENTITY CASCADE;")
        conn.commit()
        print("Database cleared by user action.")
    except Exception as e:
        conn.rollback()
        print(f"Error clearing PostgreSQL database: {e}", file=sys.stderr)
    finally:
        cursor.close()
        conn.close()
    return redirect(url_for('index'))


@app.route('/launch_manual_scrape', methods=['POST'])
def launch_manual_scrape():
    clear_data()
    print("Dispatching ETL chain to Celery worker.")
    scrape_and_transform_chain.delay()
    return redirect(url_for('index'))


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000, debug=True)
