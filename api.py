from datetime import datetime

from flask import Flask, jsonify, request
from flasgger import Swagger

import psycopg2

app = Flask(__name__)
swagger = Swagger(app)

# Database connection parameters
DB_HOST = "localhost"
DB_NAME = "wxdata"
DB_USER = "web_user" #only has SELECT privileges
DB_PASSWORD = ""
DB_PORT = "5432"

# Pagination parameters
DEFAULT_PAGE = 1
DEFAULT_PER_PAGE = 10

def connect_db():
    """Connects to the PostgreSQL database."""
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST,
                                database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASSWORD,
                                port=DB_PORT)
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
    return conn

def close_db(conn):
    """Closes the database connection."""
    if conn:
        conn.close()

def paginate(cursor, page, per_page):
    """Paginates the cursor results."""
    page = int(request.args.get('page', page))
    per_page = int(request.args.get('per_page', per_page))
    page = max(page, 1)
    start = (page - 1) * per_page
    return cursor, page, per_page, start, per_page

@app.route('/api/weather', methods=['GET'])
def get_weather_data():
    """
    Get weather data from GHCN station data base
    Allows filtering by date and station ID, and supports pagination.
    ---
    parameters:
      - name: date
        in: query
        type: string
        format: date
        description: Filter by date (YYYY-MM-DD)
      - name: station_id
        in: query
        type: string
        description: Filter by station ID
      - name: page
        in: query
        type: integer
        default: 1
        description: Page number for pagination
      - name: per_page
        in: query
        type: integer
        default: 10
        description: Number of items per page
    responses:
      200:
        description: A list of weather data records
        schema:
          type: object
          properties:
            items:
              type: array
              items:
                type: object
                properties:
                  station_id:
                    type: string
                    description: Station ID
                  date:
                    type: string
                    format: date
                    description: Observation date
                  max_temperature:
                    type: number
                    format: float
                    description: Maximum Temperature in C
                  min_temperature:
                    type: number
                    format: float
                    description: Minimum Temperature in C
                  precipitation:
                    type: number
                    format: float
                    description: Total precipitation in mm
            page:
              type: integer
              description: Current page number
            per_page:
              type: integer
              description: Number of items per page
      400:
        description: Invalid input (e.g., invalid date format)
        schema:
          type: object
          properties:
            error:
              type: string
              description: Error message
      500:
        description: Database connection or query error
        schema:
          type: object
          properties:
            error:
              type: string
              description: Error message
    """
    conn = connect_db()
    if not conn:
        return jsonify({'error': 'Failed to connect to the database'}), 500
    cursor = conn.cursor()

    query = "SELECT * FROM station_data WHERE 1=1"
    conditions = []
    params = []

    date_str = request.args.get('date')
    if date_str:
        conditions.append("date = %s")
        try:
            params.append(datetime.strptime(date_str, '%Y-%m-%d').date())
        except ValueError:
            close_db(conn)
            return jsonify({'error': 'Invalid date format. Please use YYYY-MM-DD.'}), 400

    station_id = request.args.get('station_id')
    if station_id:
        conditions.append("station_id = %s")
        params.append(station_id)

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY date DESC, station_id LIMIT %s OFFSET %s"

    cursor, page, per_page, start, limit = paginate(cursor, DEFAULT_PAGE, DEFAULT_PER_PAGE)
    params.append(limit)
    params.append(start)

    try:
        cursor.execute(query, tuple(params))
        columns = [desc[0] for desc in cursor.description]
        items = [dict(zip(columns, row)) for row in cursor.fetchall()]
    except psycopg2.Error as e:
        close_db(conn)
        return jsonify({'error': f"Database query error: {e}"}), 500
    finally:
        cursor.close()
        close_db(conn)

    return jsonify({
        'items': items,
        'page': page,
        'per_page': per_page
    })

@app.route('/api/weather/stats', methods=['GET'])
def get_weather_stats():
    """
    Get annual weather statistics from GHCN stations
    Allows filtering by year and station ID, and supports pagination.
    ---
    parameters:
      - name: year
        in: query
        type: number
        format: integer
        description: Filter by year
      - name: station_id
        in: query
        type: string
        description: Filter by station ID
      - name: page
        in: query
        type: integer
        default: 1
        description: Page number for pagination
      - name: per_page
        in: query
        type: integer
        default: 10
        description: Number of items per page
    responses:
      200:
        description: A list of weather statistics records
        schema:
          type: object
          properties:
            items:
              type: array
              items:
                type: object
                properties:
                  station_id:
                    type: string
                    description: Station ID
                  year:
                    type: number
                    format: integer
                    description: Observation date
                  max_temperature_avg:
                    type: number
                    format: float
                    description: Average maximum temperature in C
                  min_temperature_avg:
                    type: number
                    format: float
                    description: Average minimum temperature in C
                  precipitation_accum:
                    type: number
                    format: float
                    description: Total annual precipitation in cm
            page:
              type: integer
              description: Current page number
            per_page:
              type: integer
              description: Number of items per page
      400:
        description: Invalid input (e.g., invalid date format)
        schema:
          type: object
          properties:
            error:
              type: string
              description: Error message
      500:
        description: Database connection or query error
        schema:
          type: object
          properties:
            error:
              type: string
              description: Error message
    """
    conn = connect_db()
    if not conn:
        return jsonify({'error': 'Failed to connect to the database'}), 500
    cursor = conn.cursor()

    query = "SELECT station_id, year, max_temperature_avg, min_temperature_avg, precipitation_accum FROM weather_stats WHERE 1=1"
    conditions = []
    params = []

    date_str = request.args.get('year')
    if date_str:
        conditions.append("year = %s")
        try:
            year = int(date_str)
            if (year > 2014 or year < 1985):
                raise ValueError()
            params.append(year)
        except ValueError:
            session.close()
            return jsonify({'error': 'Invalid year.  Year must be between 1985 and 2014'}), 400
        except TypeError:
            session.close()
            return jsonify({'error': 'Invalid year.  Year must be an integer'}), 400

    station_id = request.args.get('station_id')
    if station_id:
        conditions.append("station_id = %s")
        params.append(station_id)

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY year DESC, station_id LIMIT %s OFFSET %s"

    cursor, page, per_page, start, limit = paginate(cursor, DEFAULT_PAGE, DEFAULT_PER_PAGE)
    params.append(limit)
    params.append(start)

    try:
        cursor.execute(query, tuple(params))
        columns = [desc[0] for desc in cursor.description]
        items = [dict(zip(columns, row)) for row in cursor.fetchall()]
    except psycopg2.Error as e:
        close_db(conn)
        return jsonify({'error': f"Database query error: {e}"}), 500
    finally:
        cursor.close()
        close_db(conn)

    return jsonify({
        'items': items,
        'page': page,
        'per_page': per_page
    })

if __name__ == '__main__':
    app.run(debug=True)
