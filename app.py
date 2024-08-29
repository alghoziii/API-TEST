from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import psycopg2
import psycopg2.extras

app = Flask(__name__)
CORS(app)


def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "testbudimas"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "admin123")
    )
    return conn


@app.route("/")
def index():
    return jsonify({
        "status": {
            "code": 200,
            "message": "Success fetching the API"
        },
    }), 200


@app.route('/data', methods=['GET'])
def get_data():
    table = request.args.get('table')
    columns = request.args.get('columns', '*')
    where_clause = request.args.get('where', '')
    limit = request.args.get('limit')
    offset = int(request.args.get('offset', 0))
    order_by = request.args.get('order_by', '')
    join_clause = request.args.get('join', '')

    if not table:
        return jsonify({
            'status': 'error',
            'message': 'Table name is required'
        }), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Inisialisasi query
        query = f'SELECT {columns} FROM {table}'

        # Handle JOIN clause
        if join_clause:
            query += ' ' + join_clause

        # Handle WHERE clause
        if where_clause:
            where_clause = where_clause.replace('"', "'")  
            query += f" WHERE {where_clause}"

        # Handle ORDER BY
        if order_by:
            query += ' ORDER BY ' + order_by

        # Handle LIMIT and OFFSET
        if limit and limit.isdigit():
            query += ' LIMIT %s OFFSET %s'
            cursor.execute(query, (limit, offset))
        elif limit:
            query += ' LIMIT %s'
            cursor.execute(query, (limit,))
        else:
            cursor.execute(query)

        # Debug: Print final query
        print(f"Final Query: {cursor.query.decode()}")

        # Execute query
        rows = cursor.fetchall()
        result = [dict(row) for row in rows]

        cursor.close()
        conn.close()

        return jsonify({
            'data': result,
            'status': 'success',
            'message': 'Data Berhasil Ditampilkan',
        }), 200

    except Exception as e:
        if conn:
            conn.close()
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
