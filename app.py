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
    limit = request.args.get('limit', type=int)
    offset = request.args.get('offset', type=int)
    order_by = request.args.get('order_by', '')
    join_tables = request.args.getlist('join')
    join_conditions = request.args.getlist('on')

    if not table:
        return jsonify({
            'status': 'error',
            'message': 'Table name is required'
        }), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        query = f'SELECT {columns} FROM {table}'

        #handle Join and ON
        if join_tables and join_conditions:
            if len(join_tables) != len(join_conditions):
                return jsonify({
                    'status': 'error',
                    'message': 'Number of join tables and on conditions must match'
                }), 400
            
        join_statements = []
        for join_table, join_condition in zip(join_tables, join_conditions):
                join_condition = join_condition.replace('"', "'")
                join_statements.append(f'JOIN {join_table} ON {join_condition}')
        query += ' ' + ' '.join(join_statements)

        if where_clause:
            where_clause = where_clause.replace('"', "'")
            query += f" WHERE {where_clause}"

        if order_by:
            query += ' ORDER BY ' + order_by

        # Handle LIMIT and OFFSET 
        if limit is not None:
            query += f' LIMIT {limit}'
        if offset is not None:
            query += f' OFFSET {offset}'

        # Debug: Print final query
        print(f"Final Query: {query}")

        # Execute query 
        cursor.execute(query)
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
