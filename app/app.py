from flask import Flask, request, jsonify
import psycopg2
import redis
import os
import time
from prometheus_client import Counter, generate_latest

app = Flask(__name__)

#  Prometheus metric
REQUEST_COUNT = Counter('request_count', 'Total HTTP Requests')

#  Redis
r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    decode_responses=True
)

#  PostgreSQL
def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("DB_PORT")
    )
    return conn

#  Wait for DB
def wait_for_db():
    while True:
        try:
            conn = get_db_connection()
            conn.close()
            print("Database is ready ")
            break
        except:
            print("Waiting for database...")
            time.sleep(2)

#  Init DB
def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

#  Home
@app.route('/')
def home():
    REQUEST_COUNT.inc()
    return "Flask Microservice Running "

#  GET tasks
@app.route('/tasks', methods=['GET'])
def get_tasks():
    REQUEST_COUNT.inc()

    cached = r.get("tasks")
    if cached:
        return jsonify({"source": "cache", "data": eval(cached)})

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks;")
    tasks = cur.fetchall()
    cur.close()
    conn.close()

    result = [{"id": t[0], "title": t[1]} for t in tasks]

    r.set("tasks", str(result))

    return jsonify({"source": "db", "data": result})

#  Add task
@app.route('/tasks', methods=['POST'])
def add_task():
    REQUEST_COUNT.inc()

    data = request.json

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO tasks (title) VALUES (%s);", (data['title'],))
    conn.commit()
    cur.close()
    conn.close()

    r.delete("tasks")

    return jsonify({"message": "Task added"}), 201

#  Delete task
@app.route('/tasks/<int:id>', methods=['DELETE'])
def delete_task(id):
    REQUEST_COUNT.inc()

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM tasks WHERE id = %s;", (id,))
    conn.commit()
    cur.close()
    conn.close()

    r.delete("tasks")

    return jsonify({"message": "Task deleted"})

#  Prometheus metrics
@app.route('/metrics')
def metrics():
    return generate_latest(), 200, {'Content-Type': 'text/plain'}

#  Run
if __name__ == '__main__':
    wait_for_db()
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
    #test ci cd pipeline