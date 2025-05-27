import sqlite3
import os
from pathlib import Path

DB_DIR = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data')))
DB_DIR.mkdir(exist_ok=True)

DB_PATH = DB_DIR / 'kuna_database.sqlite'
CSV_DIR = DB_DIR / 'csv_files'
CSV_DIR.mkdir(exist_ok=True)

def get_db_connection():
    """Create a connection to the SQLite database"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def create_tables():
    """Create necessary tables if they don't exist"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS csv_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        filename TEXT NOT NULL,
        original_filename TEXT NOT NULL,
        uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (id),
        UNIQUE(user_id, original_filename)
    )
    ''')
    
    conn.commit()
    conn.close()
