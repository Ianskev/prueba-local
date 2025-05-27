from backend.database.db import get_db_connection

class User:
    """User model for database operations"""
    
    @staticmethod
    def create(username, email, hashed_password):
        """Create a new user in the database"""
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO users (username, email, password) VALUES (?, ?, ?)",
            (username, email, hashed_password)
        )
        conn.commit()
        user_id = cursor.lastrowid
        conn.close()
        return user_id

    @staticmethod
    def get_by_email(email):
        """Get user by email"""
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE email = ?", (email,))
        user = cursor.fetchone()
        conn.close()
        return dict(user) if user else None
        
    @staticmethod
    def get_by_id(user_id):
        """Get user by ID"""
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        user = cursor.fetchone()
        conn.close()
        return dict(user) if user else None
