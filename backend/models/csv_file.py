from backend.database.db import get_db_connection

class CSVFile:
    """CSV File model for database operations"""
    
    @staticmethod
    def create(user_id, filename, original_filename):
        """Create a new CSV file record"""
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO csv_files (user_id, filename, original_filename) VALUES (?, ?, ?)",
                (user_id, filename, original_filename)
            )
            conn.commit()
            file_id = cursor.lastrowid
            conn.close()
            return file_id
        except Exception as e:
            conn.close()
            return None
    
    @staticmethod
    def exists(user_id, original_filename):
        """Check if a file with the same name already exists for this user"""
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id FROM csv_files WHERE user_id = ? AND original_filename = ?",
            (user_id, original_filename)
        )
        result = cursor.fetchone()
        conn.close()
        return result is not None
    
    @staticmethod
    def get_by_user_id(user_id):
        """Get all CSV files for a specific user"""
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM csv_files WHERE user_id = ?", (user_id,))
        csv_files = cursor.fetchall()
        conn.close()
        return [dict(row) for row in csv_files]
