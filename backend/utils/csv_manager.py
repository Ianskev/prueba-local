import os
import csv
import uuid
from backend.database.db import CSV_DIR
from backend.models.csv_file import CSVFile

def save_csv_file(file, user_id):
    """
    Save a CSV file to disk and create a record in the database
    
    Args:
        file: UploadFile from FastAPI
        user_id: ID of the user uploading the file
        
    Returns:
        dict: Information about the saved file or None if error
    """
    if CSVFile.exists(user_id, file.filename):
        return None
        
    unique_filename = f"{uuid.uuid4().hex}_{file.filename}"
    file_path = os.path.join(CSV_DIR, unique_filename)
    
    try:
        with open(file_path, "wb") as f:
            content = file.file.read()
            f.write(content)
    except Exception:
        return None
        
    file_id = CSVFile.create(user_id, unique_filename, file.filename)
    
    if not file_id:
        if os.path.exists(file_path):
            os.remove(file_path)
        return None
        
    return {
        "id": file_id,
        "filename": unique_filename,
        "original_filename": file.filename
    }

def get_csv_content(filename):
    """
    Read CSV file content
    
    Args:
        filename: The unique filename stored in the database
        
    Returns:
        tuple: (column_names, rows) or None if error
    """
    file_path = os.path.join(CSV_DIR, filename)
    
    if not os.path.exists(file_path):
        return None
        
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            columns = next(reader)
            rows = list(reader)
            return columns, rows
    except Exception:
        return None
