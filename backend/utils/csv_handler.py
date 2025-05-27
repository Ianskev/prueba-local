import os
import pandas as pd
import sys
from fastapi import UploadFile, HTTPException

# Add the root directory to sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from parser.parser import execute_sql

class CSVHandler:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.upload_dir = os.path.join("uploads", f"user_{user_id}")
        os.makedirs(self.upload_dir, exist_ok=True)

    async def save_csv_file(self, file: UploadFile) -> str:
        """Save a CSV file to the user's directory, checking for duplicates"""
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are allowed")
        
        # Check if file already exists
        file_path = os.path.join(self.upload_dir, file.filename)
        if os.path.exists(file_path):
            raise HTTPException(status_code=400, detail=f"File {file.filename} already exists")
        
        # Save the file
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        return file_path
    
    def get_csv_preview(self, filename: str, rows: int = 5) -> dict:
        """Get a preview of the CSV file"""
        file_path = os.path.join(self.upload_dir, filename)
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail=f"File {filename} not found")
        
        try:
            df = pd.read_csv(file_path)
            return {
                "columns": df.columns.tolist(),
                "records": df.head(rows).values.tolist()
            }
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error reading CSV: {str(e)}")
    
    def import_csv_to_table(self, filename: str, table_name: str) -> dict:
        """Import a CSV file into a table"""
        file_path = os.path.join(self.upload_dir, filename)
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail=f"File {filename} not found")
        
        try:
            # Read CSV to get column names
            df = pd.read_csv(file_path)
            columns = df.columns.tolist()
            
            # Create SQL insert statements for each row
            success_count = 0
            error_count = 0
            
            for _, row in df.iterrows():
                # Format the values properly based on data types
                values = []
                for val in row.tolist():
                    if pd.isna(val):
                        values.append("NULL")
                    elif isinstance(val, str):
                        values.append(f"'{val}'")
                    else:
                        values.append(str(val))
                
                # Create the INSERT statement
                insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
                
                try:
                    result, message = execute_sql(insert_query)
                    if result is not None or "successful" in message.lower():
                        success_count += 1
                    else:
                        error_count += 1
                except Exception:
                    error_count += 1
            
            return {
                "success_count": success_count,
                "error_count": error_count,
                "total_rows": len(df)
            }
            
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error importing CSV: {str(e)}")
    
    def list_user_files(self) -> list:
        """List all CSV files uploaded by the user"""
        try:
            files = [f for f in os.listdir(self.upload_dir) if f.endswith('.csv')]
            return files
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error listing files: {str(e)}")
