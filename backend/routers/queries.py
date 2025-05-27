from fastapi import APIRouter, Depends, HTTPException
import time
import sys
import os

# Add the root directory to sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from parser.parser import execute_sql
from backend.schemas import QueryRequest, QueryResult
from backend.utils.auth import get_current_active_user
from backend.database import User, Table, File, get_db
from sqlalchemy.orm import Session

router = APIRouter(prefix="/sql", tags=["queries"])

@router.post("/", response_model=QueryResult)
def execute_query(
    query_request: QueryRequest,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    # For CREATE TABLE statements, we should track the table in the database
    query_lower = query_request.query.lower().strip()
    
    try:
        start = time.time()
        result, message = execute_sql(query_request.query)
        end = time.time()
        
        # If it's a CREATE TABLE query and successful, add it to the user's tables
        if query_lower.startswith("create table") and "successfully" in message.lower():
            # Extract table name from query - simplified version
            table_name = query_lower.split("create table")[1].split("(")[0].strip()
            
            # Check if table already exists for this user
            existing_table = db.query(Table).filter(
                Table.user_id == current_user.id,
                Table.name == table_name
            ).first()
            
            if not existing_table:
                # Create table record
                db_table = Table(
                    name=table_name,
                    user_id=current_user.id
                )
                db.add(db_table)
                db.commit()
        
    except Exception as e:
        end = time.time()
        result, message = None, str(e)
    
    result_pagination = {
        'columns': [],
        'records': []
    }
    
    if result is not None:
        result_pagination = {
            'columns': result['columns'],
            'records': result['records'][query_request.offset:query_request.offset + query_request.limit]
        }
        total = len(result['records'])
    else:
        total = 0
    
    return {
        'data': result_pagination,
        'total': total,
        'message': message,
        'execution_time': end - start
    }

@router.get("/tables")
def list_user_tables(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    tables = db.query(Table).filter(Table.user_id == current_user.id).all()
    return [{"id": table.id, "name": table.name, "created_at": table.created_at} for table in tables]

@router.get("/dashboard")
def get_dashboard(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    tables = db.query(Table).filter(Table.user_id == current_user.id).all()
    files = db.query(File).filter(File.user_id == current_user.id).all()
    
    return {
        "tables": [{"id": table.id, "name": table.name} for table in tables],
        "files": [{"id": file.id, "filename": file.filename} for file in files]
    }
