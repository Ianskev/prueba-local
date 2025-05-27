from fastapi import APIRouter, Depends, HTTPException
from backend.schemas.query import Query, QueryResult
from backend.utils.auth import get_current_user
import time
from parser import parser

router = APIRouter()

@router.post("/", response_model=QueryResult)
async def execute_sql_query(
    q: Query,
    current_user = Depends(get_current_user)
):
    """Execute a SQL query"""
    try:
        start = time.time()
        result, message = parser.execute_sql(q.query, user_id=current_user["id"])
        end = time.time()
    except RuntimeError as e:
        end = time.time()
        result, message = None, str(e)
    
    resultPagination = {
        'columns': [],
        'records': []
    }
    
    if result is not None:
        resultPagination = {
            'columns': result['columns'],
            'records': result['records'][q.offset : q.offset + q.limit]
        }
        total = len(result['records'])
    else:
        total = 0
    
    return {
        'data': resultPagination,
        'total': total,
        'message': message,
        'execution_time': end - start
    }

@router.get("/dashboard")
async def get_user_dashboard(current_user = Depends(get_current_user)):
    """Get user dashboard information"""
    from backend.models.csv_file import CSVFile
    csv_files = CSVFile.get_by_user_id(current_user["id"])
    
    try:
        tables_result, _ = parser.execute_sql("SHOW TABLES;", user_id=current_user["id"])
        tables = tables_result.get('records', []) if tables_result else []
    except:
        tables = []
    
    return {
        "user": {
            "id": current_user["id"],
            "username": current_user["username"],
            "email": current_user["email"]
        },
        "tables": tables,
        "csv_files": csv_files
    }
