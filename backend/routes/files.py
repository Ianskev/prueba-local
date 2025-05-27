from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from backend.utils.auth import get_current_user
from backend.utils.csv_manager import save_csv_file, get_csv_content
from backend.models.csv_file import CSVFile
from typing import List

router = APIRouter()

@router.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    current_user = Depends(get_current_user)
):
    """Upload a CSV file"""
    if not file.filename.endswith('.csv'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only CSV files are allowed"
        )
        
    result = save_csv_file(file, current_user["id"])
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File already exists or couldn't be saved"
        )
        
    return {"message": "File uploaded successfully", "file": result}

@router.get("/list")
async def list_csv_files(current_user = Depends(get_current_user)):
    """List all CSV files for the current user"""
    files = CSVFile.get_by_user_id(current_user["id"])
    return {"files": files}

@router.get("/content/{file_id}")
async def get_file_content(
    file_id: int,
    current_user = Depends(get_current_user)
):
    """Get the content of a CSV file"""
    files = CSVFile.get_by_user_id(current_user["id"])
    
    file = next((f for f in files if f["id"] == file_id), None)
    if not file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found"
        )
        
    content = get_csv_content(file["filename"])
    if not content:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File content could not be read"
        )
        
    columns, rows = content
    return {
        "filename": file["original_filename"],
        "columns": columns,
        "rows": rows
    }
