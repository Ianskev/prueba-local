from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlalchemy.orm import Session
from typing import List

from backend.database import get_db, User, File as FileModel
from backend.schemas import File as FileSchema
from backend.utils.auth import get_current_active_user
from backend.utils.csv_handler import CSVHandler

router = APIRouter(prefix="/files", tags=["files"])

@router.post("/upload", response_model=FileSchema)
async def upload_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    # Check if file already exists for this user
    existing_file = db.query(FileModel).filter(
        FileModel.user_id == current_user.id,
        FileModel.filename == file.filename
    ).first()
    
    if existing_file:
        raise HTTPException(
            status_code=400,
            detail=f"A file with name '{file.filename}' already exists for this user"
        )
    
    # Save the file
    csv_handler = CSVHandler(current_user.id)
    file_path = await csv_handler.save_csv_file(file)
    
    # Create file record in database
    db_file = FileModel(
        filename=file.filename,
        file_path=file_path,
        user_id=current_user.id
    )
    
    db.add(db_file)
    db.commit()
    db.refresh(db_file)
    
    return db_file

@router.get("/", response_model=List[FileSchema])
def get_user_files(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    files = db.query(FileModel).filter(FileModel.user_id == current_user.id).all()
    return files

@router.get("/{file_id}/preview")
def preview_csv_file(
    file_id: int,
    rows: int = 5,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    # Check if file exists and belongs to user
    file = db.query(FileModel).filter(
        FileModel.id == file_id,
        FileModel.user_id == current_user.id
    ).first()
    
    if not file:
        raise HTTPException(status_code=404, detail="File not found")
    
    # Get preview
    csv_handler = CSVHandler(current_user.id)
    return csv_handler.get_csv_preview(file.filename, rows)

@router.post("/{file_id}/import/{table_name}")
def import_csv_to_table(
    file_id: int,
    table_name: str,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    # Check if file exists and belongs to user
    file = db.query(FileModel).filter(
        FileModel.id == file_id,
        FileModel.user_id == current_user.id
    ).first()
    
    if not file:
        raise HTTPException(status_code=404, detail="File not found")
    
    # Import the CSV
    csv_handler = CSVHandler(current_user.id)
    import_result = csv_handler.import_csv_to_table(file.filename, table_name)
    return import_result
