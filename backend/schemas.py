from pydantic import BaseModel, EmailStr
from typing import Optional, List, Dict, Any
from datetime import datetime

# User schemas
class UserBase(BaseModel):
    email: EmailStr
    username: str

class UserCreate(UserBase):
    password: str

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class User(UserBase):
    id: int
    is_active: bool
    created_at: datetime

    class Config:
        orm_mode = True

# Token schemas
class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    email: Optional[str] = None

# File schemas
class FileBase(BaseModel):
    filename: str

class FileCreate(FileBase):
    pass

class File(FileBase):
    id: int
    uploaded_at: datetime
    user_id: int

    class Config:
        orm_mode = True

# Table schemas
class TableBase(BaseModel):
    name: str

class TableCreate(TableBase):
    pass

class Table(TableBase):
    id: int
    created_at: datetime
    user_id: int

    class Config:
        orm_mode = True

# Query schemas
class QueryRequest(BaseModel):
    query: str
    offset: int = 0
    limit: int = 50

class QueryResult(BaseModel):
    data: Dict[str, Any]
    total: int
    message: str
    execution_time: float

# Dashboard schemas
class Dashboard(BaseModel):
    tables: List[Table]
    files: List[File]
