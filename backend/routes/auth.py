from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import OAuth2PasswordRequestForm
from backend.schemas.user import UserCreate, UserLogin, Token, UserResponse
from backend.models.user import User
from backend.utils.auth import get_password_hash, verify_password, create_access_token

router = APIRouter()

@router.post("/register", response_model=UserResponse)
async def register(user: UserCreate):
    """Register a new user"""
    existing_user = User.get_by_email(user.email)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
        
    hashed_password = get_password_hash(user.password)
    user_id = User.create(user.username, user.email, hashed_password)
    
    return {
        "id": user_id,
        "username": user.username,
        "email": user.email
    }

@router.post("/login", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """Login user and return JWT token"""
    user = User.get_by_email(form_data.username)
    
    if not user or not verify_password(form_data.password, user["password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    access_token = create_access_token(
        data={"sub": user["id"]}
    )
    
    return {"access_token": access_token, "token_type": "bearer"}
