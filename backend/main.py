import sys, os
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)
import subprocess

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
import time

try:
    from parser import parser
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "bitarray"])
    from parser import parser

# Import routes
from backend.routes.auth import router as auth_router
from backend.routes.files import router as file_router
from backend.routes.query import router as query_router

# Import database
from backend.database.db import create_tables

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth_router, prefix="/auth", tags=["Authentication"])
app.include_router(file_router, prefix="/files", tags=["File Management"])
app.include_router(query_router, prefix="/sql", tags=["SQL Queries"])

@app.on_event("startup")
async def startup_db_client():
    # Initialize database tables at startup
    create_tables()

