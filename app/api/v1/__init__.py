from fastapi import APIRouter
from app.api.v1 import auth, birth_records, users

api_router = APIRouter()
api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])
api_router.include_router(birth_records.router, prefix="/birth-records", tags=["birth-records"])
api_router.include_router(users.router, prefix="/users", tags=["users"])