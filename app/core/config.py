# app/core/config.py

from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # Database
    DATABASE_URL: str
    
    # JWT
    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int
    
    # API
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Birth Records API"
    
    # Admin
    FIRST_SUPERUSER_EMAIL: str
    FIRST_SUPERUSER_PASSWORD: str
    
    class Config:
        env_file = ".env"
        
settings = Settings()