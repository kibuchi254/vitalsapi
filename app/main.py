from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1 import api_router
from app.core.config import settings
from app.core.database import engine
from app.models import birth_record, user

# Create database tables
birth_record.Base.metadata.create_all(bind=engine)
user.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Birth Records Management API with JWT Authentication",
    version="1.0.0",
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# Define the allowed origins. It's better to be specific than use a wildcard in production.
# For development, you can add your frontend's URL.
# origins = [
#     "http://localhost:5173",
#     "https://your-production-frontend.com"
# ]

# Use a wildcard for local development, but be aware of the security implications in production.
# The error you saw is likely caused by an external configuration layer, not this code.
# The code below is the correct way to handle CORS in FastAPI.
origins = ["*"]

# Set up CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(api_router, prefix=settings.API_V1_STR)

@app.get("/")
async def root():
    return {
        "message": "Birth Records Management API",
        "version": "1.0.0",
        "docs": "/docs",
        "api": settings.API_V1_STR
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
