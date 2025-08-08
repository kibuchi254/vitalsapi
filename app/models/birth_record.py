from sqlalchemy import Column, Integer, String, Date, DateTime
from sqlalchemy.sql import func
from app.core.database import Base
import uuid
from sqlalchemy.dialects.postgresql import UUID

class BirthRecord(Base):
    __tablename__ = "birth_records"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    record_date = Column(Date, nullable=False, index=True)
    ip_number = Column(String(255), nullable=False, index=True)
    mother_name = Column(String(100), nullable=False)
    admission_date = Column(Date, nullable=False)
    discharge_date = Column(Date, nullable=True)
    date_of_birth = Column(Date, nullable=False)
    gender = Column(String(10), nullable=False)
    mode_of_delivery = Column(String(100), nullable=False)
    child_name = Column(String(100), nullable=False)
    father_name = Column(String(100), nullable=True)
    birth_notification_no = Column(String(50), unique=True, nullable=False, index=True)
    
    # Additional fields for better tracking
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    created_by = Column(Integer, nullable=True)