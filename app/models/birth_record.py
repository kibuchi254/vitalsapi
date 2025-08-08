from sqlalchemy import Column, String, Date, Integer, DateTime, func
from sqlalchemy.dialects.postgresql import UUID
from app.core.database import Base
import uuid

class BirthRecord(Base):
    __tablename__ = "birth_records"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    record_date = Column(Date, nullable=False)
    ip_number = Column(String(50), nullable=False)
    mother_name = Column(String(100), nullable=False)
    admission_date = Column(Date, nullable=False)
    discharge_date = Column(Date, nullable=True)
    date_of_birth = Column(Date, nullable=False)
    gender = Column(String(20), nullable=False)
    mode_of_delivery = Column(String(50), nullable=False)
    child_name = Column(String(100), nullable=False)
    father_name = Column(String(100), nullable=True)
    birth_notification_no = Column(String(50), unique=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), nullable=True)
    created_by = Column(Integer, nullable=True)