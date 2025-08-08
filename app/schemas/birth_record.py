from typing import Optional
from pydantic import BaseModel
from datetime import date, datetime
from uuid import UUID

class BirthRecordBase(BaseModel):
    record_date: Optional[date] = None
    ip_number: str
    mother_name: str
    admission_date: Optional[date] = None
    discharge_date: Optional[date] = None
    date_of_birth: Optional[date] = None
    gender: Optional[str] = None
    mode_of_delivery: Optional[str] = None
    child_name: Optional[str] = None
    father_name: Optional[str] = None
    birth_notification_no: Optional[str] = None

class BirthRecordCreate(BirthRecordBase):
    pass

class BirthRecordUpdate(BaseModel):
    record_date: Optional[date] = None
    ip_number: Optional[str] = None
    mother_name: Optional[str] = None
    admission_date: Optional[date] = None
    discharge_date: Optional[date] = None
    date_of_birth: Optional[date] = None
    gender: Optional[str] = None
    mode_of_delivery: Optional[str] = None
    child_name: Optional[str] = None
    father_name: Optional[str] = None
    birth_notification_no: Optional[str] = None

class BirthRecord(BirthRecordBase):
    id: UUID
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[int] = None

    class Config:
        orm_mode = True