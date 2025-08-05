from pydantic import BaseModel, validator
from typing import Optional
from datetime import date, datetime
from uuid import UUID

class BirthRecordBase(BaseModel):
    record_date: date
    ip_number: str
    admission_date: date
    discharge_date: Optional[date] = None
    date_of_birth: date
    gender: str
    mode_of_delivery: str
    child_name: str
    father_name: str
    birth_notification_no: str

    @validator('gender')
    def validate_gender(cls, v):
        if v.lower() not in ['male', 'female', 'other']:
            raise ValueError('Gender must be Male, Female, or Other')
        return v.title()

    @validator('mode_of_delivery')
    def validate_delivery_mode(cls, v):
        valid_modes = ['normal', 'c-section', 'vacuum', 'forceps', 'breech']
        if v.lower() not in valid_modes:
            raise ValueError(f'Mode of delivery must be one of: {", ".join(valid_modes)}')
        return v.title()

class BirthRecordCreate(BirthRecordBase):
    pass

class BirthRecordUpdate(BaseModel):
    record_date: Optional[date] = None
    ip_number: Optional[str] = None
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
    created_at: datetime
    updated_at: Optional[datetime] = None
    created_by: Optional[int] = None

    class Config:
        from_attributes = True