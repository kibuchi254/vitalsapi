from typing import Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy.orm import Session
from uuid import UUID
from datetime import date

from app.api import deps
from app.crud.birth_record import birth_record as birth_record_crud
from app.schemas.birth_record import BirthRecord, BirthRecordCreate, BirthRecordUpdate
from app.models.user import User
from app.utils.excel_parser import parse_excel_file

router = APIRouter()

@router.get("/", response_model=List[BirthRecord])
def read_birth_records(
    db: Session = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100,
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Retrieve birth records"""
    records = birth_record_crud.get_multi(db, skip=skip, limit=limit)
    return records

@router.post("/", response_model=BirthRecord)
def create_birth_record(
    *,
    db: Session = Depends(deps.get_db),
    record_in: BirthRecordCreate,
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Create new birth record"""
    # Check if birth notification number already exists
    existing_record = birth_record_crud.get_by_notification_no(
        db, notification_no=record_in.birth_notification_no
    )
    if existing_record:
        raise HTTPException(
            status_code=400,
            detail="Birth notification number already exists"
        )
    
    record = birth_record_crud.create(
        db=db, obj_in=record_in, created_by=current_user.id
    )
    return record

@router.get("/{record_id}", response_model=BirthRecord)
def read_birth_record(
    *,
    db: Session = Depends(deps.get_db),
    record_id: UUID,
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Get birth record by ID"""
    record = birth_record_crud.get(db=db, id=record_id)
    if not record:
        raise HTTPException(status_code=404, detail="Birth record not found")
    return record

@router.put("/{record_id}", response_model=BirthRecord)
def update_birth_record(
    *,
    db: Session = Depends(deps.get_db),
    record_id: UUID,
    record_in: BirthRecordUpdate,
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Update a birth record"""
    record = birth_record_crud.get(db=db, id=record_id)
    if not record:
        raise HTTPException(status_code=404, detail="Birth record not found")
    
    # Check if trying to update birth notification number to existing one
    if record_in.birth_notification_no:
        existing_record = birth_record_crud.get_by_notification_no(
            db, notification_no=record_in.birth_notification_no
        )
        if existing_record and existing_record.id != record_id:
            raise HTTPException(
                status_code=400,
                detail="Birth notification number already exists"
            )
    
    record = birth_record_crud.update(db=db, db_obj=record, obj_in=record_in)
    return record

@router.delete("/{record_id}")
def delete_birth_record(
    *,
    db: Session = Depends(deps.get_db),
    record_id: UUID,
    current_user: User = Depends(deps.get_current_active_superuser),
) -> Any:
    """Delete a birth record (superuser only)"""
    record = birth_record_crud.get(db=db, id=record_id)
    if not record:
        raise HTTPException(status_code=404, detail="Birth record not found")
    
    birth_record_crud.remove(db=db, id=record_id)
    return {"message": "Birth record deleted successfully"}

@router.get("/search/", response_model=List[BirthRecord])
def search_birth_records(
    *,
    db: Session = Depends(deps.get_db),
    q: str = Query(..., min_length=1),
    skip: int = 0,
    limit: int = 100,
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Search birth records by child name, father name, or notification number"""
    records = birth_record_crud.search(db=db, query=q, skip=skip, limit=limit)
    return records

@router.get("/date-range/", response_model=List[BirthRecord])
def get_records_by_date_range(
    *,
    db: Session = Depends(deps.get_db),
    start_date: date,
    end_date: date,
    skip: int = 0,
    limit: int = 100,
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Get birth records within a date range"""
    records = birth_record_crud.get_by_date_range(
        db=db, start_date=start_date, end_date=end_date, skip=skip, limit=limit
    )
    return records

@router.post("/upload-excel/")
async def upload_excel_file(
    *,
    db: Session = Depends(deps.get_db),
    file: UploadFile = File(...),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Upload and parse Excel file with birth records"""
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(
            status_code=400,
            detail="File must be an Excel file (.xlsx or .xls)"
        )
    
    try:
        # Read file content
        content = await file.read()
        
        # Parse the Excel file
        records_data = parse_excel_file(content)
        
        created_records = []
        errors = []
        
        for i, record_data in enumerate(records_data):
            try:
                # Check if birth notification number already exists
                existing_record = birth_record_crud.get_by_notification_no(
                    db, notification_no=record_data['birth_notification_no']
                )
                if existing_record:
                    errors.append(f"Row {i+1}: Birth notification number {record_data['birth_notification_no']} already exists")
                    continue
                
                # Create BirthRecordCreate object
                record_create = BirthRecordCreate(**record_data)
                
                # Create the record
                record = birth_record_crud.create(
                    db=db, obj_in=record_create, created_by=current_user.id
                )
                created_records.append(record)
                
            except Exception as e:
                errors.append(f"Row {i+1}: {str(e)}")
        
        return {
            "message": f"Successfully processed {len(created_records)} records",
            "created_count": len(created_records),
            "error_count": len(errors),
            "errors": errors
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Error processing Excel file: {str(e)}"
        )