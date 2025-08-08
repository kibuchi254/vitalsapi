from typing import Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from uuid import UUID
from datetime import date
import logging
import traceback
from pydantic import ValidationError

from app.api import deps
from app.crud.birth_record import birth_record as birth_record_crud
from app.schemas.birth_record import BirthRecord, BirthRecordCreate, BirthRecordUpdate
from app.models.user import User
from app.utils.excel_parser import parse_excel_file

# Set up logging
logger = logging.getLogger(__name__)

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
    """Search birth records by child name, mother name, father name, or notification number"""
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
    dry_run: bool = Query(False, description="Preview records without saving"),
) -> Any:
    """
    Upload and parse Excel file with birth records
    """
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(
            status_code=400,
            detail="File must be an Excel file (.xlsx or .xls)"
        )
    
    if file.size and file.size > 10 * 1024 * 1024:
        raise HTTPException(
            status_code=400,
            detail="File size too large. Maximum allowed size is 10MB"
        )

    logger.info(f"Processing Excel file: {file.filename} (User: {current_user.id})")
    
    try:
        content = await file.read()
        logger.info(f"File content read successfully. Size: {len(content)} bytes")
        
        records_data = parse_excel_file(content)
        logger.info(f"Successfully parsed {len(records_data)} records from Excel")
        
        if not records_data:
            raise HTTPException(
                status_code=400,
                detail="No valid records found in the Excel file"
            )
        
        if records_data:
            logger.debug(f"Sample record structure: {records_data[0]}")
            
        created_records = []
        errors = []
        validation_errors = []
        duplicate_errors = []
        database_errors = []
        
        processed_notification_nos = set()
        notification_nos = [record.get('birth_notification_no') for record in records_data 
                           if record.get('birth_notification_no')]
        existing_records = birth_record_crud.get_by_notification_nos(db, notification_nos=notification_nos)
        
        for i, record_data in enumerate(records_data):
            row_number = i + 2
            
            try:
                logger.debug(f"Processing row {row_number}: {record_data}")
                
                required_fields = ['record_date', 'ip_number', 'mother_name', 'date_of_birth', 'child_name', 'birth_notification_no']
                missing_fields = [field for field in required_fields if not record_data.get(field)]
                
                if missing_fields:
                    validation_errors.append(f"Row {row_number}: Missing required fields: {', '.join(missing_fields)}")
                    logger.debug(f"Row {row_number} failed: Missing fields {missing_fields}")
                    continue
                
                notification_no = record_data['birth_notification_no']
                
                if notification_no in processed_notification_nos:
                    duplicate_errors.append(f"Row {row_number}: Duplicate birth notification number within file: {notification_no}")
                    logger.debug(f"Row {row_number} failed: Duplicate notification number {notification_no}")
                    continue
                
                processed_notification_nos.add(notification_no)
                
                if notification_no in existing_records:
                    duplicate_errors.append(f"Row {row_number}: Birth notification number already exists in database: {notification_no}")
                    logger.debug(f"Row {row_number} failed: Notification number {notification_no} exists in database")
                    continue
                
                try:
                    record_create = BirthRecordCreate(**record_data)
                    logger.debug(f"Created valid schema object for row {row_number}: {record_create.dict()}")
                except ValidationError as ve:
                    validation_errors.append(f"Row {row_number}: Validation error - {str(ve)}")
                    logger.debug(f"Row {row_number} failed: Validation error - {str(ve)}")
                    continue
                except Exception as ve:
                    validation_errors.append(f"Row {row_number}: Data validation failed - {str(ve)}")
                    logger.debug(f"Row {row_number} failed: Data validation error - {str(ve)}")
                    continue
                
                if dry_run:
                    created_records.append({
                        "row": row_number,
                        "data": record_create.dict(),
                        "status": "valid"
                    })
                    logger.debug(f"Row {row_number} validated successfully for dry run")
                    continue
                
                try:
                    record = birth_record_crud.create(
                        db=db, obj_in=record_create, created_by=current_user.id
                    )
                    created_records.append({
                        "row": row_number,
                        "id": str(record.id),
                        "birth_notification_no": record.birth_notification_no,
                        "child_name": record.child_name,
                        "mother_name": record.mother_name
                    })
                    logger.info(f"Successfully created record for row {row_number} with ID: {record.id}")
                    
                except ValueError as ve:
                    database_errors.append(f"Row {row_number}: Database error - {str(ve)}")
                    logger.error(f"Row {row_number} failed: Database error - {str(ve)}")
                    
                except Exception as de:
                    db.rollback()
                    database_errors.append(f"Row {row_number}: Unexpected database error - {str(de)}")
                    logger.error(f"Row {row_number} failed: Unexpected database error - {str(de)}")
                    
            except Exception as e:
                errors.append(f"Row {row_number}: Processing error - {str(e)}")
                logger.error(f"Row {row_number} failed: Processing error - {str(e)}")
                logger.error(traceback.format_exc())
        
        all_errors = validation_errors + duplicate_errors + database_errors + errors
        
        total_processed = len(records_data)
        success_count = len(created_records)
        error_count = len(all_errors)
        
        response_data = {
            "message": f"Processed {total_processed} records from Excel file",
            "dry_run": dry_run,
            "total_records": total_processed,
            "success_count": success_count,
            "error_count": error_count,
            "created_records": created_records if dry_run or success_count <= 10 else created_records[:10],
            "errors": {
                "validation_errors": validation_errors,
                "duplicate_errors": duplicate_errors,
                "database_errors": database_errors,
                "other_errors": errors
            }
        }
        
        if dry_run:
            response_data["message"] = f"Dry run completed. {success_count} records would be created, {error_count} errors found"
        else:
            response_data["message"] = f"Successfully created {success_count} records, {error_count} errors encountered"
        
        logger.info(f"Upload summary - Total: {total_processed}, Success: {success_count}, Errors: {error_count}")
        
        return response_data
        
    except ValidationError as ve:
        logger.error(f"Validation error: {str(ve)}")
        raise HTTPException(
            status_code=422,
            detail=f"Data validation error: {str(ve)}"
        )
        
    except ValueError as ve:
        logger.error(f"Value error: {str(ve)}")
        raise HTTPException(
            status_code=400,
            detail=str(ve)
        )
        
    except Exception as e:
        logger.error(f"Unexpected error processing Excel file: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"Error processing Excel file: {str(e)}"
        )

@router.post("/validate-excel/")
async def validate_excel_file(
    *,
    file: UploadFile = File(...),
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Validate Excel file without saving to database (dry run)
    """
    return await upload_excel_file(
        db=None,
        file=file,
        current_user=current_user,
        dry_run=True
    )

@router.get("/upload-status/{notification_no}")
def check_record_exists(
    *,
    db: Session = Depends(deps.get_db),
    notification_no: str,
    current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """Check if a birth notification number already exists"""
    existing_record = birth_record_crud.get_by_notification_no(
        db, notification_no=notification_no
    )
    
    return {
        "notification_no": notification_no,
        "exists": existing_record is not None,
        "record_id": str(existing_record.id) if existing_record else None
    }